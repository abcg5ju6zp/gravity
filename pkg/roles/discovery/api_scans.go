package discovery

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"beryju.io/gravity/pkg/roles/discovery/types"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// APIScan is the immutable metadata of one scan round.
type APIScan struct {
	ScanID         string     `json:"scanId" required:"true"`
	Subnet         string     `json:"subnet" required:"true"`
	CIDR           string     `json:"cidr" required:"true"`
	SourceRevision int64      `json:"sourceRevision"`
	Status         string     `json:"status" required:"true"`
	NodeID         string     `json:"nodeId,omitempty"`
	StartedAt      time.Time  `json:"startedAt" required:"true"`
	FinishedAt     *time.Time `json:"finishedAt,omitempty"`
	DeviceCount    int        `json:"deviceCount"`
}

type APIScansGetInput struct {
	Subnet string `query:"subnet" description:"Optionally filter scans by subnet"`
}

type APIScansGetOutput struct {
	Scans []APIScan `json:"scans"`
}

// APIScanDevice is one immutable per-device scan observation.
type APIScanDevice struct {
	Identifier     string    `json:"identifier" required:"true"`
	Hostname       string    `json:"hostname" required:"true"`
	IP             string    `json:"ip" required:"true"`
	MAC            string    `json:"mac" required:"true"`
	Subnet         string    `json:"subnet" required:"true"`
	ScanID         string    `json:"scanId" required:"true"`
	SourceRevision int64     `json:"sourceRevision"`
	ObservedAt     time.Time `json:"observedAt" required:"true"`
	ContentHash    string    `json:"contentHash" required:"true"`
}

type APIScanDevicesGetInput struct {
	ScanID string `query:"scanId" required:"true" description:"Stable identity of the scan"`
	Subnet string `query:"subnet" description:"Optional subnet, narrows the scan lookup"`
}

type APIScanDevicesGetOutput struct {
	Devices []APIScanDevice `json:"devices"`
}

func (r *Role) APIScansGet() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIScansGetInput, output *APIScansGetOutput) error {
		key := r.i.KV().Key(types.KeyRole, types.KeyScans)
		if input.Subnet != "" {
			key = key.Add(input.Subnet)
		}
		key = key.Prefix(true)
		res, err := r.i.KV().Get(ctx, key.String(), clientv3.WithPrefix())
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		for _, kv := range res.Kvs {
			// Meta keys are exactly {subnet}/{scanID}; snapshot keys are
			// nested under .../{scanID}/devices/{identifier}.
			rel := trimKey(string(kv.Key), r.i.KV().Key(types.KeyRole, types.KeyScans).Prefix(true).String())
			if countSlashes(rel) != 1 {
				continue
			}
			sc, err := r.scanFromKV(kv)
			if err != nil {
				r.log.Warn("failed to parse scan metadata", zap.Error(err))
				continue
			}
			apiScan := APIScan{
				ScanID:         sc.Identifier,
				Subnet:         sc.Subnet,
				CIDR:           sc.CIDR,
				SourceRevision: sc.SourceRevision,
				Status:         sc.Status,
				NodeID:         sc.NodeID,
				StartedAt:      sc.StartedAt,
				DeviceCount:    sc.DeviceCount,
			}
			if !sc.FinishedAt.IsZero() {
				finished := sc.FinishedAt
				apiScan.FinishedAt = &finished
			}
			output.Scans = append(output.Scans, apiScan)
		}
		return nil
	})
	u.SetName("discovery.get_scans")
	u.SetTitle("Discovery scan results")
	u.SetTags("roles/discovery")
	u.SetExpectedErrors(status.Internal)
	return u
}

func (r *Role) APIScanDevicesGet() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIScanDevicesGetInput, output *APIScanDevicesGetOutput) error {
		if input.ScanID == "" {
			return status.InvalidArgument
		}
		var prefixes []string
		if input.Subnet != "" {
			prefixes = []string{
				r.i.KV().Key(
					types.KeyRole, types.KeyScans, input.Subnet, input.ScanID, "devices",
				).Prefix(true).String(),
			}
		} else {
			// Without a subnet the scan could live below any subnet; scanIDs
			// are UUIDs and unique, so iterate the scopes and match by id.
			scans, err := r.i.KV().Get(ctx, r.i.KV().Key(types.KeyRole, types.KeyScans).Prefix(true).String(), clientv3.WithPrefix())
			if err != nil {
				return status.Wrap(err, status.Internal)
			}
			found := map[string]struct{}{}
			for _, kv := range scans.Kvs {
				rel := trimKey(string(kv.Key), r.i.KV().Key(types.KeyRole, types.KeyScans).Prefix(true).String())
				parts := strings.SplitN(rel, "/", 3)
				if len(parts) < 2 || parts[1] != input.ScanID {
					continue
				}
				if _, ok := found[parts[0]]; ok {
					continue
				}
				found[parts[0]] = struct{}{}
				prefixes = append(prefixes, r.i.KV().Key(
					types.KeyRole, types.KeyScans, parts[0], input.ScanID, "devices",
				).Prefix(true).String())
			}
		}
		for _, prefix := range prefixes {
			res, err := r.i.KV().Get(ctx, prefix, clientv3.WithPrefix())
			if err != nil {
				return status.Wrap(err, status.Internal)
			}
			for _, kv := range res.Kvs {
				var snap ScanDevice
				if err := json.Unmarshal(kv.Value, &snap); err != nil {
					r.log.Warn("failed to parse scan device snapshot", zap.Error(err))
					continue
				}
				output.Devices = append(output.Devices, APIScanDevice{
					Identifier:     snap.Identifier,
					Hostname:       snap.Hostname,
					IP:             snap.IP,
					MAC:            snap.MAC,
					Subnet:         snap.Subnet,
					ScanID:         snap.ScanID,
					SourceRevision: snap.SourceRev,
					ObservedAt:     snap.ObservedAt,
					ContentHash:    snap.ContentHash,
				})
			}
		}
		return nil
	})
	u.SetName("discovery.get_scan_devices")
	u.SetTitle("Discovery scan devices")
	u.SetTags("roles/discovery")
	u.SetExpectedErrors(status.InvalidArgument, status.Internal)
	return u
}
