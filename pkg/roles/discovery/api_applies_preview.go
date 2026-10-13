package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"strings"

	"beryju.io/gravity/pkg/roles"
	dhcptypes "beryju.io/gravity/pkg/roles/dhcp/types"
	"beryju.io/gravity/pkg/roles/discovery/types"
	dnstypes "beryju.io/gravity/pkg/roles/dns/types"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	errInvalidPreviewInput = errors.New("invalid preview input")
	errScanNotFound        = errors.New("scan not found")
	errScanNotFinished     = errors.New("scan is not finished")
	errScanExpired         = errors.New("scan results expired")
	errHostnameNoDomain    = errors.New("device hostname has no domain and no zone given")
)

// APIAppliesPreviewInput pins an immutable scan and the apply targets. The
// response contains the per-device revisions the apply job must echo back.
type APIAppliesPreviewInput struct {
	ScanID  string   `json:"scanId" required:"true"`
	Targets []string `json:"targets" required:"true" enum:"dhcp,dns" description:"Apply targets, any of dhcp, dns"`

	DHCPScope string `json:"dhcpScope" description:"Required when dhcp is targeted"`
	DNSZone   string `json:"dnsZone" description:"Optional explicit DNS zone; falls back to the device FQDN"`

	// Identifiers optionally limits the preview; empty means every device of
	// the scan.
	Identifiers []string `json:"identifiers,omitempty"`
}

// APIApplyPreviewStep is one still-executable (or blocked) object change.
type APIApplyPreviewStep struct {
	Name      string `json:"name" required:"true"`
	TargetKey string `json:"targetKey" required:"true"`
	Action    string `json:"action" required:"true"`
	Reason    string `json:"reason,omitempty"`
	Siblings  int64  `json:"siblings,omitempty"`
	CreateRev int64  `json:"createRevision"`
	ModRev    int64  `json:"modRevision"`
}

// APIApplyPreviewItem is the pinned per-device diff.
type APIApplyPreviewItem struct {
	Identifier string `json:"identifier" required:"true"`

	Hostname    string `json:"hostname" required:"true"`
	IP          string `json:"ip" required:"true"`
	MAC         string `json:"mac" required:"true"`
	Subnet      string `json:"subnet" required:"true"`
	ContentHash string `json:"contentHash" required:"true"`

	// Revision of the live /discovery/devices/{id} key at preview time
	// (0 when absent).
	DeviceRevision int64 `json:"deviceRevision"`

	// Executable is false whenever the item or any step carries a reason
	// (drift, conflict, missing scope/zone, missing MAC or unavailable role).
	Executable bool     `json:"executable"`
	Reasons    []string `json:"reasons,omitempty"`

	Steps []APIApplyPreviewStep `json:"steps" required:"true"`
}

type APIAppliesPreviewOutput struct {
	ScanID         string                `json:"scanId" required:"true"`
	Subnet         string                `json:"subnet" required:"true"`
	SourceRevision int64                 `json:"sourceRevision"`
	Items          []APIApplyPreviewItem `json:"items" required:"true"`
}

func (r *Role) APIAppliesPreview() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIAppliesPreviewInput, output *APIAppliesPreviewOutput) error {
		if input.ScanID == "" || len(input.Targets) == 0 {
			return status.Wrap(errInvalidPreviewInput, status.InvalidArgument)
		}
		for _, target := range input.Targets {
			if target != types.TargetDHCP && target != types.TargetDNS {
				return status.Wrap(errInvalidPreviewInput, status.InvalidArgument)
			}
		}

		sc, err := r.findScan(ctx, input.ScanID)
		if err == errScanNotFound {
			return status.Wrap(err, status.NotFound)
		}
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		if sc.Status != types.ScanStatusFinished {
			return status.Wrap(errScanNotFinished, status.InvalidArgument)
		}

		snapshots, err := r.listScanDevices(ctx, sc.Subnet, sc.Identifier)
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		if len(snapshots) == 0 {
			// Scan metadata exists but the immutable snapshots expired.
			return status.Wrap(errScanExpired, status.InvalidArgument)
		}

		wanted := map[string]struct{}{}
		for _, id := range input.Identifiers {
			wanted[id] = struct{}{}
		}

		output.ScanID = sc.Identifier
		output.Subnet = sc.Subnet
		output.SourceRevision = sc.SourceRevision

		for _, snap := range snapshots {
			if len(wanted) > 0 {
				if _, ok := wanted[snap.Identifier]; !ok {
					continue
				}
			}
			item := r.previewItem(ctx, sc, snap, input.Targets, input.DHCPScope, input.DNSZone)
			output.Items = append(output.Items, item)
		}
		return nil
	})
	u.SetName("discovery.preview_apply")
	u.SetTitle("Preview discovery device apply")
	u.SetTags("roles/discovery")
	u.SetExpectedErrors(status.InvalidArgument, status.NotFound, status.Internal)
	return u
}

func (r *Role) previewItem(ctx context.Context, sc *Scan, snap ScanDevice, targets []string, dhcpScope, dnsZone string) APIApplyPreviewItem {
	item := APIApplyPreviewItem{
		Identifier:  snap.Identifier,
		Hostname:    snap.Hostname,
		IP:          snap.IP,
		MAC:         snap.MAC,
		Subnet:      snap.Subnet,
		ContentHash: snap.ContentHash,
		Steps:       []APIApplyPreviewStep{},
		Reasons:     []string{},
	}

	// Live device checks.
	liveKey := r.i.KV().Key(types.KeyRole, types.KeyDevices, snap.Identifier).String()
	live, err := r.i.KV().Get(ctx, liveKey)
	if err == nil && len(live.Kvs) > 0 {
		item.DeviceRevision = live.Kvs[0].ModRevision
		if dev := r.deviceFromKV(live.Kvs[0]); dev != nil {
			switch {
			case dev.Source == types.DeviceSourceManual:
				item.Reasons = append(item.Reasons, types.ReasonDeviceManual)
			case dev.ApplyID != "":
				item.Reasons = append(item.Reasons, types.ReasonDeviceClaimed)
			case dev.SourceRevision > sc.SourceRevision || (dev.ScanID != "" && dev.ScanID != sc.Identifier && dev.SourceRevision >= sc.SourceRevision):
				item.Reasons = append(item.Reasons, types.ReasonDeviceNewerScan)
			}
		}
	} else {
		item.Reasons = append(item.Reasons, types.ReasonDeviceMissing)
	}

	for _, target := range targets {
		switch target {
		case types.TargetDHCP:
			if snap.MAC == "" {
				item.Reasons = append(item.Reasons, types.ReasonMACRequired)
				item.Steps = append(item.Steps, APIApplyPreviewStep{
					Name:   types.StepNameDHCPReservation,
					Action: roles.EnsureActionSkip,
					Reason: types.ReasonMACRequired,
				})
				continue
			}
			step := r.previewLeaseStep(ctx, snap, dhcpScope)
			item.Steps = append(item.Steps, step)
			if step.Reason != "" {
				item.Reasons = append(item.Reasons, step.Reason)
			}
		case types.TargetDNS:
			fwd, rev, ok := r.previewDNSSteps(ctx, snap, dnsZone)
			if !ok {
				item.Reasons = append(item.Reasons, types.ReasonSnapshotMismatch)
				continue
			}
			item.Steps = append(item.Steps, fwd, rev)
			if fwd.Reason != "" {
				item.Reasons = append(item.Reasons, fwd.Reason)
			}
			if rev.Reason != "" {
				item.Reasons = append(item.Reasons, rev.Reason)
			}
		}
	}

	item.Executable = len(item.Reasons) == 0
	return item
}

func (r *Role) previewLeaseStep(ctx context.Context, snap ScanDevice, scopeName string) APIApplyPreviewStep {
	step := APIApplyPreviewStep{Name: types.StepNameDHCPReservation}
	ev := roles.NewEvent(ctx, map[string]interface{}{
		types.EventFieldDryRun:   true,
		types.EventFieldMAC:      snap.MAC,
		types.EventFieldHostname: shortHostname(snap.Hostname),
		types.EventFieldAddress:  snap.IP,
		types.EventFieldScope:    scopeName,
	})
	r.i.DispatchEvent(dhcptypes.EventTopicDHCPLeaseEnsure, ev)
	outcomes := ev.Outcomes()
	if len(outcomes) == 0 {
		step.Action = roles.EnsureActionSkip
		step.Reason = types.ReasonRoleUnavailable
		step.TargetKey = r.i.KV().Key("dhcp", "leases", snap.MAC).String()
		return step
	}
	o := outcomes[0]
	step.TargetKey = o.TargetKey
	step.Action = o.Action
	step.Reason = o.Reason
	step.CreateRev = o.CreateRevision
	step.ModRev = o.ModRevision
	return step
}

func (r *Role) previewDNSSteps(ctx context.Context, snap ScanDevice, zone string) (fwd APIApplyPreviewStep, rev APIApplyPreviewStep, ok bool) {
	fqdn, hostname, err := buildFQDN(snap.Hostname, zone)
	if err != nil {
		return fwd, rev, false
	}
	dispatch := func(direction string) roles.EnsureOutcome {
		ev := roles.NewEvent(ctx, map[string]interface{}{
			types.EventFieldDryRun:     true,
			types.EventFieldDirection:  direction,
			types.EventFieldFQDN:       fqdn,
			types.EventFieldHostname:   hostname,
			types.EventFieldAddress:    snap.IP,
			types.EventFieldIdentifier: snap.Identifier,
		})
		r.i.DispatchEvent(dnstypes.EventTopicDNSRecordEnsure, ev)
		outcomes := ev.Outcomes()
		if len(outcomes) == 0 {
			name := types.StepNameDNSForward
			if direction == types.EventDirectionReverse {
				name = types.StepNameDNSReverse
			}
			return roles.EnsureOutcome{
				Step:   name,
				Action: roles.EnsureActionSkip,
				Reason: types.ReasonRoleUnavailable,
			}
		}
		return outcomes[0]
	}
	o := dispatch(types.EventDirectionForward)
	fwd = APIApplyPreviewStep{
		Name:      o.Step,
		TargetKey: o.TargetKey,
		Action:    o.Action,
		Reason:    o.Reason,
		Siblings:  o.Siblings,
		CreateRev: o.CreateRevision,
		ModRev:    o.ModRevision,
	}
	o = dispatch(types.EventDirectionReverse)
	rev = APIApplyPreviewStep{
		Name:      o.Step,
		TargetKey: o.TargetKey,
		Action:    o.Action,
		Reason:    o.Reason,
		Siblings:  o.Siblings,
		CreateRev: o.CreateRevision,
		ModRev:    o.ModRevision,
	}
	return fwd, rev, true
}

// buildFQDN mirrors the legacy Device.toDNS FQDN derivation.
func buildFQDN(deviceHostname, zone string) (fqdn, hostname string, err error) {
	parts := strings.Split(deviceHostname, ".")
	hostname = parts[0]
	if zone != "" {
		return hostname + "." + strings.TrimSuffix(zone, "."), hostname, nil
	}
	if len(parts) == 1 {
		return "", "", errHostnameNoDomain
	}
	return deviceHostname, hostname, nil
}

func shortHostname(h string) string {
	return strings.Split(h, ".")[0]
}

// findScan locates an immutable scan metadata key by scanID across subnets.
func (r *Role) findScan(ctx context.Context, scanID string) (*Scan, error) {
	prefix := r.i.KV().Key(types.KeyRole, types.KeyScans).Prefix(true).String()
	res, err := r.i.KV().Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	for _, kv := range res.Kvs {
		rel := trimKey(string(kv.Key), prefix)
		parts := strings.SplitN(rel, "/", 3)
		if len(parts) != 2 || parts[1] != scanID {
			continue
		}
		return r.scanFromKV(kv)
	}
	return nil, errScanNotFound
}

// listScanDevices returns all immutable snapshots of one scan.
func (r *Role) listScanDevices(ctx context.Context, subnet, scanID string) ([]ScanDevice, error) {
	prefix := r.i.KV().Key(
		types.KeyRole, types.KeyScans, subnet, scanID, "devices",
	).Prefix(true).String()
	res, err := r.i.KV().Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	out := []ScanDevice{}
	for _, kv := range res.Kvs {
		var snap ScanDevice
		if err := json.Unmarshal(kv.Value, &snap); err != nil {
			r.log.Warn("failed to parse scan device snapshot", zap.ByteString("key", kv.Key), zap.Error(err))
			continue
		}
		out = append(out, snap)
	}
	return out, nil
}
