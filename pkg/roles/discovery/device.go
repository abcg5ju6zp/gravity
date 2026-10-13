package discovery

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"beryju.io/gravity/pkg/roles"
	dhcptypes "beryju.io/gravity/pkg/roles/dhcp/types"
	"beryju.io/gravity/pkg/roles/discovery/types"
	dnstypes "beryju.io/gravity/pkg/roles/dns/types"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

type Device struct {
	inst       roles.Instance
	Identifier string `json:"-"`

	Hostname string `json:"hostname"`
	IP       string `json:"ip"`
	MAC      string `json:"mac"`

	// Provenance of the latest live projection. Empty/legacy values are
	// treated as scan-sourced for backwards compatibility.
	Source         string `json:"source,omitempty"`
	ScanID         string `json:"scanId,omitempty"`
	SourceRevision int64  `json:"sourceRevision,omitempty"`
	Subnet         string `json:"subnet,omitempty"`
	ContentHash    string `json:"contentHash,omitempty"`

	// ApplyID/ApplyStatus are set when a discovery apply job marks the
	// device as provisioned. Claimed devices are never overwritten by scan
	// projections.
	ApplyID     string     `json:"applyId,omitempty"`
	ApplyStatus string     `json:"applyStatus,omitempty"`
	AppliedAt   *time.Time `json:"appliedAt,omitempty"`
}

// DeviceContentHash is the stable hash pinned by apply previews and apply
// job creation. It covers exactly the identifying device attributes.
func DeviceContentHash(hostname, ip, mac, subnet string) string {
	sum := sha256.Sum256([]byte(strings.Join([]string{hostname, ip, mac, subnet}, "\x00")))
	return hex.EncodeToString(sum[:])
}

func (r *Role) newDevice() *Device {
	return &Device{
		inst: r.i,
	}
}

func (r *Role) deviceFromKV(kv *mvccpb.KeyValue) *Device {
	prefix := r.i.KV().Key(types.KeyRole, types.KeyDevices).Prefix(true).String()
	identifier := strings.TrimPrefix(string(kv.Key), prefix)
	dev := r.newDevice()
	dev.Identifier = identifier

	err := json.Unmarshal(kv.Value, &dev)
	if err != nil {
		r.log.Warn("failed to parse device", zap.Error(err))
		return nil
	}
	return dev
}

func (d *Device) toDNS(ctx context.Context, zone string) error {
	hostname := strings.Split(d.Hostname, ".")
	fqdn := d.Hostname
	if zone != "" {
		fqdn = hostname[0] + "." + zone
	}
	if zone == "" && len(hostname) == 1 {
		return errors.New("device hostname has no domain and no zone given")
	}
	d.inst.DispatchEvent(dnstypes.EventTopicDNSRecordCreateForward, roles.NewEvent(ctx, map[string]interface{}{
		"fqdn":       fqdn,
		"hostname":   hostname[0],
		"address":    d.IP,
		"identifier": d.Identifier,
		"expiry":     int64(-1),
	}))
	d.inst.DispatchEvent(dnstypes.EventTopicDNSRecordCreateReverse, roles.NewEvent(ctx, map[string]interface{}{
		"fqdn":       fqdn,
		"address":    d.IP,
		"identifier": d.Identifier,
		"expiry":     int64(-1),
	}))
	// Maybe delete device? Mark as applied?
	return nil
}

func (d *Device) toDHCP(ctx context.Context, scope string) error {
	if scope == "" {
		return errors.New("blank scope")
	}
	if d.MAC == "" {
		return errors.New("mac address blank")
	}
	hostname := strings.Split(d.Hostname, ".")[0]
	d.inst.DispatchEvent(dhcptypes.EventTopicDHCPCreateLease, roles.NewEvent(ctx, map[string]interface{}{
		"mac":      d.MAC,
		"hostname": hostname,
		"address":  d.IP,
		"scope":    scope,
	}))
	return nil
}
