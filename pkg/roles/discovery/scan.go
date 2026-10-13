package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/roles"
	"beryju.io/gravity/pkg/roles/discovery/types"
	"github.com/google/uuid"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

var (
	errScanMetaExists      = errors.New("scan metadata already exists")
	errInvalidScanStatus   = errors.New("invalid scan target status")
	errScanStaleTransition = errors.New("scan metadata changed since transition started")
)

// staleScanAfter is how long a scan may stay in "running" before the role
// startup reaper marks it as failed.
const staleScanAfter = 2 * time.Hour

// Scan is the immutable metadata of one subnet discovery round.
// Running -> finished/failed is the only allowed transition, guarded by a
// compare-and-swap; finished/failed records are never written again.
type Scan struct {
	inst roles.Instance `json:"-"`
	log  *zap.Logger    `json:"-"`
	role *Role          `json:"-"`

	// Identifier is the scanID
	Identifier string `json:"-"`

	Subnet         string    `json:"subnet"`
	CIDR           string    `json:"cidr"`
	SourceRevision int64     `json:"sourceRevision"`
	Status         string    `json:"status"`
	NodeID         string    `json:"nodeId,omitempty"`
	StartedAt      time.Time `json:"startedAt"`
	FinishedAt     time.Time `json:"finishedAt,omitempty"`
	DeviceCount    int       `json:"deviceCount"`

	etcdKey string `json:"-"`
	// modRev is the ModRevision of the running meta, used as the CAS guard
	// for the one-time lifecycle flip.
	modRev int64 `json:"-"`
}

// ScanDevice is one immutable per-device observation of a scan round.
type ScanDevice struct {
	Identifier  string    `json:"identifier"`
	Hostname    string    `json:"hostname"`
	IP          string    `json:"ip"`
	MAC         string    `json:"mac"`
	Subnet      string    `json:"subnet"`
	ScanID      string    `json:"scanId"`
	SourceRev   int64     `json:"sourceRevision"`
	ObservedAt  time.Time `json:"observedAt"`
	ContentHash string    `json:"contentHash"`
}

func (r *Role) newScan(subnetName, cidr string) *Scan {
	return &Scan{
		Identifier: uuid.New().String(),
		Subnet:     subnetName,
		CIDR:       cidr,
		Status:     types.ScanStatusRunning,
		NodeID:     extconfig.Get().Instance.Identifier,
		StartedAt:  time.Now().UTC(),
		inst:       r.i,
		log:        r.log.With(zap.String("subnet", subnetName)),
		role:       r,
	}
}

func (r *Role) scanMetaKey(subnetName, scanID string) string {
	return r.i.KV().Key(types.KeyRole, types.KeyScans, subnetName, scanID).String()
}

func (r *Role) scanDeviceKey(subnetName, scanID, identifier string) string {
	return r.i.KV().Key(
		types.KeyRole, types.KeyScans, subnetName, scanID, "devices", identifier,
	).String()
}

func (r *Role) scanFromKV(kv *mvccpb.KeyValue) (*Scan, error) {
	sc := &Scan{}
	if err := json.Unmarshal(kv.Value, sc); err != nil {
		return nil, err
	}
	sc.inst = r.i
	sc.role = r
	sc.log = r.log
	sc.etcdKey = string(kv.Key)
	sc.modRev = kv.ModRevision
	if sc.Identifier == "" {
		prefix := r.i.KV().Key(types.KeyRole, types.KeyScans, sc.Subnet).Prefix(true).String()
		sc.Identifier = trimKey(string(kv.Key), prefix)
	}
	return sc, nil
}

func trimKey(full, prefix string) string {
	out := full
	if len(prefix) > 0 && len(out) >= len(prefix) && out[:len(prefix)] == prefix {
		out = out[len(prefix):]
	}
	for len(out) > 0 && out[0] == '/' {
		out = out[1:]
	}
	return out
}

// beginScan captures the etcd source revision and the live-device baseline,
// grants the shared TTL lease for this scan's immutable results and creates
// the running scan meta (create-only CAS).
func (r *Role) beginScan(ctx context.Context, s *Subnet) (*Scan, map[string]*mvccpb.KeyValue, clientv3.LeaseID, error) {
	// One serializable read captures both the source revision and the
	// baseline revisions of the current live device projection.
	devicesPrefix := r.i.KV().Key(types.KeyRole, types.KeyDevices).Prefix(true).String()
	baselineResp, err := r.i.KV().Get(ctx, devicesPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, nil, 0, err
	}
	baseline := make(map[string]*mvccpb.KeyValue)
	for _, kv := range baselineResp.Kvs {
		baseline[trimKey(string(kv.Key), devicesPrefix)] = kv
	}

	leaseID := clientv3.LeaseID(0)
	if s.DiscoveryTTL > 0 {
		grant, err := r.i.KV().Grant(ctx, int64(s.DiscoveryTTL))
		if err != nil {
			return nil, nil, 0, err
		}
		leaseID = grant.ID
	}

	sc := r.newScan(s.Identifier, s.CIDR)
	sc.SourceRevision = baselineResp.Header.Revision
	raw, err := json.Marshal(sc)
	if err != nil {
		return nil, nil, 0, err
	}
	metaKey := r.scanMetaKey(sc.Subnet, sc.Identifier)
	res, err := r.i.KV().Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(metaKey), "=", 0)).
		Then(clientv3.OpPut(metaKey, string(raw))).
		Commit()
	if err != nil {
		return nil, nil, 0, err
	}
	if !res.Succeeded {
		return nil, nil, 0, errScanMetaExists
	}
	sc.modRev = res.Header.Revision
	return sc, baseline, leaseID, nil
}

// finish performs the one-time running -> finished/failed transition and
// attaches the scan TTL lease so immutable results expire with the subnet's
// DiscoveryTTL.
func (sc *Scan) finish(ctx context.Context, status string, deviceCount int, leaseID clientv3.LeaseID) error {
	if status != types.ScanStatusFinished && status != types.ScanStatusFailed {
		return errInvalidScanStatus
	}
	sc.Status = status
	sc.DeviceCount = deviceCount
	sc.FinishedAt = time.Now().UTC()
	raw, err := json.Marshal(sc)
	if err != nil {
		return err
	}
	metaKey := sc.role.scanMetaKey(sc.Subnet, sc.Identifier)
	opts := []clientv3.OpOption{}
	if leaseID != 0 {
		opts = append(opts, clientv3.WithLease(leaseID))
	}
	res, err := sc.inst.KV().Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(metaKey), "=", sc.modRev)).
		Then(clientv3.OpPut(metaKey, string(raw), opts...)).
		Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		return errScanStaleTransition
	}
	return nil
}

// persistScanResults writes immutable per-device snapshots (create-only CAS)
// and conditionally projects observations into the live device view.
//
// The projection never overwrites: manually sourced devices, devices already
// claimed by an apply job, devices produced by a newer scan, or devices whose
// live revision moved during the scan. Snapshots are always written,
// regardless of whether the projection is applied.
func (r *Role) persistScanResults(
	ctx context.Context,
	sc *Scan,
	devices []Device,
	baseline map[string]*mvccpb.KeyValue,
	leaseID clientv3.LeaseID,
) int {
	observedAt := time.Now().UTC()
	written := 0
	for idx := range devices {
		dev := devices[idx]
		if dev.Identifier == "" {
			continue
		}
		dev.Source = types.DeviceSourceScan
		dev.ScanID = sc.Identifier
		dev.SourceRevision = sc.SourceRevision
		dev.Subnet = sc.Subnet
		dev.ContentHash = DeviceContentHash(dev.Hostname, dev.IP, dev.MAC, dev.Subnet)

		snapshot := ScanDevice{
			Identifier:  dev.Identifier,
			Hostname:    dev.Hostname,
			IP:          dev.IP,
			MAC:         dev.MAC,
			Subnet:      sc.Subnet,
			ScanID:      sc.Identifier,
			SourceRev:   sc.SourceRevision,
			ObservedAt:  observedAt,
			ContentHash: dev.ContentHash,
		}
		r.writeSnapshot(ctx, sc, dev.Identifier, snapshot, leaseID)
		if r.projectDevice(ctx, sc, &dev, baseline[dev.Identifier], leaseID) {
			written++
		}
		r.dispatchDeviceFound(ctx, &dev)
	}
	return written
}

func (r *Role) writeSnapshot(ctx context.Context, sc *Scan, identifier string, snapshot ScanDevice, leaseID clientv3.LeaseID) {
	raw, err := json.Marshal(&snapshot)
	if err != nil {
		r.log.Warn("failed to marshal scan snapshot", zap.Error(err))
		return
	}
	key := r.scanDeviceKey(sc.Subnet, sc.Identifier, identifier)
	opts := []clientv3.OpOption{}
	if leaseID != 0 {
		opts = append(opts, clientv3.WithLease(leaseID))
	}
	res, err := r.i.KV().Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(raw), opts...)).
		Commit()
	if err != nil {
		r.log.Warn("failed to persist scan snapshot", zap.String("key", key), zap.Error(err))
		return
	}
	if !res.Succeeded {
		// Snapshot already exists for this (scan, identifier): keep the
		// immutable original, never overwrite.
		r.log.Debug("scan snapshot already exists, keeping immutable original", zap.String("key", key))
	}
}

// projectDevice conditionally updates the live /discovery/devices/{id}
// projection. Returns true when the projection was created or updated.
func (r *Role) projectDevice(
	ctx context.Context,
	sc *Scan,
	dev *Device,
	existing *mvccpb.KeyValue,
	leaseID clientv3.LeaseID,
) bool {
	raw, err := json.Marshal(dev)
	if err != nil {
		r.log.Warn("failed to marshal device projection", zap.Error(err))
		return false
	}
	key := r.i.KV().Key(types.KeyRole, types.KeyDevices, dev.Identifier).String()
	leaseOpt := []clientv3.OpOption{}
	if leaseID != 0 {
		leaseOpt = append(leaseOpt, clientv3.WithLease(leaseID))
	}

	if existing == nil {
		// Baseline said absent: only create if it is still absent. A manual
		// create racing the scan wins.
		res, err := r.i.KV().Txn(ctx).
			If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, string(raw), leaseOpt...)).
			Commit()
		if err != nil {
			r.log.Warn("failed to project new device", zap.String("key", key), zap.Error(err))
			return false
		}
		return res.Succeeded
	}

	current := r.deviceFromKV(existing)
	if !projectable(current, sc) {
		return false
	}
	// Preserve the device's existing TTL lease (if any) instead of replacing
	// it with the fresh scan lease.
	putOpts := leaseOpt
	if existing.Lease != 0 {
		putOpts = []clientv3.OpOption{clientv3.WithLease(clientv3.LeaseID(existing.Lease))}
	}
	res, err := r.i.KV().Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", existing.ModRevision)).
		Then(clientv3.OpPut(key, string(raw), putOpts...)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		r.log.Warn("failed to project device update", zap.String("key", key), zap.Error(err))
		return false
	}
	if res.Succeeded {
		return true
	}
	// Revision moved during the scan: only retry the projection if the new
	// owner is still a plain older-scan projection. Manual edits, claimed
	// devices and newer scans always win.
	if len(res.Responses) == 0 || res.Responses[0].GetResponseRange() == nil {
		return false
	}
	kvs := res.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		// Device was deleted manually during the scan; do not resurrect.
		return false
	}
	latest := r.deviceFromKV(kvs[0])
	if !projectable(latest, sc) {
		r.log.Info("not overwriting device changed during scan",
			zap.String("identifier", dev.Identifier),
			zap.String("source", latest.Source),
			zap.String("scanId", latest.ScanID))
		return false
	}
	res, err = r.i.KV().Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", kvs[0].ModRevision)).
		Then(clientv3.OpPut(key, string(raw), putOpts...)).
		Commit()
	if err != nil {
		r.log.Warn("failed to re-project device update", zap.String("key", key), zap.Error(err))
		return false
	}
	return res.Succeeded
}

// projectable reports whether a scan may overwrite the current live device.
// A nil/unparseable device is left untouched.
func projectable(current *Device, sc *Scan) bool {
	if current == nil {
		return false
	}
	if current.Source == types.DeviceSourceManual {
		return false
	}
	if current.ApplyID != "" {
		return false
	}
	if current.SourceRevision > sc.SourceRevision {
		return false
	}
	return true
}

func (r *Role) dispatchDeviceFound(ctx context.Context, dev *Device) {
	key := r.i.KV().Key(types.KeyRole, types.KeyDevices, dev.Identifier)
	ev := roles.NewEvent(ctx, map[string]interface{}{
		"device": dev,
	})
	ev.Payload.RelatedObjectKey = key
	r.i.DispatchEvent(types.EventTopicDiscoveryDeviceFound, ev)
}

// reapStaleScans flips scan metas that have been in "running" for longer than
// staleScanAfter (e.g. the scanning node crashed) to "failed". It never
// touches finished/failed scans.
func (r *Role) reapStaleScans(ctx context.Context) {
	prefix := r.i.KV().Key(types.KeyRole, types.KeyScans).Prefix(true).String()
	res, err := r.i.KV().Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		r.log.Warn("failed to list scans for reaping", zap.Error(err))
		return
	}
	cutoff := time.Now().UTC().Add(-staleScanAfter)
	for _, kv := range res.Kvs {
		// Meta keys live directly below {prefix}/{subnet}/{scanID}; the
		// immutable device snapshots are nested deeper.
		rel := trimKey(string(kv.Key), prefix)
		if countSlashes(rel) != 1 {
			continue
		}
		sc, err := r.scanFromKV(kv)
		if err != nil {
			continue
		}
		if sc.Status != types.ScanStatusRunning || sc.StartedAt.After(cutoff) {
			continue
		}
		sc.Status = types.ScanStatusFailed
		sc.FinishedAt = time.Now().UTC()
		raw, err := json.Marshal(sc)
		if err != nil {
			continue
		}
		// Attach the subnet TTL so the reaped metadata expires too.
		leaseOpt := []clientv3.OpOption{}
		if ttl := r.subnetTTL(ctx, sc.Subnet); ttl > 0 {
			if grant, grantErr := r.i.KV().Grant(ctx, ttl); grantErr == nil {
				leaseOpt = append(leaseOpt, clientv3.WithLease(grant.ID))
			}
		}
		_, err = r.i.KV().Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(string(kv.Key)), "=", kv.ModRevision)).
			Then(clientv3.OpPut(string(kv.Key), string(raw), leaseOpt...)).
			Commit()
		if err != nil {
			r.log.Warn("failed to reap stale scan", zap.String("scan", sc.Identifier), zap.Error(err))
		}
	}
}

// subnetTTL reads the configured DiscoveryTTL for a subnet (0 when missing).
func (r *Role) subnetTTL(ctx context.Context, subnetName string) int64 {
	res, err := r.i.KV().Get(ctx, r.i.KV().Key(types.KeyRole, "subnets", subnetName).String())
	if err != nil || len(res.Kvs) == 0 {
		return 0
	}
	sub, err := r.subnetFromKV(res.Kvs[0])
	if err != nil || sub == nil {
		return 0
	}
	return int64(sub.DiscoveryTTL)
}

func countSlashes(s string) int {
	n := 0
	for _, c := range s {
		if c == '/' {
			n++
		}
	}
	return n
}
