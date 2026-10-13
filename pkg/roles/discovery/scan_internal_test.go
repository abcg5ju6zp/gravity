package discovery

import (
	"encoding/json"
	"testing"
	"time"

	"beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func kvGet(t *testing.T, r *Role, key string) *clientv3.GetResponse {
	t.Helper()
	res, err := r.i.KV().Get(tests.Context(), key)
	assert.NoError(t, err)
	return res
}

func beginTestScan(t *testing.T, r *Role, ttl int) (*Scan, map[string]*mvccpb.KeyValue, clientv3.LeaseID) {
	t.Helper()
	sc, baseline, leaseID, err := r.beginScan(tests.Context(), testSubnet(r, ttl))
	assert.NoError(t, err)
	return sc, baseline, leaseID
}

// TestScanLifecycleAndSnapshots covers TR-2.1/2.2/2.5 basics.
func TestScanLifecycleAndSnapshots(t *testing.T) {
	tests.Setup(t)
	ctx := tests.Context()
	r := newTestRole(t)

	sc, baseline, leaseID := beginTestScan(t, r, 86400)
	assert.NotEmpty(t, sc.Identifier)
	assert.Positive(t, sc.SourceRevision)
	assert.Equal(t, types.ScanStatusRunning, sc.Status)

	// Running meta exists and carries no lease yet.
	metaKey := r.scanMetaKey(sc.Subnet, sc.Identifier)
	running := kvGet(t, r, metaKey)
	if assert.Len(t, running.Kvs, 1) {
		assert.Equal(t, int64(0), running.Kvs[0].Lease)
	}
	if _, exists := baseline["192.0.2.1"]; exists {
		t.Fatal("fresh baseline should not contain the device")
	}

	devices := []Device{
		{Identifier: "192.0.2.1", IP: "192.0.2.1", Hostname: "host-a", MAC: "aa:bb:cc:00:00:01"},
		{Identifier: "192.0.2.2", IP: "192.0.2.2", Hostname: "host-b", MAC: "aa:bb:cc:00:00:02"},
	}
	count := r.persistScanResults(ctx, sc, devices, baseline, leaseID)
	assert.Equal(t, 2, count)

	assert.NoError(t, sc.finish(ctx, types.ScanStatusFinished, len(devices), leaseID))

	// Finished meta.
	finishedKV := kvGet(t, r, metaKey).Kvs
	if assert.Len(t, finishedKV, 1) {
		var meta Scan
		assert.NoError(t, json.Unmarshal(finishedKV[0].Value, &meta))
		assert.Equal(t, types.ScanStatusFinished, meta.Status)
		assert.Equal(t, 2, meta.DeviceCount)
		assert.Equal(t, int64(leaseID), finishedKV[0].Lease, "finished meta must be TTL retained")
	}

	// Snapshot exists, immutable, and carries identity/hash.
	snapKey := r.scanDeviceKey(sc.Subnet, sc.Identifier, "192.0.2.1")
	snapRes := kvGet(t, r, snapKey)
	if assert.Len(t, snapRes.Kvs, 1) {
		var snap ScanDevice
		assert.NoError(t, json.Unmarshal(snapRes.Kvs[0].Value, &snap))
		assert.Equal(t, sc.Identifier, snap.ScanID)
		assert.Equal(t, sc.SourceRevision, snap.SourceRev)
		assert.Equal(t, "host-a", snap.Hostname)
		expectedHash := DeviceContentHash("host-a", "192.0.2.1", "aa:bb:cc:00:00:01", sc.Subnet)
		assert.Equal(t, expectedHash, snap.ContentHash)
		assert.Equal(t, int64(leaseID), snapRes.Kvs[0].Lease, "snapshot must inherit DiscoveryTTL")
	}

	// TR-2.2: replaying persistence keeps the immutable snapshot and the
	// second projection is rejected (key now exists outside baseline).
	before := kvGet(t, r, snapKey).Kvs[0]
	count2 := r.persistScanResults(ctx, sc, devices, baseline, leaseID)
	assert.Equal(t, 0, count2, "replayed scan must not re-project existing devices")
	after := kvGet(t, r, snapKey).Kvs[0]
	assert.Equal(t, before.ModRevision, after.ModRevision)
	assert.Equal(t, before.Value, after.Value)

	// The lifecycle flip is one-shot.
	assert.ErrorIs(t, sc.finish(ctx, types.ScanStatusFinished, 2, leaseID), errScanStaleTransition)
}

func TestProjectionSkipsProtectedDevices(t *testing.T) {
	tests.Setup(t)
	ctx := tests.Context()
	r := newTestRole(t)

	putDevice := func(id string, dev Device) {
		raw, err := json.Marshal(&dev)
		assert.NoError(t, err)
		_, err = r.i.KV().Put(ctx, r.i.KV().Key(types.KeyRole, types.KeyDevices, id).String(), string(raw))
		assert.NoError(t, err)
	}

	scanned := func(id string) Device {
		return Device{
			Identifier:     id,
			IP:             id,
			Hostname:       "old",
			MAC:            "11:11:11:11:11:11",
			Source:         types.DeviceSourceScan,
			ScanID:         "old-scan",
			SourceRevision: 5,
		}
	}

	// (a) manually sourced device
	manual := scanned("192.0.2.10")
	manual.Source = types.DeviceSourceManual
	putDevice("192.0.2.10", manual)

	// (b) device from a newer scan (source revision above any rev this scan
	// could have captured, e.g. an overlapping scan of another subnet)
	newer := scanned("192.0.2.50")
	newer.SourceRevision = 9_000_000_000_000_000_000
	newer.ScanID = "future-scan"
	putDevice("192.0.2.50", newer)

	// (c) device already claimed by an apply job
	claimed := scanned("192.0.2.20")
	claimed.ApplyID = "apply-existing"
	putDevice("192.0.2.20", claimed)

	// (d) plain scanned device that is manually edited during the scan
	edited := scanned("192.0.2.30")
	putDevice("192.0.2.30", edited)

	// (e) plain scanned device deleted during the scan
	deleted := scanned("192.0.2.40")
	putDevice("192.0.2.40", deleted)

	sc, baseline, leaseID, err := r.beginScan(ctx, testSubnet(r, 86400))
	assert.NoError(t, err)

	// Baseline captured all five.
	for _, id := range []string{"192.0.2.10", "192.0.2.20", "192.0.2.30", "192.0.2.40", "192.0.2.50"} {
		assert.Contains(t, baseline, id)
	}

	// Mutations happening while nmap is running:
	// (d) edit device to a manual record
	editedNew := manual
	editedNew.Identifier = "192.0.2.30"
	editedNew.Source = types.DeviceSourceManual
	editedNew.Hostname = "manual-renamed"
	putDevice("192.0.2.30", editedNew)
	// (e) manual delete
	_, err = r.i.KV().Delete(ctx, r.i.KV().Key(types.KeyRole, types.KeyDevices, "192.0.2.40").String())
	assert.NoError(t, err)

	observed := []Device{
		{Identifier: "192.0.2.10", IP: "192.0.2.10", Hostname: "scan-a", MAC: "aa"},
		{Identifier: "192.0.2.20", IP: "192.0.2.20", Hostname: "scan-b", MAC: "bb"},
		{Identifier: "192.0.2.30", IP: "192.0.2.30", Hostname: "scan-c", MAC: "cc"},
		{Identifier: "192.0.2.40", IP: "192.0.2.40", Hostname: "scan-d", MAC: "dd"},
		{Identifier: "192.0.2.50", IP: "192.0.2.50", Hostname: "scan-e", MAC: "ee"},
	}
	// Revisions of the protected devices as they are just before projection.
	revisionBefore := func(id string) int64 {
		res := kvGet(t, r, r.i.KV().Key(types.KeyRole, types.KeyDevices, id).String())
		if len(res.Kvs) == 0 {
			return 0
		}
		return res.Kvs[0].ModRevision
	}
	unchangedRevs := map[string]int64{
		"192.0.2.10": revisionBefore("192.0.2.10"),
		"192.0.2.20": revisionBefore("192.0.2.20"),
		"192.0.2.30": revisionBefore("192.0.2.30"),
		"192.0.2.50": revisionBefore("192.0.2.50"),
	}

	projected := r.persistScanResults(ctx, sc, observed, baseline, leaseID)
	assert.Equal(t, 0, projected, "all four devices must be protected from the scan")

	// Their revisions must not move.
	for id, rev := range unchangedRevs {
		assert.Equal(t, rev, revisionBefore(id), "device %s rewritten by scan", id)
	}

	// Live values unchanged.
	checks := map[string]string{
		"192.0.2.10": "old",
		"192.0.2.20": "old",
		"192.0.2.30": "manual-renamed",
	}
	for id, hostname := range checks {
		res := kvGet(t, r, r.i.KV().Key(types.KeyRole, types.KeyDevices, id).String())
		if assert.Len(t, res.Kvs, 1, id) {
			var d Device
			assert.NoError(t, json.Unmarshal(res.Kvs[0].Value, &d))
			assert.Equal(t, hostname, d.Hostname, id)
		}
	}
	// Deleted device is not resurrected.
	assert.Len(t, kvGet(t, r, r.i.KV().Key(types.KeyRole, types.KeyDevices, "192.0.2.40").String()).Kvs, 0)

	// The newer-scan device keeps its projection.
	if res := kvGet(t, r, r.i.KV().Key(types.KeyRole, types.KeyDevices, "192.0.2.50").String()); assert.Len(t, res.Kvs, 1) {
		var d Device
		assert.NoError(t, json.Unmarshal(res.Kvs[0].Value, &d))
		assert.Equal(t, "future-scan", d.ScanID)
		assert.Equal(t, "old", d.Hostname)
	}

	// Snapshots are still written for every observation.
	for _, id := range []string{"192.0.2.10", "192.0.2.20", "192.0.2.30", "192.0.2.40", "192.0.2.50"} {
		assert.Len(t, kvGet(t, r, r.scanDeviceKey(sc.Subnet, sc.Identifier, id)).Kvs, 1, id)
	}
}

func TestProjectionPreservesTTLAndUpdatesSameScanDevices(t *testing.T) {
	tests.Setup(t)
	ctx := tests.Context()
	r := newTestRole(t)

	// Existing plain scan device with its own TTL lease.
	old := Device{
		Identifier:     "192.0.2.1",
		IP:             "192.0.2.1",
		Hostname:       "old-name",
		MAC:            "aa:bb:cc:00:00:01",
		Source:         types.DeviceSourceScan,
		ScanID:         "older-scan",
		SourceRevision: 1,
	}
	raw, err := json.Marshal(&old)
	assert.NoError(t, err)
	preserveGrant, err := r.i.KV().Grant(ctx, 120)
	assert.NoError(t, err)
	oldKey := r.i.KV().Key(types.KeyRole, types.KeyDevices, "192.0.2.1").String()
	_, err = r.i.KV().Put(ctx, oldKey, string(raw), clientv3.WithLease(preserveGrant.ID))
	assert.NoError(t, err)

	sc, baseline, leaseID, err := r.beginScan(ctx, testSubnet(r, 86400))
	assert.NoError(t, err)
	assert.Contains(t, baseline, "192.0.2.1")

	devices := []Device{{Identifier: "192.0.2.1", IP: "192.0.2.1", Hostname: "new-name", MAC: "aa:bb:cc:00:00:01"}}
	assert.Equal(t, 1, r.persistScanResults(ctx, sc, devices, baseline, leaseID))

	updated := kvGet(t, r, oldKey)
	if assert.Len(t, updated.Kvs, 1) {
		// TR-2.4: original lease preserved.
		assert.Equal(t, int64(preserveGrant.ID), updated.Kvs[0].Lease)
		var d Device
		assert.NoError(t, json.Unmarshal(updated.Kvs[0].Value, &d))
		assert.Equal(t, "new-name", d.Hostname)
		assert.Equal(t, sc.Identifier, d.ScanID)
		assert.Equal(t, sc.SourceRevision, d.SourceRevision)
		assert.Equal(t, types.DeviceSourceScan, d.Source)
	}

	// A brand new device gets the scan lease.
	fresh := []Device{{Identifier: "192.0.2.99", IP: "192.0.2.99", Hostname: "fresh", MAC: "dd"}}
	assert.Equal(t, 1, r.persistScanResults(ctx, sc, fresh, baseline, leaseID))
	freshKV := kvGet(t, r, r.i.KV().Key(types.KeyRole, types.KeyDevices, "192.0.2.99").String())
	if assert.Len(t, freshKV.Kvs, 1) {
		assert.Equal(t, int64(leaseID), freshKV.Kvs[0].Lease)
	}
}

func TestReapStaleScans(t *testing.T) {
	tests.Setup(t)
	ctx := tests.Context()
	r := newTestRole(t)
	s := testSubnet(r, 86400)

	putMeta := func(scanID string, status string, started time.Time) string {
		sc := r.newScan(s.Identifier, s.CIDR)
		sc.Identifier = scanID
		sc.Status = status
		sc.StartedAt = started
		raw, err := json.Marshal(sc)
		assert.NoError(t, err)
		key := r.scanMetaKey(s.Identifier, scanID)
		_, err = r.i.KV().Put(ctx, key, string(raw))
		assert.NoError(t, err)
		return key
	}

	staleKey := putMeta("stale-scan", types.ScanStatusRunning, time.Now().UTC().Add(-3*time.Hour))
	freshKey := putMeta("fresh-scan", types.ScanStatusRunning, time.Now().UTC())
	finishedKey := putMeta("done-scan", types.ScanStatusFinished, time.Now().UTC().Add(-3*time.Hour))

	r.reapStaleScans(ctx)

	var stale Scan
	assert.NoError(t, json.Unmarshal(kvGet(t, r, staleKey).Kvs[0].Value, &stale))
	assert.Equal(t, types.ScanStatusFailed, stale.Status)

	var fresh Scan
	assert.NoError(t, json.Unmarshal(kvGet(t, r, freshKey).Kvs[0].Value, &fresh))
	assert.Equal(t, types.ScanStatusRunning, fresh.Status)

	var done Scan
	assert.NoError(t, json.Unmarshal(kvGet(t, r, finishedKey).Kvs[0].Value, &done))
	assert.Equal(t, types.ScanStatusFinished, done.Status)
}
