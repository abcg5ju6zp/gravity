package discovery_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles/dhcp"
	dhcpTypes "beryju.io/gravity/pkg/roles/dhcp/types"
	"beryju.io/gravity/pkg/roles/discovery"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/roles/dns"
	dnsTypes "beryju.io/gravity/pkg/roles/dns/types"
	"beryju.io/gravity/pkg/storage"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func mustGetKV(t *testing.T, c *storage.Client, ctx context.Context, key string) []*mvccpb.KeyValue {
	t.Helper()
	res, err := c.Get(ctx, key)
	require.NoError(t, err)
	return res.Kvs
}

// TestApplyRecoveryAfterRestart simulates a worker crash: the job is in
// running state with some confirmed objects and pending steps. A second
// discovery role (sharing the etcd state) takes over on Start.
func TestApplyRecoveryAfterRestart(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	kv := rootInst.ForRole("test", ctx).KV()

	// Enable the target roles so the recovery node can execute steps.
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyScopes, "test").String(),
		tests.MustJSON(dhcp.Scope{
			SubnetCIDR: "192.0.2.0/24",
			IPAM:       map[string]string{"type": "internal", "range_start": "192.0.2.1", "range_end": "192.0.2.250"},
			DNS:        &dhcp.ScopeDNS{Zone: "gravity.beryju.io."},
		}),
	))
	for _, z := range []string{"gravity.beryju.io.", "0.192.in-addr.arpa."} {
		tests.PanicIfError(kv.Put(
			ctx,
			kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, z).String(),
			tests.MustJSON(dns.Zone{}),
		))
	}
	dhcpRole := dhcp.New(rootInst.ForRole("dhcp", ctx))
	require.NoError(t, dhcpRole.Start(ctx, []byte(tests.MustJSON(dhcp.RoleConfig{Port: 0}))))
	t.Cleanup(dhcpRole.Stop)
	dnsRole := dns.New(rootInst.ForRole("dns", ctx))
	require.NoError(t, dnsRole.Start(ctx, []byte(tests.MustJSON(dns.RoleConfig{Port: 0}))))
	t.Cleanup(dnsRole.Stop)

	applyID := tests.RandomString("apply")
	scanID := tests.RandomString("scan")
	ip, mac, host := "192.0.2.10", "aa:bb:cc:00:00:10", "host-a"
	now := time.Now().UTC()
	confirmedAt := now.Add(-time.Minute)

	leaseKey := kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, mac).String()
	fwdKey := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", host, "A", ip).String()
	revKey := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "0.192.in-addr.arpa.", "10.2", "PTR", ip).String()
	deviceKey := kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, ip).String()

	// Objects the crashed worker already wrote (all with the job provenance).
	prov := map[string]interface{}{"applyId": applyID, "scanId": scanID, "source": discoveryTypes.ProvenanceSource}
	leaseVal, err := json.Marshal(map[string]interface{}{
		"hostname": host, "address": ip, "scopeKey": "test", "expiry": -1, "discovery": prov,
	})
	require.NoError(t, err)
	tests.PanicIfError(kv.Put(ctx, leaseKey, string(leaseVal)))
	fwdVal, err := json.Marshal(map[string]interface{}{
		"data": ip, "ttl": 0, "discovery": prov,
	})
	require.NoError(t, err)
	tests.PanicIfError(kv.Put(ctx, fwdKey, string(fwdVal)))
	tests.PanicIfError(kv.Put(
		ctx,
		deviceKey,
		tests.MustJSON(discovery.Device{
			Hostname: host, IP: ip, MAC: mac,
			Source: discoveryTypes.DeviceSourceScan, ScanID: scanID,
			ApplyID: applyID, ApplyStatus: discoveryTypes.ItemResultApplied,
		}),
	))

	// Revisions of the pre-existing objects: they must not be rewritten.
	beforeLease := mustGetKV(t, kv, ctx, leaseKey)
	require.Len(t, beforeLease, 1)
	leaseModBefore := beforeLease[0].ModRevision
	beforeFwd := mustGetKV(t, kv, ctx, fwdKey)
	require.Len(t, beforeFwd, 1)
	fwdModBefore := beforeFwd[0].ModRevision

	// The interrupted job: three steps confirmed, reverse still pending.
	job := discovery.ApplyJob{
		Subnet: "subnet-a", ScanID: scanID, SourceRevision: 100,
		Targets:   []string{discoveryTypes.TargetDHCP, discoveryTypes.TargetDNS},
		DHCPScope: "test", DNSZone: "gravity.beryju.io.",
		Status: discoveryTypes.JobStatusRunning, FencingGeneration: 1,
		CreatedAt: now.Add(-2 * time.Minute), StartedAt: &now,
		Items: []discovery.ApplyItem{{
			Identifier: ip, Hostname: host, IP: ip, MAC: mac, Subnet: "subnet-a",
			Result: discoveryTypes.ItemResultPending,
			Steps: []discovery.ApplyStep{
				{Name: discoveryTypes.StepNameDeviceMark, TargetKey: deviceKey,
					ExpectedModRevision: 1, Status: discoveryTypes.StepStatusConfirmed, ConfirmedAt: &confirmedAt, ModRev: 1},
				{Name: discoveryTypes.StepNameDHCPReservation, TargetKey: leaseKey,
					ExpectedCreateRevision: 1, Status: discoveryTypes.StepStatusConfirmed, ConfirmedAt: &confirmedAt, ModRev: leaseModBefore},
				{Name: discoveryTypes.StepNameDNSForward, TargetKey: fwdKey,
					ExpectedCreateRevision: 1, Status: discoveryTypes.StepStatusConfirmed, ConfirmedAt: &confirmedAt, ModRev: fwdModBefore},
				{Name: discoveryTypes.StepNameDNSReverse, TargetKey: revKey,
					ExpectedCreateRevision: 0, Status: discoveryTypes.StepStatusPending},
			},
		}},
	}
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, applyID).String(),
		tests.MustJSON(job),
	))

	// Start the discovery role: recovery must take over and complete the job.
	role := discovery.New(rootInst.ForRole("discovery", ctx))
	require.NoError(t, role.Start(ctx, []byte(tests.MustJSON(discovery.RoleConfig{
		Enabled:               true,
		ApplyRetentionSeconds: 120,
	}))))
	t.Cleanup(role.Stop)

	var final discovery.APIApplyJob
	require.Eventually(t, func() bool {
		err := role.APIApplyGet().Interact(ctx, discovery.APIApplyGetInput{ApplyID: applyID}, &final)
		return err == nil && final.Status == discoveryTypes.JobStatusCompleted
	}, 5*time.Second, 20*time.Millisecond)

	item := findItem(final, ip)
	require.NotNil(t, item)
	assert.Equal(t, discoveryTypes.ItemResultApplied, item.Result)
	reverse := findStep(item, discoveryTypes.StepNameDNSReverse)
	require.NotNil(t, reverse)
	assert.Equal(t, discoveryTypes.StepStatusConfirmed, reverse.Status)

	// The missing reverse object now exists exactly once.
	afterRev, err := kv.Get(ctx, revKey)
	require.NoError(t, err)
	require.Len(t, afterRev.Kvs, 1)
	var rec dns.Record
	require.NoError(t, json.Unmarshal(afterRev.Kvs[0].Value, &rec))
	require.NotNil(t, rec.Discovery)
	assert.Equal(t, applyID, rec.Discovery.ApplyID)

	// Confirmed objects were not rewritten.
	afterLease := mustGetKV(t, kv, ctx, leaseKey)
	require.Len(t, afterLease, 1)
	assert.Equal(t, leaseModBefore, afterLease[0].ModRevision)
	afterFwd := mustGetKV(t, kv, ctx, fwdKey)
	require.Len(t, afterFwd, 1)
	assert.Equal(t, fwdModBefore, afterFwd[0].ModRevision)
}

// TestApplyRetentionLease verifies the terminal job and idempotency index are
// attached to the configured retention lease; pending jobs carry no lease.
func TestApplyRetentionLease(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	kv := rootInst.ForRole("test", ctx).KV()
	now := time.Now().UTC()
	applyID := tests.RandomString("apply")
	jobKey := kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, applyID).String()

	// Running jobs carry no retention lease.
	job := discovery.ApplyJob{
		ScanID: "s", Status: discoveryTypes.JobStatusRunning, FencingGeneration: 1,
		CreatedAt: now, Items: []discovery.ApplyItem{},
	}
	tests.PanicIfError(kv.Put(ctx, jobKey, tests.MustJSON(job)))
	res, err := kv.Get(ctx, jobKey)
	require.NoError(t, err)
	require.Len(t, res.Kvs, 1)
	assert.Equal(t, int64(0), res.Kvs[0].Lease)

	// Create a green one-device fixture and run a real job with override TTL.
	f := newWorkerFixtureWithRetention(t, rootInst, 60, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	p := f.preview(t, nil)
	in := createInputFromPreview(f.scanID, p)
	in.IdempotencyKey = "retention-idem"
	created := f.create(t, in, "retention-idem")
	final := f.waitTerminal(t, created.ApplyID)
	require.Equal(t, discoveryTypes.JobStatusCompleted, final.Status)

	terminalKey := kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, created.ApplyID).String()
	res, err = kv.Get(ctx, terminalKey)
	require.NoError(t, err)
	require.Len(t, res.Kvs, 1)
	t.Logf("TERMINAL JOB: %s", string(res.Kvs[0].Value))
	assert.NotZero(t, res.Kvs[0].Lease, "terminal job must carry a retention lease")
	ttl, err := kv.TimeToLive(ctx, clientv3.LeaseID(res.Kvs[0].Lease))
	require.NoError(t, err)
	assert.LessOrEqual(t, ttl.TTL, int64(60))
	assert.Greater(t, ttl.TTL, int64(0))

	idx, err := kv.Get(ctx, kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyAppliesIdem, "retention-idem").String())
	require.NoError(t, err)
	require.Len(t, idx.Kvs, 1)
	assert.NotZero(t, idx.Kvs[0].Lease, "idempotency index must share retention")
}

// newWorkerFixtureWithRetention builds the standard worker fixture on an
// already-bootstrapped root instance, starting the discovery role with an
// explicit retention override so its worker picks up the config.
func newWorkerFixtureWithRetention(t *testing.T, rootInst *instance.Instance, retention int64, zones ...string) *workerFixture {
	t.Helper()
	ctx := tests.Context()
	kv := rootInst.ForRole("test", ctx).KV()

	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyScopes, "test").String(),
		tests.MustJSON(dhcp.Scope{
			SubnetCIDR: "192.0.2.0/24",
			IPAM:       map[string]string{"type": "internal", "range_start": "192.0.2.1", "range_end": "192.0.2.250"},
			DNS:        &dhcp.ScopeDNS{Zone: "gravity.beryju.io."},
		}),
	))
	for _, z := range zones {
		tests.PanicIfError(kv.Put(
			ctx,
			kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, z).String(),
			tests.MustJSON(dns.Zone{}),
		))
	}
	dhcpRole2 := dhcp.New(rootInst.ForRole("dhcp", ctx))
	require.NoError(t, dhcpRole2.Start(ctx, []byte(tests.MustJSON(dhcp.RoleConfig{Port: 0}))))
	t.Cleanup(dhcpRole2.Stop)
	dnsRole2 := dns.New(rootInst.ForRole("dns", ctx))
	require.NoError(t, dnsRole2.Start(ctx, []byte(tests.MustJSON(dns.RoleConfig{Port: 0}))))
	t.Cleanup(dnsRole2.Stop)

	f := &workerFixture{
		rootInst: rootInst,
		role:     discovery.New(rootInst.ForRole("discovery-recovery", ctx)),
		scanID:   tests.RandomString("scan"),
		subnet:   "subnet-a",
		ips:      map[string]string{},
	}
	// Configure retention on the role before jobs run.
	require.NoError(t, f.role.Start(ctx, []byte(tests.MustJSON(discovery.RoleConfig{
		Enabled:               true,
		ApplyRetentionSeconds: retention,
	}))))
	t.Cleanup(f.role.Stop)

	meta := discovery.Scan{
		Subnet: f.subnet, CIDR: "192.0.2.0/24", SourceRevision: 100,
		Status:    discoveryTypes.ScanStatusFinished,
		StartedAt: time.Now().Add(-time.Minute), FinishedAt: time.Now(),
	}
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyScans, f.subnet, f.scanID).String(),
		tests.MustJSON(meta),
	))
	return f
}
