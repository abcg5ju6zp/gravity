package discovery_test

import (
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
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type workerFixture struct {
	rootInst *instance.Instance
	role     *discovery.Role
	scanID   string
	subnet   string
	ips      map[string]string
}

func newWorkerFixture(t *testing.T, withDHCP, withDNS bool, zones ...string) *workerFixture {
	t.Helper()
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	kv := rootInst.ForRole("test", ctx).KV()
	f := &workerFixture{
		rootInst: rootInst,
		role:     discovery.New(rootInst.ForRole("discovery", ctx)),
		scanID:   tests.RandomString("scan"),
		subnet:   "subnet-a",
		ips:      map[string]string{},
	}

	if withDHCP {
		tests.PanicIfError(kv.Put(
			ctx,
			kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyScopes, "test").String(),
			tests.MustJSON(dhcp.Scope{
				SubnetCIDR: "192.0.2.0/24",
				IPAM:       map[string]string{"type": "internal", "range_start": "192.0.2.1", "range_end": "192.0.2.250"},
				DNS:        &dhcp.ScopeDNS{Zone: "gravity.beryju.io."},
			}),
		))
		dhcpRole := dhcp.New(rootInst.ForRole("dhcp", ctx))
		require.NoError(t, dhcpRole.Start(ctx, []byte(tests.MustJSON(dhcp.RoleConfig{Port: 0}))))
		t.Cleanup(dhcpRole.Stop)
	}
	if withDNS {
		for _, z := range zones {
			tests.PanicIfError(kv.Put(
				ctx,
				kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, z).String(),
				tests.MustJSON(dns.Zone{}),
			))
		}
		dnsRole := dns.New(rootInst.ForRole("dns", ctx))
		require.NoError(t, dnsRole.Start(ctx, []byte(tests.MustJSON(dns.RoleConfig{Port: 0}))))
		t.Cleanup(dnsRole.Stop)
	}

	// Finished scan meta.
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

func (f *workerFixture) addDevice(t *testing.T, ip, mac, hostname string) {
	t.Helper()
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	f.ips[hostname] = ip
	snap := discovery.ScanDevice{
		Identifier: ip, Hostname: hostname, IP: ip, MAC: mac, Subnet: f.subnet,
		ScanID: f.scanID, SourceRev: 100, ObservedAt: time.Now(),
		ContentHash: discovery.DeviceContentHash(hostname, ip, mac, f.subnet),
	}
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyScans, f.subnet, f.scanID, "devices", ip).String(),
		tests.MustJSON(snap),
	))
	dev := discovery.Device{
		Hostname: hostname, IP: ip, MAC: mac,
		Source: discoveryTypes.DeviceSourceScan, ScanID: f.scanID,
	}
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, ip).String(),
		tests.MustJSON(dev),
	))
}

func createInputFromPreview(scanID string, p discovery.APIAppliesPreviewOutput) discovery.APIAppliesCreateInput {
	out := discovery.APIAppliesCreateInput{
		ScanID:    scanID,
		Targets:   targetsOf(p.Items[0]),
		DHCPScope: "test",
		DNSZone:   "gravity.beryju.io.",
	}
	for _, item := range p.Items {
		pins := []discovery.APIApplyStepPin{{
			Name: discoveryTypes.StepNameDeviceMark, ModRevision: item.DeviceRevision,
		}}
		for _, s := range item.Steps {
			pins = append(pins, discovery.APIApplyStepPin{
				Name: s.Name, TargetKey: s.TargetKey,
				CreateRevision: s.CreateRev, ModRevision: s.ModRev,
			})
		}
		out.Items = append(out.Items, discovery.APIApplyItemInput{
			Identifier: item.Identifier, ContentHash: item.ContentHash,
			DeviceRevision: item.DeviceRevision, Steps: pins,
		})
	}
	return out
}

func hasDNS(steps []discovery.APIApplyPreviewStep) bool {
	for _, s := range steps {
		if s.Name == discoveryTypes.StepNameDNSForward {
			return true
		}
	}
	return false
}

func targetsOf(first discovery.APIApplyPreviewItem) []string {
	t := []string{}
	for _, s := range first.Steps {
		switch s.Name {
		case discoveryTypes.StepNameDHCPReservation:
			t = append(t, discoveryTypes.TargetDHCP)
		case discoveryTypes.StepNameDNSForward:
			t = append(t, discoveryTypes.TargetDNS)
		}
	}
	return t
}

func (f *workerFixture) preview(t *testing.T, ips []string) discovery.APIAppliesPreviewOutput {
	t.Helper()
	ctx := tests.Context()
	in := discovery.APIAppliesPreviewInput{
		ScanID:    f.scanID,
		Targets:   []string{discoveryTypes.TargetDHCP, discoveryTypes.TargetDNS},
		DHCPScope: "test", DNSZone: "gravity.beryju.io.",
		Identifiers: ips,
	}
	var out discovery.APIAppliesPreviewOutput
	require.NoError(t, f.role.APIAppliesPreview().Interact(ctx, in, &out))
	return out
}

func (f *workerFixture) create(t *testing.T, in discovery.APIAppliesCreateInput, idem string) discovery.APIApplyJob {
	t.Helper()
	ctx := tests.Context()
	in.IdempotencyKey = idem
	var out discovery.APIApplyJob
	require.NoError(t, f.role.APIAppliesCreate().Interact(ctx, in, &out))
	return out
}

func (f *workerFixture) waitTerminal(t *testing.T, applyID string) discovery.APIApplyJob {
	t.Helper()
	var job discovery.APIApplyJob
	require.Eventually(t, func() bool {
		err := f.role.APIApplyGet().Interact(tests.Context(), discovery.APIApplyGetInput{ApplyID: applyID}, &job)
		if err != nil {
			return false
		}
		terminal := job.Status == discoveryTypes.JobStatusCompleted ||
			job.Status == discoveryTypes.JobStatusPartial ||
			job.Status == discoveryTypes.JobStatusFailed ||
			job.Status == discoveryTypes.JobStatusCancelled
		// FinishedAt is written atomically with the terminal transition.
		return terminal && job.FinishedAt != nil
	}, 5*time.Second, 20*time.Millisecond)
	return job
}

func findItem(job discovery.APIApplyJob, id string) *discovery.ApplyItem {
	for i := range job.Items {
		if job.Items[i].Identifier == id {
			return &job.Items[i]
		}
	}
	return nil
}

func findStep(item *discovery.ApplyItem, name string) *discovery.ApplyStep {
	for i := range item.Steps {
		if item.Steps[i].Name == name {
			return &item.Steps[i]
		}
	}
	return nil
}

func TestApplyWorkerFullExecution(t *testing.T) {
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")

	p := f.preview(t, []string{"192.0.2.10"})
	require.True(t, p.Items[0].Executable)
	job := f.create(t, createInputFromPreview(f.scanID, p), "")
	final := f.waitTerminal(t, job.ApplyID)

	assert.Equal(t, discoveryTypes.JobStatusCompleted, final.Status)
	item := findItem(final, "192.0.2.10")
	require.NotNil(t, item)
	assert.Equal(t, discoveryTypes.ItemResultApplied, item.Result)
	for _, s := range item.Steps {
		assert.Equal(t, discoveryTypes.StepStatusConfirmed, s.Status, s.Name)
		require.NotNil(t, s.ConfirmedAt, s.Name)
		assert.NotEmpty(t, s.TargetKey, s.Name)
	}

	// Same provenance on all four objects.
	leaseRes, err := kv.Get(ctx, kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, "aa:bb:cc:00:00:10").String())
	require.NoError(t, err)
	require.Len(t, leaseRes.Kvs, 1)
	var lease dhcp.Lease
	require.NoError(t, json.Unmarshal(leaseRes.Kvs[0].Value, &lease))
	require.NotNil(t, lease.Discovery)
	assert.Equal(t, job.ApplyID, lease.Discovery.ApplyID)
	assert.Equal(t, f.scanID, lease.Discovery.ScanID)

	for _, key := range []string{
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", "host-a", "A", "192.0.2.10").String(),
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "0.192.in-addr.arpa.", "10.2", "PTR", "192.0.2.10").String(),
	} {
		res, err := kv.Get(ctx, key)
		require.NoError(t, err)
		require.Len(t, res.Kvs, 1, key)
		var rec dns.Record
		require.NoError(t, json.Unmarshal(res.Kvs[0].Value, &rec))
		require.NotNil(t, rec.Discovery, key)
		assert.Equal(t, job.ApplyID, rec.Discovery.ApplyID, key)
	}

	// The live device carries the apply identity.
	devRes, err := kv.Get(ctx, kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, "192.0.2.10").String())
	require.NoError(t, err)
	require.Len(t, devRes.Kvs, 1)
	var dev discovery.Device
	require.NoError(t, json.Unmarshal(devRes.Kvs[0].Value, &dev))
	assert.Equal(t, job.ApplyID, dev.ApplyID)
	assert.Equal(t, discoveryTypes.ItemResultApplied, dev.ApplyStatus)
}

func TestApplyWorkerConflictIsolation(t *testing.T) {
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	f.addDevice(t, "192.0.2.11", "aa:bb:cc:00:00:11", "host-b")

	p := f.preview(t, nil)
	require.Len(t, p.Items, 2)

	// Device B is touched by a newer scan before the job claims it.
	devB := discovery.Device{
		Hostname: "host-b", IP: "192.0.2.11", MAC: "aa:bb:cc:00:00:11",
		Source: discoveryTypes.DeviceSourceScan, ScanID: tests.RandomString("newer"),
		SourceRevision: 999,
	}
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, "192.0.2.11").String(),
		tests.MustJSON(devB),
	))
	// A foreign reservation pre-empts device B's lease.
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, "aa:bb:cc:00:00:11").String(),
		tests.MustJSON(dhcp.Lease{Hostname: "other", Address: "192.0.2.99", ScopeKey: "test", Expiry: -1}),
	))

	job := f.create(t, createInputFromPreview(f.scanID, p), "")
	final := f.waitTerminal(t, job.ApplyID)
	assert.Equal(t, discoveryTypes.JobStatusPartial, final.Status, "one blocked device yields partial")

	itemA := findItem(final, "192.0.2.10")
	itemB := findItem(final, "192.0.2.11")
	require.NotNil(t, itemA)
	require.NotNil(t, itemB)
	assert.Equal(t, discoveryTypes.ItemResultApplied, itemA.Result)
	assert.Equal(t, discoveryTypes.ItemResultConflict, itemB.Result)

	// B's device mark conflicted; foreign lease untouched and no DNS objects
	// for B exist.
	devRes, _ := kv.Get(ctx, kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, "192.0.2.11").String())
	var markedB discovery.Device
	require.NoError(t, json.Unmarshal(devRes.Kvs[0].Value, &markedB))
	assert.Empty(t, markedB.ApplyID)
	leaseB, _ := kv.Get(ctx, kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, "aa:bb:cc:00:00:11").String())
	var kept dhcp.Lease
	require.NoError(t, json.Unmarshal(leaseB.Kvs[0].Value, &kept))
	assert.Equal(t, "192.0.2.99", kept.Address)
	assert.Nil(t, kept.Discovery)
	for _, key := range []string{
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", "host-b", "A", "192.0.2.11").String(),
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "0.192.in-addr.arpa.", "11.2", "PTR", "192.0.2.11").String(),
	} {
		res, _ := kv.Get(ctx, key)
		assert.Empty(t, res.Kvs, key)
	}
}

func TestApplyWorkerMissingReverseZonePartial(t *testing.T) {
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.")
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	p := f.preview(t, nil)
	job := f.create(t, createInputFromPreview(f.scanID, p), "")
	final := f.waitTerminal(t, job.ApplyID)
	assert.Equal(t, discoveryTypes.JobStatusPartial, final.Status)
	item := findItem(final, "192.0.2.10")
	reverse := findStep(item, discoveryTypes.StepNameDNSReverse)
	require.NotNil(t, reverse)
	assert.Equal(t, discoveryTypes.StepStatusConflict, reverse.Status)
	assert.Equal(t, discoveryTypes.ReasonZoneMissing, reverse.Reason)
	for _, name := range []string{
		discoveryTypes.StepNameDeviceMark,
		discoveryTypes.StepNameDHCPReservation,
		discoveryTypes.StepNameDNSForward,
	} {
		assert.Equal(t, discoveryTypes.StepStatusConfirmed, findStep(item, name).Status, name)
	}
}

func TestApplyWorkerRolesUnavailableSkipped(t *testing.T) {
	// No dhcp/dns roles at all: all target steps skipped, job completed.
	f := newWorkerFixture(t, false, false)
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	// Preview without roles marks everything non-executable, but submission
	// is accepted; the worker records role_unavailable skips.
	p := f.preview(t, nil)
	for _, s := range p.Items[0].Steps {
		assert.Equal(t, "skip", s.Action)
		assert.Equal(t, discoveryTypes.ReasonRoleUnavailable, s.Reason)
	}
	job := f.create(t, createInputFromPreview(f.scanID, p), "")
	final := f.waitTerminal(t, job.ApplyID)
	assert.Equal(t, discoveryTypes.JobStatusCompleted, final.Status)
	item := findItem(final, "192.0.2.10")
	require.NotNil(t, item)
	for _, s := range item.Steps {
		if s.Name == discoveryTypes.StepNameDeviceMark {
			assert.Equal(t, discoveryTypes.StepStatusConfirmed, s.Status)
			continue
		}
		assert.Equal(t, discoveryTypes.StepStatusSkipped, s.Status, s.Name)
		assert.Equal(t, discoveryTypes.ReasonRoleUnavailable, s.Reason, s.Name)
	}
}

func TestApplyWorkerTwoJobsSameDevice(t *testing.T) {
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	p := f.preview(t, nil)

	// Two concurrent jobs pin the same absent target revisions.
	in1 := createInputFromPreview(f.scanID, p)
	in2 := createInputFromPreview(f.scanID, p)
	job1 := f.create(t, in1, "job-1")
	job2 := f.create(t, in2, "job-2")
	final1 := f.waitTerminal(t, job1.ApplyID)
	final2 := f.waitTerminal(t, job2.ApplyID)

	results := []string{
		findItem(final1, "192.0.2.10").Result,
		findItem(final2, "192.0.2.10").Result,
	}
	assert.Contains(t, results, discoveryTypes.ItemResultApplied)
	assert.Contains(t, results, discoveryTypes.ItemResultConflict)

	// Exactly one lease/record set exists and it has a single owner.
	leaseRes, err := kv.Get(ctx, kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, "aa:bb:cc:00:00:10").String())
	require.NoError(t, err)
	require.Len(t, leaseRes.Kvs, 1)
	var lease dhcp.Lease
	require.NoError(t, json.Unmarshal(leaseRes.Kvs[0].Value, &lease))
	require.NotNil(t, lease.Discovery)
	winner := lease.Discovery.ApplyID
	assert.Contains(t, []string{job1.ApplyID, job2.ApplyID}, winner)
	// The device mark belongs to the same winner.
	devRes, _ := kv.Get(ctx, kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, "192.0.2.10").String())
	var dev discovery.Device
	require.NoError(t, json.Unmarshal(devRes.Kvs[0].Value, &dev))
	assert.Equal(t, winner, dev.ApplyID)
}
