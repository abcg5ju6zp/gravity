package discovery_test

import (
	"testing"

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

func TestApplyRetryAfterZoneFix(t *testing.T) {
	// First run lacks the reverse zone: job ends partial. After the zone is
	// created, retry completes only the missing step.
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.")
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	p := f.preview(t, nil)
	job := f.create(t, createInputFromPreview(f.scanID, p), "")
	final := f.waitTerminal(t, job.ApplyID)
	require.Equal(t, discoveryTypes.JobStatusPartial, final.Status)
	item := findItem(final, "192.0.2.10")
	require.Equal(t, discoveryTypes.StepStatusConflict, findStep(item, discoveryTypes.StepNameDNSReverse).Status)

	// Captured revisions of the confirmed objects must stay frozen.
	leaseKey := kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, "aa:bb:cc:00:00:10").String()
	fwdKey := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", "host-a", "A", "192.0.2.10").String()
	deviceKey := kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, "192.0.2.10").String()
	revKey := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "0.192.in-addr.arpa.", "10.2", "PTR", "192.0.2.10").String()
	leaseMod := mustGetKV(t, kv, ctx, leaseKey)[0].ModRevision
	fwdMod := mustGetKV(t, kv, ctx, fwdKey)[0].ModRevision
	devMod := mustGetKV(t, kv, ctx, deviceKey)[0].ModRevision

	// Add the missing reverse zone and wait until it resolves in preview.
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "0.192.in-addr.arpa.").String(),
		tests.MustJSON(dns.Zone{}),
	))
	require.Eventually(t, func() bool {
		var pv discovery.APIAppliesPreviewOutput
		if err := f.role.APIAppliesPreview().Interact(ctx, discovery.APIAppliesPreviewInput{
			ScanID: f.scanID, Targets: []string{discoveryTypes.TargetDNS},
			DNSZone: "gravity.beryju.io.",
		}, &pv); err != nil || len(pv.Items) != 1 {
			return false
		}
		for _, s := range pv.Items[0].Steps {
			if s.Name == discoveryTypes.StepNameDNSReverse {
				return s.Action == "create"
			}
		}
		return false
	}, 3_000_000_000, 20*1_000_000)

	var retried discovery.APIApplyJob
	require.NoError(t, f.role.APIApplyRetry().Interact(ctx,
		discovery.APIApplyGetInput{ApplyID: job.ApplyID}, &retried))
	assert.Equal(t, int64(2), retried.FencingGeneration)

	completed := f.waitTerminal(t, job.ApplyID)
	assert.Equal(t, discoveryTypes.JobStatusCompleted, completed.Status)
	item = findItem(completed, "192.0.2.10")
	for _, s := range item.Steps {
		assert.Equal(t, discoveryTypes.StepStatusConfirmed, s.Status, s.Name)
	}
	reverse := findStep(item, discoveryTypes.StepNameDNSReverse)
	assert.Contains(t, reverse.TargetKey, "/10.2/PTR/")

	// Only the reverse record was created; everything else stayed frozen.
	require.Len(t, mustGetKV(t, kv, ctx, revKey), 1)
	assert.Equal(t, leaseMod, mustGetKV(t, kv, ctx, leaseKey)[0].ModRevision)
	assert.Equal(t, fwdMod, mustGetKV(t, kv, ctx, fwdKey)[0].ModRevision)
	assert.Equal(t, devMod, mustGetKV(t, kv, ctx, deviceKey)[0].ModRevision)
}

func TestApplyRetryPersistentConflict(t *testing.T) {
	// A foreign reservation blocks the dhcp step on both attempts; retry
	// must leave it conflicting and must not rewrite anything.
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	p := f.preview(t, nil)

	// Pre-empt the lease before the first execution.
	leaseKey := kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, "aa:bb:cc:00:00:10").String()
	tests.PanicIfError(kv.Put(
		ctx, leaseKey,
		tests.MustJSON(dhcp.Lease{Hostname: "other", Address: "192.0.2.99", ScopeKey: "test", Expiry: -1}),
	))

	first := f.create(t, createInputFromPreview(f.scanID, p), "")
	final1 := f.waitTerminal(t, first.ApplyID)
	require.Equal(t, discoveryTypes.JobStatusPartial, final1.Status)
	dhcp1 := findStep(findItem(final1, "192.0.2.10"), discoveryTypes.StepNameDHCPReservation)
	require.Equal(t, discoveryTypes.StepStatusConflict, dhcp1.Status)
	mod1 := mustGetKV(t, kv, ctx, leaseKey)[0].ModRevision

	// Retry without resolving the conflict: still partial, no rewrite.
	var retried discovery.APIApplyJob
	require.NoError(t, f.role.APIApplyRetry().Interact(ctx,
		discovery.APIApplyGetInput{ApplyID: first.ApplyID}, &retried))
	final2 := f.waitTerminal(t, first.ApplyID)
	assert.Equal(t, discoveryTypes.JobStatusPartial, final2.Status)
	dhcp2 := findStep(findItem(final2, "192.0.2.10"), discoveryTypes.StepNameDHCPReservation)
	assert.Equal(t, discoveryTypes.StepStatusConflict, dhcp2.Status)
	// Re-validation reports the concrete foreign drift; the foreign object
	// itself is never rewritten.
	assert.Contains(t, []string{
		discoveryTypes.ReasonTargetChanged,
		discoveryTypes.ReasonTargetOwnedOther,
	}, dhcp2.Reason)
	assert.Equal(t, mod1, mustGetKV(t, kv, ctx, leaseKey)[0].ModRevision)
}

func TestApplyRetryRejectsRunningAndCompleted(t *testing.T) {
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx := tests.Context()
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	p := f.preview(t, nil)
	job := f.create(t, createInputFromPreview(f.scanID, p), "")
	final := f.waitTerminal(t, job.ApplyID)
	require.Equal(t, discoveryTypes.JobStatusCompleted, final.Status)

	err := f.role.APIApplyRetry().Interact(ctx,
		discovery.APIApplyGetInput{ApplyID: job.ApplyID}, &discovery.APIApplyJob{})
	assert.Error(t, err)

	err = f.role.APIApplyRetry().Interact(ctx,
		discovery.APIApplyGetInput{ApplyID: "missing"}, &discovery.APIApplyJob{})
	assert.Error(t, err)
}
