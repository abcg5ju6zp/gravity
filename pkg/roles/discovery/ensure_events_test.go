package discovery_test

import (
	"context"
	"encoding/json"
	"testing"

	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles"
	"beryju.io/gravity/pkg/roles/dhcp"
	dhcpTypes "beryju.io/gravity/pkg/roles/dhcp/types"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/roles/dns"
	dnsTypes "beryju.io/gravity/pkg/roles/dns/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
)

func startEnsureRoles(t *testing.T, ctx context.Context, rootInst *instance.Instance, fwdZone, revZone string) {
	t.Helper()
	kv := rootInst.ForRole("test", ctx).KV()
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyScopes, "test").String(),
		tests.MustJSON(dhcp.Scope{
			SubnetCIDR: "192.0.2.0/24",
			IPAM: map[string]string{
				"type":        "internal",
				"range_start": "192.0.2.1",
				"range_end":   "192.0.2.250",
			},
			DNS: &dhcp.ScopeDNS{Zone: fwdZone},
		}),
	))
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, fwdZone).String(),
		tests.MustJSON(dns.Zone{}),
	))
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, revZone).String(),
		tests.MustJSON(dns.Zone{}),
	))
	dhcpRole := dhcp.New(rootInst.ForRole("dhcp", ctx))
	tests.PanicIfError(dhcpRole.Start(ctx, []byte(tests.MustJSON(dhcp.RoleConfig{Port: 0}))))
	t.Cleanup(dhcpRole.Stop)
	dnsRole := dns.New(rootInst.ForRole("dns", ctx))
	tests.PanicIfError(dnsRole.Start(ctx, []byte(tests.MustJSON(dns.RoleConfig{Port: 0}))))
	t.Cleanup(dnsRole.Stop)
}

func TestEnsureDHCPLease(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	startEnsureRoles(t, ctx, rootInst, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	inst := rootInst.ForRole("discovery", ctx)
	kv := inst.KV()

	mac := "aa:bb:cc:00:00:01"
	leaseKey := kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, mac).String()

	// A fence key stands in for the apply job key.
	putFence, err := kv.Put(ctx, kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, "job-1").String(), "{}")
	assert.NoError(t, err)
	fenceKey := kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, "job-1").String()

	payload := func(dryRun bool, createRev, modRev int64, fenceRev int64) map[string]interface{} {
		return map[string]interface{}{
			discoveryTypes.EventFieldDryRun:            dryRun,
			discoveryTypes.EventFieldApplyID:           "apply-1",
			discoveryTypes.EventFieldScanID:            "scan-1",
			discoveryTypes.EventFieldMAC:               mac,
			discoveryTypes.EventFieldHostname:          "test",
			discoveryTypes.EventFieldAddress:           "192.0.2.10",
			discoveryTypes.EventFieldScope:             "test",
			discoveryTypes.EventFieldFenceKey:          fenceKey,
			discoveryTypes.EventFieldFenceRev:          fenceRev,
			discoveryTypes.EventFieldExpectedCreateRev: createRev,
			discoveryTypes.EventFieldExpectedModRev:    modRev,
		}
	}
	dispatch := func(data map[string]interface{}) []roles.EnsureOutcome {
		ev := roles.NewEvent(ctx, data)
		inst.DispatchEvent(dhcpTypes.EventTopicDHCPLeaseEnsure, ev)
		return ev.Outcomes()
	}

	// 1. dry-run on absent target: create, revisions zero.
	out := dispatch(payload(true, 0, 0, putFence.Header.Revision))
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionCreate, out[0].Action)
		assert.Equal(t, leaseKey, out[0].TargetKey)
		assert.Empty(t, out[0].Reason)
	}
	res, _ := kv.Get(ctx, leaseKey)
	assert.Empty(t, res.Kvs, "dry-run must not write")

	// 2. ensure: pinned absent + fence matches -> written with provenance.
	out = dispatch(payload(false, 0, 0, putFence.Header.Revision))
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionCreate, out[0].Action)
		assert.Positive(t, out[0].ModRevision)
	}
	res, _ = kv.Get(ctx, leaseKey)
	if assert.Len(t, res.Kvs, 1) {
		var lease dhcp.Lease
		assert.NoError(t, json.Unmarshal(res.Kvs[0].Value, &lease))
		assert.Equal(t, "192.0.2.10", lease.Address)
		assert.Equal(t, "apply-1", lease.Discovery.ApplyID)
		assert.Equal(t, "scan-1", lease.Discovery.ScanID)
	}
	createdRev := res.Kvs[0].ModRevision

	// 3. dry-run now reports keep/already-provisioned.
	out = dispatch(payload(true, 1, createdRev, putFence.Header.Revision))
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionKeep, out[0].Action)
		assert.True(t, out[0].AlreadyProvisioned)
	}

	// 4. retry with stale absent-pin: reconcile, no second write.
	out = dispatch(payload(false, 0, 0, putFence.Header.Revision))
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionKeep, out[0].Action)
		assert.Equal(t, discoveryTypes.ReasonAlreadyProvisioned, out[0].Reason)
		assert.True(t, out[0].AlreadyProvisioned)
	}
	res, _ = kv.Get(ctx, leaseKey)
	assert.Equal(t, createdRev, res.Kvs[0].ModRevision, "retry must not rewrite")

	// 5. stale fence: transaction blocked even though target matches.
	out = dispatch(payload(false, 1, createdRev, putFence.Header.Revision+1))
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionConflict, out[0].Action)
		assert.Equal(t, discoveryTypes.ReasonJobNotClaimable, out[0].Reason)
	}

	// 6. missing scope / missing mac.
	ev := roles.NewEvent(ctx, map[string]interface{}{
		discoveryTypes.EventFieldDryRun:   true,
		discoveryTypes.EventFieldApplyID:  "apply-1",
		discoveryTypes.EventFieldMAC:      "bb:bb:bb",
		discoveryTypes.EventFieldHostname: "x",
		discoveryTypes.EventFieldAddress:  "192.0.2.11",
		discoveryTypes.EventFieldScope:    "does-not-exist",
	})
	inst.DispatchEvent(dhcpTypes.EventTopicDHCPLeaseEnsure, ev)
	if assert.Len(t, ev.Outcomes(), 1) {
		assert.Equal(t, roles.EnsureActionSkip, ev.Outcomes()[0].Action)
		assert.Equal(t, discoveryTypes.ReasonScopeMissing, ev.Outcomes()[0].Reason)
	}
	ev = roles.NewEvent(ctx, map[string]interface{}{
		discoveryTypes.EventFieldDryRun: true,
		discoveryTypes.EventFieldScope:  "test",
	})
	inst.DispatchEvent(dhcpTypes.EventTopicDHCPLeaseEnsure, ev)
	if assert.Len(t, ev.Outcomes(), 1) {
		assert.Equal(t, roles.EnsureActionSkip, ev.Outcomes()[0].Action)
		assert.Equal(t, discoveryTypes.ReasonMACRequired, ev.Outcomes()[0].Reason)
	}
}

func TestEnsureDHCPLeaseConflicts(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	startEnsureRoles(t, ctx, rootInst, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	inst := rootInst.ForRole("discovery", ctx)
	kv := inst.KV()

	mac := "cc:cc:cc:00:00:01"
	// Foreign, incompatible reservation.
	foreign, err := json.Marshal(&dhcp.Lease{
		Hostname: "someone-else",
		Address:  "192.0.2.99",
		ScopeKey: "test",
		Expiry:   -1,
	})
	assert.NoError(t, err)
	leaseKey := kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, mac).String()
	putRes, err := kv.Put(ctx, leaseKey, string(foreign))
	assert.NoError(t, err)

	dispatch := func(dryRun bool) []roles.EnsureOutcome {
		ev := roles.NewEvent(ctx, map[string]interface{}{
			discoveryTypes.EventFieldDryRun:            dryRun,
			discoveryTypes.EventFieldApplyID:           "apply-9",
			discoveryTypes.EventFieldMAC:               mac,
			discoveryTypes.EventFieldHostname:          "test",
			discoveryTypes.EventFieldAddress:           "192.0.2.10",
			discoveryTypes.EventFieldScope:             "test",
			discoveryTypes.EventFieldExpectedCreateRev: 1,
			discoveryTypes.EventFieldExpectedModRev:    putRes.Header.Revision,
		})
		inst.DispatchEvent(dhcpTypes.EventTopicDHCPLeaseEnsure, ev)
		return ev.Outcomes()
	}

	out := dispatch(true)
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionConflict, out[0].Action)
		assert.Equal(t, discoveryTypes.ReasonTargetOwnedOther, out[0].Reason)
	}

	// Foreign record changes before ensure (revision moves): keep mine.
	foreign2, err := json.Marshal(&dhcp.Lease{
		Hostname: "someone-else-2",
		Address:  "192.0.2.98",
		ScopeKey: "test",
		Expiry:   -1,
	})
	assert.NoError(t, err)
	_, err = kv.Put(ctx, leaseKey, string(foreign2))
	assert.NoError(t, err)

	out = dispatch(false)
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionConflict, out[0].Action)
		assert.Equal(t, discoveryTypes.ReasonTargetOwnedOther, out[0].Reason)
	}
	res, _ := kv.Get(ctx, leaseKey)
	var kept dhcp.Lease
	assert.NoError(t, json.Unmarshal(res.Kvs[0].Value, &kept))
	assert.Equal(t, "someone-else-2", kept.Hostname, "foreign lease must never be overwritten")
	assert.Nil(t, kept.Discovery)
}

func TestEnsureDNSRecords(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	startEnsureRoles(t, ctx, rootInst, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	inst := rootInst.ForRole("discovery", ctx)
	kv := inst.KV()

	putFence, err := kv.Put(ctx, kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, "job-2").String(), "{}")
	assert.NoError(t, err)
	fenceKey := kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, "job-2").String()

	forwardKey := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", "test", "A", "192.0.2.10").String()
	reverseKey := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "0.192.in-addr.arpa.", "10.2", "PTR", "192.0.2.10").String()

	dnsPayload := func(direction, dryRun interface{}, createRev, modRev int64) map[string]interface{} {
		return map[string]interface{}{
			discoveryTypes.EventFieldDryRun:            dryRun,
			discoveryTypes.EventFieldApplyID:           "apply-2",
			discoveryTypes.EventFieldScanID:            "scan-2",
			discoveryTypes.EventFieldDirection:         direction,
			discoveryTypes.EventFieldFQDN:              "test.gravity.beryju.io.",
			discoveryTypes.EventFieldHostname:          "test",
			discoveryTypes.EventFieldAddress:           "192.0.2.10",
			discoveryTypes.EventFieldIdentifier:        "192.0.2.10",
			discoveryTypes.EventFieldFenceKey:          fenceKey,
			discoveryTypes.EventFieldFenceRev:          putFence.Header.Revision,
			discoveryTypes.EventFieldExpectedCreateRev: createRev,
			discoveryTypes.EventFieldExpectedModRev:    modRev,
		}
	}
	dispatch := func(data map[string]interface{}) []roles.EnsureOutcome {
		ev := roles.NewEvent(ctx, data)
		inst.DispatchEvent(dnsTypes.EventTopicDNSRecordEnsure, ev)
		return ev.Outcomes()
	}

	// dry-run resolves both zones and computes keys, without writing.
	out := dispatch(dnsPayload(discoveryTypes.EventDirectionForward, true, 0, 0))
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionCreate, out[0].Action)
		assert.Equal(t, discoveryTypes.StepNameDNSForward, out[0].Step)
		assert.Equal(t, forwardKey, out[0].TargetKey)
	}
	out = dispatch(dnsPayload(discoveryTypes.EventDirectionReverse, true, 0, 0))
	if assert.Len(t, out, 1) {
		assert.Equal(t, roles.EnsureActionCreate, out[0].Action)
		assert.Equal(t, discoveryTypes.StepNameDNSReverse, out[0].Step)
		assert.Equal(t, reverseKey, out[0].TargetKey)
	}
	for _, key := range []string{forwardKey, reverseKey} {
		res, _ := kv.Get(ctx, key)
		assert.Empty(t, res.Kvs)
	}

	// ensure both directions.
	for _, dir := range []string{discoveryTypes.EventDirectionForward, discoveryTypes.EventDirectionReverse} {
		out = dispatch(dnsPayload(dir, false, 0, 0))
		if assert.Len(t, out, 1, dir) {
			assert.Equal(t, roles.EnsureActionCreate, out[0].Action, dir)
		}
	}
	for _, key := range []string{forwardKey, reverseKey} {
		res, _ := kv.Get(ctx, key)
		if assert.Len(t, res.Kvs, 1, key) {
			var rec dns.Record
			assert.NoError(t, json.Unmarshal(res.Kvs[0].Value, &rec))
			assert.Equal(t, "apply-2", rec.Discovery.ApplyID, key)
		}
	}

	// retries reconcile without rewriting.
	fwdRes, _ := kv.Get(ctx, forwardKey)
	revRes, _ := kv.Get(ctx, reverseKey)
	fwdMod, revMod := fwdRes.Kvs[0].ModRevision, revRes.Kvs[0].ModRevision
	for _, dir := range []string{discoveryTypes.EventDirectionForward, discoveryTypes.EventDirectionReverse} {
		out = dispatch(dnsPayload(dir, false, 0, 0))
		if assert.Len(t, out, 1, dir) {
			assert.Equal(t, roles.EnsureActionKeep, out[0].Action, dir)
			assert.True(t, out[0].AlreadyProvisioned, dir)
		}
	}
	fwdRes, _ = kv.Get(ctx, forwardKey)
	revRes, _ = kv.Get(ctx, reverseKey)
	assert.Equal(t, fwdMod, fwdRes.Kvs[0].ModRevision)
	assert.Equal(t, revMod, revRes.Kvs[0].ModRevision)

	// Missing reverse zone -> skip.
	ev := roles.NewEvent(ctx, map[string]interface{}{
		discoveryTypes.EventFieldDryRun:     true,
		discoveryTypes.EventFieldDirection:  discoveryTypes.EventDirectionReverse,
		discoveryTypes.EventFieldFQDN:       "test.other.example.",
		discoveryTypes.EventFieldAddress:    "198.51.100.7",
		discoveryTypes.EventFieldIdentifier: "198.51.100.7",
	})
	inst.DispatchEvent(dnsTypes.EventTopicDNSRecordEnsure, ev)
	if assert.Len(t, ev.Outcomes(), 1) {
		assert.Equal(t, roles.EnsureActionSkip, ev.Outcomes()[0].Action)
		assert.Equal(t, discoveryTypes.ReasonZoneMissing, ev.Outcomes()[0].Reason)
	}
}

func TestEnsureDNSSiblingsAndConflicts(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	startEnsureRoles(t, ctx, rootInst, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	inst := rootInst.ForRole("discovery", ctx)
	kv := inst.KV()

	// Another uid owns an A record with different data at the same name.
	otherKey := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", "test", "A", "other-mac").String()
	foreign := dns.Record{Data: "192.0.2.200"}
	raw, err := json.Marshal(&foreign)
	assert.NoError(t, err)
	_, err = kv.Put(ctx, otherKey, string(raw))
	assert.NoError(t, err)

	ev := roles.NewEvent(ctx, map[string]interface{}{
		discoveryTypes.EventFieldDryRun:     true,
		discoveryTypes.EventFieldApplyID:    "apply-3",
		discoveryTypes.EventFieldDirection:  discoveryTypes.EventDirectionForward,
		discoveryTypes.EventFieldFQDN:       "test.gravity.beryju.io.",
		discoveryTypes.EventFieldHostname:   "test",
		discoveryTypes.EventFieldAddress:    "192.0.2.10",
		discoveryTypes.EventFieldIdentifier: "192.0.2.10",
	})
	inst.DispatchEvent(dnsTypes.EventTopicDNSRecordEnsure, ev)
	out := ev.Outcomes()
	if assert.Len(t, out, 1) {
		// Our own uid key is still absent -> create; sibling reported.
		assert.Equal(t, roles.EnsureActionCreate, out[0].Action)
		assert.Equal(t, int64(1), out[0].Siblings)
	}
}
