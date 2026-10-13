package discovery_test

import (
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
	"github.com/stretchr/testify/require"
)

// A foreign, incompatible object must never be confirmed even when the
// client pins its CURRENT revisions.
func TestEnsureForeignObjectPinnedAtCurrentRevision(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	kv := rootInst.ForRole("test", ctx).KV()

	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyScopes, "test").String(),
		tests.MustJSON(dhcp.Scope{
			SubnetCIDR: "192.0.2.0/24",
			IPAM:       map[string]string{"type": "internal", "range_start": "192.0.2.1", "range_end": "192.0.2.250"},
		}),
	))
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.").String(),
		tests.MustJSON(dns.Zone{}),
	))
	dhcpRole := dhcp.New(rootInst.ForRole("dhcp", ctx))
	require.NoError(t, dhcpRole.Start(ctx, []byte(tests.MustJSON(dhcp.RoleConfig{Port: 0}))))
	t.Cleanup(dhcpRole.Stop)
	dnsRole := dns.New(rootInst.ForRole("dns", ctx))
	require.NoError(t, dnsRole.Start(ctx, []byte(tests.MustJSON(dns.RoleConfig{Port: 0}))))
	t.Cleanup(dnsRole.Stop)
	inst := rootInst.ForRole("discovery", ctx)

	// Fence key.
	fence, err := kv.Put(ctx, kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, "job-x").String(), "{}")
	require.NoError(t, err)

	// Foreign lease, incompatible content.
	mac := "ab:cd:ef:00:00:01"
	leaseKey := kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyLeases, mac).String()
	putRes, err := kv.Put(ctx, leaseKey, tests.MustJSON(dhcp.Lease{
		Hostname: "someone-else", Address: "192.0.2.77", ScopeKey: "test", Expiry: -1,
	}))
	require.NoError(t, err)
	_ = putRes

	// Pin the CURRENT revisions of the foreign object - still must conflict.
	ev := roles.NewEvent(ctx, map[string]interface{}{
		discoveryTypes.EventFieldDryRun:             false,
		discoveryTypes.EventFieldApplyID:            "apply-x",
		discoveryTypes.EventFieldMAC:                mac,
		discoveryTypes.EventFieldHostname:           "wanted",
		discoveryTypes.EventFieldAddress:            "192.0.2.10",
		discoveryTypes.EventFieldScope:              "test",
		discoveryTypes.EventFieldFenceKey:           kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, "job-x").String(),
		discoveryTypes.EventFieldFenceRev:           fence.Header.Revision,
		discoveryTypes.EventFieldExpectedCreateRev:  putRes.Header.Revision,
		discoveryTypes.EventFieldExpectedModRev:     putRes.Header.Revision,
	})
	inst.DispatchEvent(dhcpTypes.EventTopicDHCPLeaseEnsure, ev)
	require.Len(t, ev.Outcomes(), 1)
	assert.Equal(t, roles.EnsureActionConflict, ev.Outcomes()[0].Action)
	assert.Equal(t, discoveryTypes.ReasonTargetOwnedOther, ev.Outcomes()[0].Reason)

	// Object untouched.
	res, _ := kv.Get(ctx, leaseKey)
	require.Len(t, res.Kvs, 1)
	assert.Equal(t, putRes.Header.Revision, res.Kvs[0].ModRevision)
	var lease dhcp.Lease
	require.NoError(t, json.Unmarshal(res.Kvs[0].Value, &lease))
	assert.Equal(t, "192.0.2.77", lease.Address)
	assert.Nil(t, lease.Discovery)

	// Same property for a foreign forward DNS record.
	fwdKey := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", "host-x", "A", "uid-1").String()
	recRes, err := kv.Put(ctx, fwdKey, tests.MustJSON(dns.Record{Data: "192.0.2.200"}))
	require.NoError(t, err)
	ev2 := roles.NewEvent(ctx, map[string]interface{}{
		discoveryTypes.EventFieldDryRun:            false,
		discoveryTypes.EventFieldApplyID:           "apply-x",
		discoveryTypes.EventFieldDirection:         discoveryTypes.EventDirectionForward,
		discoveryTypes.EventFieldFQDN:              "host-x.gravity.beryju.io.",
		discoveryTypes.EventFieldHostname:          "host-x",
		discoveryTypes.EventFieldAddress:           "192.0.2.10",
		discoveryTypes.EventFieldIdentifier:        "uid-1",
		discoveryTypes.EventFieldFenceKey:          kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, "job-x").String(),
		discoveryTypes.EventFieldFenceRev:          fence.Header.Revision,
		discoveryTypes.EventFieldExpectedCreateRev: recRes.Header.Revision,
		discoveryTypes.EventFieldExpectedModRev:    recRes.Header.Revision,
	})
	inst.DispatchEvent(dnsTypes.EventTopicDNSRecordEnsure, ev2)
	require.Len(t, ev2.Outcomes(), 1)
	assert.Equal(t, roles.EnsureActionConflict, ev2.Outcomes()[0].Action)
	assert.Equal(t, discoveryTypes.ReasonTargetOwnedOther, ev2.Outcomes()[0].Reason)
	after, _ := kv.Get(ctx, fwdKey)
	require.Len(t, after.Kvs, 1)
	assert.Equal(t, recRes.Header.Revision, after.Kvs[0].ModRevision)
}
