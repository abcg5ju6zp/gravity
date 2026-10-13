package dhcp

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"beryju.io/gravity/pkg/roles"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/roles/dns"
	dnsTypes "beryju.io/gravity/pkg/roles/dns/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
)

// dispatchTestInstance wraps the no-op test instance with real in-memory
// event dispatch, so the DHCP and DNS roles can be wired together in an
// internal test without importing pkg/instance (which would import cycle).
type dispatchTestInstance struct {
	*dhcpTestInstance
	eventMu  sync.RWMutex
	handlers map[string][]roles.EventHandler
}

func newDispatchTestInstance(t *testing.T) *dispatchTestInstance {
	t.Helper()
	return &dispatchTestInstance{
		dhcpTestInstance: newDHCPTestInstance(tests.Context()),
		handlers:         make(map[string][]roles.EventHandler),
	}
}

func (i *dispatchTestInstance) DispatchEvent(topic string, ev *roles.Event) {
	i.eventMu.RLock()
	handlers := make([]roles.EventHandler, len(i.handlers[topic]))
	copy(handlers, i.handlers[topic])
	i.eventMu.RUnlock()
	for _, handler := range handlers {
		handler(ev)
	}
}

func (i *dispatchTestInstance) AddEventListener(topic string, handler roles.EventHandler) {
	i.eventMu.Lock()
	defer i.eventMu.Unlock()
	i.handlers[topic] = append(i.handlers[topic], handler)
}

func TestLeaseProvenanceJSONCompat(t *testing.T) {
	// Old-style lease JSON (no discovery field) must keep unmarshalling and
	// re-marshalling without the field appearing.
	oldJSON := `{"address":"192.0.2.1","hostname":"test","scopeKey":"s","expiry":-1}`
	var lease Lease
	assert.NoError(t, json.Unmarshal([]byte(oldJSON), &lease))
	assert.Nil(t, lease.Discovery)
	raw, err := json.Marshal(&lease)
	assert.NoError(t, err)
	assert.NotContains(t, string(raw), "discovery")

	// New-style lease JSON carries the provenance.
	lease.Discovery = &discoveryTypes.Provenance{
		ApplyID: "apply-1",
		ScanID:  "scan-1",
		Source:  discoveryTypes.ProvenanceSource,
	}
	raw, err = json.Marshal(&lease)
	assert.NoError(t, err)
	assert.Contains(t, string(raw), `"applyId":"apply-1"`)
	var back Lease
	assert.NoError(t, json.Unmarshal(raw, &back))
	assert.NotNil(t, back.Discovery)
	assert.Equal(t, "apply-1", back.Discovery.ApplyID)
}

func TestProvenanceReservationSkipsDNSChain(t *testing.T) {
	tests.Setup(t)
	ctx := tests.Context()
	inst := newDispatchTestInstance(t)
	kv := inst.KV()

	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key("dhcp", "scopes", "test").String(),
		tests.MustJSON(Scope{
			SubnetCIDR: "192.0.2.0/24",
			TTL:        86400,
			DNS: &ScopeDNS{
				Zone: "gravity.beryju.io.",
			},
			IPAM: map[string]string{
				"type":        "internal",
				"range_start": "192.0.2.1",
				"range_end":   "192.0.2.250",
			},
		}),
	))
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.").String(),
		tests.MustJSON(dns.Zone{}),
	))
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "0.192.in-addr.arpa.").String(),
		tests.MustJSON(dns.Zone{}),
	))

	dnsRole := dns.New(inst)
	assert.NoError(t, dnsRole.Start(ctx, []byte(tests.MustJSON(dns.RoleConfig{Port: 0}))))
	defer dnsRole.Stop()
	role := New(inst)
	assert.NoError(t, role.Start(ctx, []byte(tests.MustJSON(RoleConfig{Port: 0}))))
	defer role.Stop()

	scope, ok := role.scopes.GetPrefix("test")
	assert.True(t, ok)

	putLease := func(mac string, provenance *discoveryTypes.Provenance) {
		lease := role.NewLease(mac)
		lease.scope = scope
		lease.ScopeKey = scope.Name
		lease.Address = "192.0.2.10"
		lease.Hostname = "provenancehost"
		lease.Expiry = -1
		lease.Discovery = provenance
		assert.NoError(t, lease.Put(ctx, -1))
	}

	// Reservation with provenance must not create DNS records via the chain.
	putLease("aa:bb:cc:00:00:01", &discoveryTypes.Provenance{
		ApplyID: "apply-1",
		ScanID:  "scan-1",
		Source:  discoveryTypes.ProvenanceSource,
	})
	// Give the (suppressed) chain a moment - records must never appear.
	time.Sleep(100 * time.Millisecond)
	for _, key := range []string{
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", "provenancehost", "A", "aa:bb:cc:00:00:01").String(),
		kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "0.192.in-addr.arpa.", "10.2", "PTR", "aa:bb:cc:00:00:01").String(),
	} {
		res, err := kv.Get(ctx, key)
		assert.NoError(t, err)
		assert.Len(t, res.Kvs, 0, "provenance reservation created a chained DNS record: %s", key)
	}

	// Positive control: a reservation without provenance keeps the old chain.
	putLease("aa:bb:cc:00:00:02", nil)
	forward := kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, "gravity.beryju.io.", "provenancehost", "A", "aa:bb:cc:00:00:02")
	assert.Eventually(t, func() bool {
		res, err := kv.Get(ctx, forward.String())
		return err == nil && len(res.Kvs) == 1
	}, time.Second, 10*time.Millisecond)
}
