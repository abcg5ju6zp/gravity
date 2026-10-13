package discovery_test

import (
	"testing"

	"beryju.io/gravity/pkg/roles/dhcp"
	"beryju.io/gravity/pkg/roles/discovery"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func foreignLease() dhcp.Lease {
	return dhcp.Lease{Hostname: "someone-else", Address: "192.0.2.77", ScopeKey: "test", Expiry: -1}
}

// When retry finds no open steps it terminates immediately; the terminal
// job key and idempotency index must still carry the retention lease.
func TestApplyRetryImmediateTerminalKeepsLease(t *testing.T) {
	// No DNS zones at all: both DNS steps stay conflict; a foreign lease
	// keeps the dhcp step conflict. Only the device mark can confirm.
	f := newWorkerFixture(t, true, true)
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")

	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key("dhcp", "leases", "aa:bb:cc:00:00:10").String(),
		tests.MustJSON(foreignLease()),
	))

	p := f.preview(t, nil)
	job := f.create(t, createInputFromPreview(f.scanID, p), "retry-no-open")
	final1 := f.waitTerminal(t, job.ApplyID)
	require.Equal(t, discoveryTypes.JobStatusPartial, final1.Status)
	jobKey := kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, job.ApplyID).String()
	require.NotZero(t, mustGetKV(t, kv, ctx, jobKey)[0].Lease, "first terminal already carries a lease")

	// Retry: nothing becomes claimable -> immediate terminal, lease kept.
	var retried discovery.APIApplyJob
	require.NoError(t, f.role.APIApplyRetry().Interact(ctx,
		discovery.APIApplyGetInput{ApplyID: job.ApplyID}, &retried))
	final2 := f.waitTerminal(t, job.ApplyID)
	require.Equal(t, discoveryTypes.JobStatusPartial, final2.Status)

	require.NotZero(t, mustGetKV(t, kv, ctx, jobKey)[0].Lease,
		"immediate-terminal retry must not detach the retention lease")
	idemKey := kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyAppliesIdem, "retry-no-open").String()
	require.NotZero(t, mustGetKV(t, kv, ctx, idemKey)[0].Lease,
		"idempotency index must keep a retention lease")

	// No target object for the device was created on either pass.
	// (Foreign lease stays foreign; zones never existed.)
	item := findItem(final2, "192.0.2.10")
	require.NotNil(t, item)
	assert.Equal(t, discoveryTypes.ItemResultConflict, item.Result)
}
