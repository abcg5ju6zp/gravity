package discovery_test

import (
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"beryju.io/gravity/pkg/roles/discovery"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplyCancelMultiDevice(t *testing.T) {
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()

	const n = 12
	ips := make([]string, 0, n)
	for i := 1; i <= n; i++ {
		ip := "192.0.2." + strconv.Itoa(100+i)
		ips = append(ips, ip)
		f.addDevice(t, ip, fmt.Sprintf("aa:bb:cc:00:01:%02x", i), "host"+strconv.Itoa(i))
	}
	p := f.preview(t, ips)
	require.Len(t, p.Items, n)
	job := f.create(t, createInputFromPreview(f.scanID, p), "")

	// Race the worker with the cancel; final invariants must hold regardless
	// of where it stopped.
	require.NoError(t, f.role.APIApplyCancel().Interact(ctx,
		discovery.APIApplyGetInput{ApplyID: job.ApplyID}, &discovery.APIApplyJob{}))

	final := f.waitTerminal(t, job.ApplyID)
	require.Equal(t, discoveryTypes.JobStatusCancelled, final.Status, "job must end cancelled")
	require.NotNil(t, final.FinishedAt)

	confirmedTargets := 0
	cancelledSteps := 0
	for _, item := range final.Items {
		for _, s := range item.Steps {
			switch s.Status {
			case discoveryTypes.StepStatusConfirmed:
				confirmedTargets++
				// Confirmed objects exist and carry this job's provenance.
				res, err := kv.Get(ctx, s.TargetKey)
				require.NoError(t, err)
				require.Len(t, res.Kvs, 1, "confirmed key must exist: %s", s.TargetKey)
				assert.Contains(t, string(res.Kvs[0].Value), job.ApplyID, s.TargetKey)
			case discoveryTypes.StepStatusCancelled:
				cancelledSteps++
				// Cancelled object keys were never written.
				if s.TargetKey != "" {
					res, err := kv.Get(ctx, s.TargetKey)
					require.NoError(t, err)
					assert.Empty(t, res.Kvs, "cancelled target must not exist: %s", s.TargetKey)
				}
			default:
				t.Fatalf("unexpected step status after cancel: %s (%s)", s.Status, s.Name)
			}
		}
	}
	// With 12 devices and immediate cancel at least some steps remain
	// unexecuted (worker cannot atomically finish all).
	assert.Greater(t, cancelledSteps, 0)
	assert.Less(t, confirmedTargets, n*4)
}

func TestApplyCancelTerminalRejected(t *testing.T) {
	f := newWorkerFixture(t, true, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx := tests.Context()
	f.addDevice(t, "192.0.2.10", "aa:bb:cc:00:00:10", "host-a")
	p := f.preview(t, nil)
	job := f.create(t, createInputFromPreview(f.scanID, p), "")
	final := f.waitTerminal(t, job.ApplyID)
	require.NotEqual(t, discoveryTypes.JobStatusCancelling, final.Status)

	// Cancelling a terminal job fails and changes nothing.
	resBefore := dumpJob(t, f, job.ApplyID)
	err := f.role.APIApplyCancel().Interact(ctx,
		discovery.APIApplyGetInput{ApplyID: job.ApplyID}, &discovery.APIApplyJob{})
	assert.Error(t, err)
	resAfter := dumpJob(t, f, job.ApplyID)
	assert.Equal(t, resBefore, resAfter)
}

func TestApplyCancelUnknown(t *testing.T) {
	f := newWorkerFixture(t, false, false)
	err := f.role.APIApplyCancel().Interact(tests.Context(),
		discovery.APIApplyGetInput{ApplyID: "nope"}, &discovery.APIApplyJob{})
	assert.Error(t, err)
}

func dumpJob(t *testing.T, f *workerFixture, id string) string {
	t.Helper()
	ctx := tests.Context()
	res, err := f.rootInst.ForRole("test", ctx).KV().Get(ctx,
		f.rootInst.ForRole("test", ctx).KV().Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies, id).String())
	require.NoError(t, err)
	require.Len(t, res.Kvs, 1)
	var generic map[string]interface{}
	require.NoError(t, json.Unmarshal(res.Kvs[0].Value, &generic))
	// Compare only status and finished presence (the cancel request must not
	// mutate a terminal job).
	return generic["status"].(string)
}
