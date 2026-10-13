package tftp_test

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"beryju.io/gravity/pkg/roles/tftp"
	"github.com/stretchr/testify/assert"
	"github.com/swaggest/usecase/status"
)

// TestBootPublish_ConcurrentArbitration fires N complete candidates at the
// same bundle concurrently; every goroutine retries its stable identity after
// losing arbitration. Commit order must be the only order: seqs form an exact
// [1..N] range, current ends at the highest seq version.
func TestBootPublish_ConcurrentArbitration(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")

	const n = 24
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = fmt.Sprintf("v%02d", i)
		createCompleteBootVersion(t, role, "b", ids[i], ids[i], bootManifest())
	}

	start := make(chan struct{})
	var aborted int64
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			<-start
			for {
				out := tftp.APIBootPublishOutput{}
				err := role.APIBootVersionPublish().Interact(ctx, tftp.APIBootVersionPublishInput{
					Bundle: "b", Version: id,
				}, &out)
				if err == nil {
					return // published now, or reconciled after an earlier own success
				}
				var s interface{ Status() status.Code }
				if assert.ErrorAs(t, err, &s) && s.Status() == status.Aborted {
					atomic.AddInt64(&aborted, 1)
					continue
				}
				t.Errorf("unexpected publish error for %s: %v", id, err)
				return
			}
		}(id)
	}
	close(start)
	wg.Wait()
	t.Logf("observed %d aborted publish attempts across %d racers", atomic.LoadInt64(&aborted), n)

	// Every version occupies exactly one unique monotonic sequence.
	seqs := map[int64]string{}
	states := map[string]string{}
	var list tftp.APIBootVersionsListOutput
	assert.NoError(t, role.APIBootVersionsList().Interact(ctx,
		tftp.APIBootVersionsListInput{Bundle: "b"}, &list))
	assert.Len(t, list.Versions, n)
	for _, v := range list.Versions {
		seq := v.Version.PublishSeq
		assert.NotZero(t, seq, v.Version.ID)
		if other, dup := seqs[seq]; dup {
			t.Fatalf("duplicate publishSeq %d for %s and %s", seq, v.Version.ID, other)
		}
		seqs[seq] = v.Version.ID
		states[v.Version.ID] = v.Version.State
	}
	for seq := int64(1); seq <= n; seq++ {
		assert.Contains(t, seqs, seq, "publish sequence must have no holes")
	}

	// Exactly one current, and it is the version carrying seq=N.
	var bundles tftp.APIBootBundlesListOutput
	assert.NoError(t, role.APIBootBundlesList().Interact(ctx, struct{}{}, &bundles))
	assert.Equal(t, int64(n), bundles.Bundles[0].PublishSeq)
	assert.Equal(t, seqs[n], bundles.Bundles[0].CurrentVersionID)
	assert.Equal(t, "published", states[seqs[n]])
}
