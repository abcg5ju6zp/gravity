package tftp

import (
	"context"
	"strings"
	"sync"
	"testing"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/swaggest/usecase/status"
	"go.uber.org/zap"
)

// newInternalManager builds a manager directly against the test etcd,
// avoiding importing pkg/instance (which would cycle through roles.Register).
func newInternalManager(t *testing.T) (*bootManager, context.Context) {
	t.Helper()
	tests.Setup(t)
	ctx := tests.Context()
	return newBootManager(extconfig.Get().EtcdClient(), zap.NewNop()), ctx
}

func completeInternalVersion(t *testing.T, m *bootManager, ctx context.Context, bundle, id string) {
	t.Helper()
	manifest := []BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true},
		{Path: "dir/kernel", Required: true},
	}
	_, err := m.createVersion(ctx, bundle, id, manifest)
	assert.NoError(t, err)
	assert.NoError(t, m.putVersionFile(ctx, bundle, id, "entry.efi", []byte("e-"+id)))
	assert.NoError(t, m.putVersionFile(ctx, bundle, id, "dir/kernel", []byte("k-"+id)))
}

// TestBootPublish_DeterministicCASLoss deterministically drives the losing
// arbitration branch: while candidate B sits just before its commit, another
// complete version C commits, so B's stale ModRevision guard must abort with
// the actual current pointer; retrying B then succeeds with the next seq.
func TestBootPublish_DeterministicCASLoss(t *testing.T) {
	m, ctx := newInternalManager(t)

	_, err := m.createBundle(ctx, "b")
	assert.NoError(t, err)
	completeInternalVersion(t, m, ctx, "b", "a")
	completeInternalVersion(t, m, ctx, "b", "b")
	completeInternalVersion(t, m, ctx, "b", "c")

	a, err := m.publishVersion(ctx, "b", "a", "")
	assert.NoError(t, err)
	assert.Equal(t, int64(1), a.PublishSeq)

	var once sync.Once
	m.testBeforeCommit = func() {
		once.Do(func() {
			// Temporarily detach the hook so the nested publish cannot recurse.
			hook := m.testBeforeCommit
			m.testBeforeCommit = nil
			defer func() { m.testBeforeCommit = hook }()

			c, err := m.publishVersion(ctx, "b", "c", "")
			assert.NoError(t, err)
			assert.Equal(t, int64(2), c.PublishSeq)
			assert.Equal(t, "c", c.CurrentVersionID)
		})
	}

	lost, err := m.publishVersion(ctx, "b", "b", "")
	assert.Nil(t, lost)
	assert.Error(t, err)
	var s interface{ Status() status.Code }
	assert.ErrorAs(t, err, &s)
	assert.Equal(t, status.Aborted, s.Status())
	var d interface{ Description() string }
	assert.ErrorAs(t, err, &d)
	assert.True(t, strings.Contains(d.Description(), "current=c"),
		"abort must carry the actual current pointer: %s", d.Description())

	// Retrying the same stable identity publishes with the next seq.
	m.testBeforeCommit = nil
	b, err := m.publishVersion(ctx, "b", "b", "")
	assert.NoError(t, err)
	assert.Equal(t, int64(3), b.PublishSeq)
	assert.Equal(t, "b", b.CurrentVersionID)

	bundle, _, err := m.loadBundle(ctx, "b")
	assert.NoError(t, err)
	assert.Equal(t, "b", bundle.CurrentVersionID)
	assert.Equal(t, int64(3), bundle.PublishSeq)

	// C is now superseded, A superseded earlier; B published.
	v, _, err := m.loadVersion(ctx, "b", "c")
	assert.NoError(t, err)
	assert.Equal(t, bootStateSuperseded, v.State)
	assert.Equal(t, int64(2), v.PublishSeq)
}
