package tftp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func deletedSet(in []APIBootDeletedVersion) map[string]string {
	out := map[string]string{}
	for _, d := range in {
		out[d.VersionID] = d.State
	}
	return out
}

// TestBootGC_ReferenceProtection verifies the exact protect/delete matrix:
// current, candidates and session-pinned versions survive; cancelled and
// unpinned superseded versions are deleted, blobs included.
func TestBootGC_ReferenceProtection(t *testing.T) {
	m, ctx := newInternalManager(t)
	base := time.Date(2026, 10, 13, 18, 0, 0, 0, time.UTC)
	now := base
	m.now = func() time.Time { return now }

	_, err := m.createBundle(ctx, "b")
	assert.NoError(t, err)
	m1, _ := sessionManifests()

	// v0 published first and pinned by C1 at the entrypoint.
	completeInternalVersionWithManifest(t, m, ctx, "b", "v0", m1)
	_, err = m.publishVersion(ctx, "b", "v0", "")
	assert.NoError(t, err)
	_, err = m.readBootFile(ctx, "1.1.1.1", "b", "entry.efi")
	assert.NoError(t, err)

	// v3 then v2 publish on top: v0 superseded (pinned), v3 superseded (no pin).
	completeInternalVersionWithManifest(t, m, ctx, "b", "v3", m1)
	_, err = m.publishVersion(ctx, "b", "v3", "")
	assert.NoError(t, err)
	completeInternalVersionWithManifest(t, m, ctx, "b", "v2", m1)
	_, err = m.publishVersion(ctx, "b", "v2", "")
	assert.NoError(t, err)

	// Open candidate v5 (protected); candidate v4 then cancelled.
	_, err = m.createVersion(ctx, "b", "v5", m1)
	assert.NoError(t, err)
	_, err = m.createVersion(ctx, "b", "v4", m1)
	assert.NoError(t, err)
	_, err = m.cancelVersion(ctx, "b", "v4")
	assert.NoError(t, err)

	// First (administrator) GC: only unpinned superseded v3 + cancelled v4 go.
	deleted, err := m.garbageCollect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"v3": bootStateSuperseded, "v4": bootStateCancelled}, deletedSet(deleted))

	assertVersionGone(t, m, ctx, "v3")
	assertVersionGone(t, m, ctx, "v4")
	assertVersionPresent(t, m, ctx, "v0", bootStateSuperseded)
	assertVersionPresent(t, m, ctx, "v2", bootStatePublished)
	assertVersionMetaPresent(t, m, ctx, "v5", bootStateCandidate)

	// Release the C1 pin through TTL expiry; v0 becomes collectible.
	now = base.Add(bootSessionTTL + time.Minute)
	m.sweepExpired()
	assert.Empty(t, m.pinnedVersionSnapshot())

	deleted, err = m.garbageCollect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"v0": bootStateSuperseded}, deletedSet(deleted))
	assertVersionGone(t, m, ctx, "v0")
	assertVersionPresent(t, m, ctx, "v2", bootStatePublished)
	assertVersionMetaPresent(t, m, ctx, "v5", bootStateCandidate)

	// Candidate survives GC until explicitly cancelled.
	_, err = m.cancelVersion(ctx, "b", "v5")
	assert.NoError(t, err)
	deleted, err = m.garbageCollect(ctx)
	assert.NoError(t, err)
	assert.Equal(t, map[string]string{"v5": bootStateCancelled}, deletedSet(deleted))
	assertVersionGone(t, m, ctx, "v5")
}

// assertVersionMetaPresent checks state without requiring received blobs
// (incomplete candidates legitimately have none).
func assertVersionMetaPresent(t *testing.T, m *bootManager, ctx context.Context, id, state string) {
	t.Helper()
	v, _, err := m.loadVersion(ctx, "b", id)
	assert.NoError(t, err, id)
	assert.Equal(t, state, v.State, id)
}

func assertVersionPresent(t *testing.T, m *bootManager, ctx context.Context, id, state string) {
	t.Helper()
	v, _, err := m.loadVersion(ctx, "b", id)
	assert.NoError(t, err, id)
	assert.Equal(t, state, v.State, id)
	data, err := m.readVersionFile(ctx, "b", id, "entry.efi")
	assert.NoError(t, err, id)
	assert.Equal(t, []byte("entry.efi@"+id), data, id)
}

func assertVersionGone(t *testing.T, m *bootManager, ctx context.Context, id string) {
	t.Helper()
	_, _, err := m.loadVersion(ctx, "b", id)
	var s interface{ Status() status.Code }
	if assert.ErrorAs(t, err, &s) {
		assert.Equal(t, status.NotFound, s.Status(), id)
	}

	res, err := m.kv.Get(ctx, bootVersionFilesPrefixKey(m.kv, "b", id).String(), clientv3.WithPrefix())
	assert.NoError(t, err)
	assert.Empty(t, res.Kvs, "blobs of %s must be deleted", id)
}
