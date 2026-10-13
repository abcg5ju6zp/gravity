package tftp

import (
	"context"
	"testing"
	"time"

	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
)

func TestBootRead_SessionTTLAndFallback(t *testing.T) {
	m, ctx := newInternalManager(t)
	now := time.Date(2026, 10, 13, 10, 0, 0, 0, time.UTC)
	m.now = func() time.Time { return now }

	_, err := m.createBundle(ctx, "b")
	assert.NoError(t, err)
	m1, m2 := sessionManifests()
	completeInternalVersionWithManifest(t, m, ctx, "b", "v1", m1)
	_, err = m.publishVersion(ctx, "b", "v1", "")
	assert.NoError(t, err)

	// C1 enters through the entrypoint → pinned to v1.
	data, err := m.readBootFile(ctx, "1.1.1.1", "b", "entry.efi")
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry.efi@v1"), data)

	completeInternalVersionWithManifest(t, m, ctx, "b", "v2", m2)
	_, err = m.publishVersion(ctx, "b", "v2", "")
	assert.NoError(t, err)

	// Before TTL expiry C1 stays on v1.
	data, err = m.readBootFile(ctx, "1.1.1.1", "b", "dir/kernel")
	assert.NoError(t, err)
	assert.Equal(t, []byte("dir/kernel@v1"), data)

	// After the 10 minute idle window the next entrypoint read re-pins to v2.
	now = now.Add(bootSessionTTL + time.Minute)
	data, err = m.readBootFile(ctx, "1.1.1.1", "b", "entry.efi")
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry.efi@v2"), data)
	data, err = m.readBootFile(ctx, "1.1.1.1", "b", "extra")
	assert.NoError(t, err)
	assert.Equal(t, []byte("extra@v2"), data)

	// Fallback without a pin: a non-entry file is served from current but does
	// not create a pin. Prove it by publishing v3 and entering afterwards —
	// the client must pin v3, proving no earlier pin to v2 existed.
	completeInternalVersionWithManifest(t, m, ctx, "b", "v3", m2)
	_, err = m.publishVersion(ctx, "b", "v3", "")
	assert.NoError(t, err)
	data, err = m.readBootFile(ctx, "2.2.2.2", "b", "dir/kernel")
	assert.NoError(t, err)
	assert.Equal(t, []byte("dir/kernel@v3"), data)
	data, err = m.readBootFile(ctx, "2.2.2.2", "b", "entry.efi")
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry.efi@v3"), data)
}

func TestBootSession_JanitorAndSweep(t *testing.T) {
	m, ctx := newInternalManager(t)
	base := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	now := base
	m.now = func() time.Time { return now }

	_, err := m.createBundle(ctx, "b")
	assert.NoError(t, err)
	m1, _ := sessionManifests()
	completeInternalVersionWithManifest(t, m, ctx, "b", "v1", m1)
	_, err = m.publishVersion(ctx, "b", "v1", "")
	assert.NoError(t, err)

	_, err = m.readBootFile(ctx, "3.3.3.3", "b", "entry.efi")
	assert.NoError(t, err)
	assert.Contains(t, m.pinnedVersionSnapshot(), "b/v1")

	// Idle beyond TTL: janitor sweep drops the pin.
	now = base.Add(bootSessionTTL + time.Minute)
	m.sweepExpired()
	assert.Empty(t, m.pinnedVersionSnapshot())

	// The janitor goroutine exits when its context is cancelled.
	jCtx, cancel := context.WithCancel(tests.Context())
	done := make(chan struct{})
	go func() {
		m.runJanitor(jCtx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("janitor did not stop after context cancellation")
	}
}

func sessionManifests() (v1, v2 []BootFileSpec) {
	v1 = []BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true},
		{Path: "dir/kernel", Required: true},
	}
	v2 = []BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true},
		{Path: "dir/kernel", Required: true},
		{Path: "extra", Required: true},
	}
	return v1, v2
}

// completeInternalVersionWithManifest uploads every required file of manifest.
func completeInternalVersionWithManifest(t *testing.T, m *bootManager, ctx context.Context, bundle, id string, files []BootFileSpec) {
	t.Helper()
	_, err := m.createVersion(ctx, bundle, id, files)
	assert.NoError(t, err)
	for _, f := range files {
		if !f.Required {
			continue
		}
		assert.NoError(t, m.putVersionFile(ctx, bundle, id, f.Path, []byte(f.Path+"@"+id)))
	}
}
