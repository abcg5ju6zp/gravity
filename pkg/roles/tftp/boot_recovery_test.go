package tftp_test

import (
	"testing"

	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles/tftp"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/swaggest/usecase/status"
)

// TestBootRecovery_AfterRestart builds state with one process, then starts a
// fresh Role against the same etcd: the current pointer and unfinished
// candidates must be recoverable; an unpinned superseded version is reclaimed
// by startup GC while a cancelled version stays queryable until explicit GC.
func TestBootRecovery_AfterRestart(t *testing.T) {
	tests.Setup(t)
	ctx := tests.Context()

	rootInst1 := instance.New()
	inst1 := rootInst1.ForRole("tftp", ctx)
	role1 := tftp.New(inst1)
	assert.NoError(t, role1.Start(ctx, []byte{}))

	createBootBundle(t, role1, "b")
	m1, _ := bootReadManifests()
	createCompleteBootVersion(t, role1, "b", "v0", "v0", m1)
	publishBoot(t, role1, "b", "v0")
	createCompleteBootVersion(t, role1, "b", "v1", "v1", m1)
	publishBoot(t, role1, "b", "v1") // v0 -> superseded, no session pins it

	// Unfinished candidate v2 and cancelled candidate v3.
	createBootCandidate(t, role1, "b", "v2", m1)
	uploadBootFile(t, role1, "b", "v2", "entry.efi", "v2") // kernel missing
	createBootCandidate(t, role1, "b", "v3", m1)
	assert.NoError(t, role1.APIBootVersionCancel().Interact(ctx,
		tftp.APIBootVersionCancelInput{Bundle: "b", Version: "v3"}, &tftp.APIBootVersionOutput{}))
	role1.Stop()

	// --- simulated restart: fresh Role, fresh in-memory session table ---
	rootInst2 := instance.New()
	inst2 := rootInst2.ForRole("tftp", ctx)
	role2 := tftp.New(inst2)
	assert.NoError(t, role2.Start(ctx, []byte{}))
	defer role2.Stop()

	// Current pointer recovered: entry served from v1 (and pins this client).
	data, err := readBootTFTP(t, role2, "8.8.8.1", "entry.efi")
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry.efi@v1"), data)

	// Unpinned superseded v0 reclaimed by startup GC; v1 untouched.
	err = role2.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v0"}, &tftp.APIBootVersionOutput{})
	assertBootStatus(t, err, status.NotFound)

	// Unfinished candidate v2 recovered: queryable, completable, publishable.
	var st tftp.APIBootVersionOutput
	assert.NoError(t, role2.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v2"}, &st))
	assert.Equal(t, "candidate", st.Version.State)
	assert.ElementsMatch(t, []string{"dir/kernel"}, st.Missing)
	uploadBootFile(t, role2, "b", "v2", "dir/kernel", "v2")
	published := publishBoot(t, role2, "b", "v2")
	assert.Equal(t, int64(3), published.PublishSeq)

	// Cancelled v3 survived restart recovery and remains queryable...
	assert.NoError(t, role2.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v3"}, &st))
	assert.Equal(t, "cancelled", st.Version.State)

	// ...until an explicit GC runs. v1 (now superseded) is protected by the
	// 8.8.8.1 read session; v3 is what gets collected.
	gc := tftp.APIBootGCOutput{}
	assert.NoError(t, role2.APIBootGC().Interact(ctx, struct{}{}, &gc))
	assert.Len(t, gc.Deleted, 1)
	assert.Equal(t, "v3", gc.Deleted[0].VersionID)
	err = role2.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v3"}, &tftp.APIBootVersionOutput{})
	assertBootStatus(t, err, status.NotFound)
	data, err = readBootTFTP(t, role2, "8.8.8.1", "dir/kernel")
	assert.NoError(t, err)
	assert.Equal(t, []byte("dir/kernel@v1"), data)
}
