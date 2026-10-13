package tftp_test

import (
	"testing"

	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles/tftp"
	"beryju.io/gravity/pkg/roles/tftp/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/swaggest/usecase/status"
)

// uploadBootFile uploads one file through the API usecase.
func uploadBootFile(t *testing.T, role *tftp.Role, bundle, version, path, contentTag string) []byte {
	t.Helper()
	data := []byte(path + "@" + contentTag)
	assert.NoError(t, role.APIBootVersionFilePut().Interact(tests.Context(), tftp.APIBootVersionFilePutInput{
		Bundle: bundle, Version: version, Path: path, Data: data,
	}, &struct{}{}))
	return data
}

// createCompleteBootVersion creates a candidate and uploads every required
// file. It returns the manifest-path -> bytes map.
func createCompleteBootVersion(t *testing.T, role *tftp.Role, bundle, version, tag string, files []tftp.BootFileSpec) map[string][]byte {
	t.Helper()
	createBootCandidate(t, role, bundle, version, files)
	content := map[string][]byte{}
	for _, f := range files {
		if !f.Required {
			continue
		}
		content[f.Path] = uploadBootFile(t, role, bundle, version, f.Path, tag)
	}
	return content
}

func publishBoot(t *testing.T, role *tftp.Role, bundle, version string) tftp.APIBootPublishOutput {
	t.Helper()
	out := tftp.APIBootPublishOutput{}
	assert.NoError(t, role.APIBootVersionPublish().Interact(tests.Context(), tftp.APIBootVersionPublishInput{
		Bundle: bundle, Version: version,
	}, &out))
	return out
}

func bundleMetaRevision(t *testing.T, inst *instance.RoleInstance, bundle string) int64 {
	t.Helper()
	res, err := inst.KV().Get(tests.Context(),
		inst.KV().Key(types.KeyRole, types.KeyBoot, types.KeyBootBundles, bundle, types.KeyBootMeta).String())
	assert.NoError(t, err)
	assert.Len(t, res.Kvs, 1)
	return res.Kvs[0].ModRevision
}

func TestBootPublish_IncompleteRejected(t *testing.T) {
	role, ctx, inst := newBootTestRole(t)
	createBootBundle(t, role, "b")
	createBootCandidate(t, role, "b", "v1", nil)
	uploadBootFile(t, role, "b", "v1", "entry.efi", "v1") // kernel still missing

	before := bundleMetaRevision(t, inst, "b")
	err := role.APIBootVersionPublish().Interact(ctx,
		tftp.APIBootVersionPublishInput{Bundle: "b", Version: "v1"}, &tftp.APIBootPublishOutput{})
	assertBootStatus(t, err, status.FailedPrecondition)
	assert.Equal(t, before, bundleMetaRevision(t, inst, "b"), "bundle meta must not be modified")

	// Version stays candidate.
	var st tftp.APIBootVersionOutput
	assert.NoError(t, role.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v1"}, &st))
	assert.Equal(t, "candidate", st.Version.State)
}

func TestBootPublish_AtomicSwitch(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")

	v1Files := createCompleteBootVersion(t, role, "b", "v1", "v1", bootManifest())
	res := publishBoot(t, role, "b", "v1")
	assert.Equal(t, int64(1), res.PublishSeq)
	assert.Equal(t, "published", res.State)

	// Pointer + version state land together.
	var st tftp.APIBootVersionOutput
	assert.NoError(t, role.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v1"}, &st))
	assert.Equal(t, "published", st.Version.State)
	assert.Equal(t, int64(1), st.Version.PublishSeq)
	var bundles tftp.APIBootBundlesListOutput
	assert.NoError(t, role.APIBootBundlesList().Interact(ctx, struct{}{}, &bundles))
	assert.Equal(t, "v1", bundles.Bundles[0].CurrentVersionID)
	assert.Equal(t, int64(1), bundles.Bundles[0].PublishSeq)

	// Every published file is reachable byte-for-byte through the version API.
	for path, want := range v1Files {
		dl := tftp.APIBootVersionFileDownloadOutput{}
		assert.NoError(t, role.APIBootVersionFileDownload().Interact(ctx,
			tftp.APIBootVersionFileDownloadInput{Bundle: "b", Version: "v1", Path: path}, &dl), path)
		assert.Equal(t, want, dl.Data, path)
	}

	// Publishing v2 atomically supersedes v1, keeps v1 blobs.
	createCompleteBootVersion(t, role, "b", "v2", "v2", bootManifest())
	res2 := publishBoot(t, role, "b", "v2")
	assert.Equal(t, int64(2), res2.PublishSeq)
	assert.NoError(t, role.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v1"}, &st))
	assert.Equal(t, "superseded", st.Version.State)
	assert.Equal(t, int64(1), st.Version.PublishSeq)
	for path := range v1Files {
		dl := tftp.APIBootVersionFileDownloadOutput{}
		assert.NoError(t, role.APIBootVersionFileDownload().Interact(ctx,
			tftp.APIBootVersionFileDownloadInput{Bundle: "b", Version: "v1", Path: path}, &dl), path)
		assert.Equal(t, []byte(path+"@v1"), dl.Data)
	}
}

func TestBootPublish_IdempotentReconcile(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")
	createCompleteBootVersion(t, role, "b", "v1", "v1", bootManifest())
	first := publishBoot(t, role, "b", "v1")
	assert.Equal(t, int64(1), first.PublishSeq)

	// Lost response: same identity retried returns the same result, no new seq.
	retry := publishBoot(t, role, "b", "v1")
	assert.Equal(t, "published", retry.State)
	assert.Equal(t, int64(1), retry.PublishSeq)
	var bundles tftp.APIBootBundlesListOutput
	assert.NoError(t, role.APIBootBundlesList().Interact(ctx, struct{}{}, &bundles))
	assert.Equal(t, int64(1), bundles.Bundles[0].PublishSeq)

	// A published version can not be cancelled (serving version immutability).
	assertBootStatus(t,
		role.APIBootVersionCancel().Interact(ctx,
			tftp.APIBootVersionCancelInput{Bundle: "b", Version: "v1"}, &tftp.APIBootVersionOutput{}),
		status.FailedPrecondition)

	// v2 overtakes v1; retrying v1 reconciles to superseded with its history seq.
	createCompleteBootVersion(t, role, "b", "v2", "v2", bootManifest())
	publishBoot(t, role, "b", "v2")
	reconcile := publishBoot(t, role, "b", "v1")
	assert.Equal(t, "superseded", reconcile.State)
	assert.Equal(t, int64(1), reconcile.PublishSeq)
	assert.Equal(t, "v2", reconcile.CurrentVersionID)

	// ifCurrentVersion fencing: mismatch rejects without consuming a seq.
	createCompleteBootVersion(t, role, "b", "v3", "v3", bootManifest())
	err := role.APIBootVersionPublish().Interact(ctx, tftp.APIBootVersionPublishInput{
		Bundle: "b", Version: "v3", IfCurrentVersion: "v1",
	}, &tftp.APIBootPublishOutput{})
	assertBootStatus(t, err, status.FailedPrecondition)
	assert.NoError(t, role.APIBootBundlesList().Interact(ctx, struct{}{}, &bundles))
	assert.Equal(t, int64(2), bundles.Bundles[0].PublishSeq)
	assert.Equal(t, "v2", bundles.Bundles[0].CurrentVersionID)

	// Matching fence allows the publish.
	fenced := tftp.APIBootPublishOutput{}
	assert.NoError(t, role.APIBootVersionPublish().Interact(ctx, tftp.APIBootVersionPublishInput{
		Bundle: "b", Version: "v3", IfCurrentVersion: "v2",
	}, &fenced))
	assert.Equal(t, int64(3), fenced.PublishSeq)
}

// TestBootPublish_CancelledRejected covers R1 finding F-2: a cancelled
// version can never be published; the call consumes no sequence and leaves
// the serving pointer untouched.
func TestBootPublish_CancelledRejected(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")
	createCompleteBootVersion(t, role, "b", "v1", "v1", bootManifest())
	assert.NoError(t, role.APIBootVersionCancel().Interact(ctx,
		tftp.APIBootVersionCancelInput{Bundle: "b", Version: "v1"}, &tftp.APIBootVersionOutput{}))

	err := role.APIBootVersionPublish().Interact(ctx,
		tftp.APIBootVersionPublishInput{Bundle: "b", Version: "v1"}, &tftp.APIBootPublishOutput{})
	assertBootStatus(t, err, status.FailedPrecondition)

	// No sequence consumed, no current pointer; other candidates still publish.
	var bundles tftp.APIBootBundlesListOutput
	assert.NoError(t, role.APIBootBundlesList().Interact(ctx, struct{}{}, &bundles))
	assert.Equal(t, int64(0), bundles.Bundles[0].PublishSeq)
	assert.Empty(t, bundles.Bundles[0].CurrentVersionID)

	createCompleteBootVersion(t, role, "b", "v2", "v2", bootManifest())
	res := publishBoot(t, role, "b", "v2")
	assert.Equal(t, int64(1), res.PublishSeq)
}

func TestBootPublish_MissingVersionAndBundle(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")
	err := role.APIBootVersionPublish().Interact(ctx,
		tftp.APIBootVersionPublishInput{Bundle: "b", Version: "ghost"}, &tftp.APIBootPublishOutput{})
	assertBootStatus(t, err, status.NotFound)

	err = role.APIBootVersionPublish().Interact(ctx,
		tftp.APIBootVersionPublishInput{Bundle: "ghost", Version: "v1"}, &tftp.APIBootPublishOutput{})
	assertBootStatus(t, err, status.NotFound)

	// Invalid input names.
	err = role.APIBootVersionPublish().Interact(ctx,
		tftp.APIBootVersionPublishInput{Bundle: "bad/name", Version: "v1"}, &tftp.APIBootPublishOutput{})
	assertBootStatus(t, err, status.InvalidArgument)
}
