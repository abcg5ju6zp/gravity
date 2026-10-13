package tftp_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles/tftp"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/swaggest/usecase/status"
)

func newBootTestRole(t *testing.T) (*tftp.Role, context.Context, *instance.RoleInstance) {
	t.Helper()
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	inst := rootInst.ForRole("tftp", ctx)
	role := tftp.New(inst)
	assert.NoError(t, role.Start(ctx, []byte{}))
	t.Cleanup(role.Stop)
	return role, ctx, inst
}

func bootManifest() []tftp.BootFileSpec {
	return []tftp.BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true},
		{Path: "dir/kernel", Required: true},
		{Path: "readme", Required: false},
	}
}

func sha256hex(data []byte) string {
	s := sha256.Sum256(data)
	return hex.EncodeToString(s[:])
}

// assertBootStatus asserts a usecase/status wrapped error carries want code.
func assertBootStatus(t *testing.T, err error, want status.Code) {
	t.Helper()
	var s interface{ Status() status.Code }
	if assert.ErrorAs(t, err, &s) {
		assert.Equal(t, want, s.Status())
	}
}

func createBootBundle(t *testing.T, role *tftp.Role, name string) {
	t.Helper()
	assert.NoError(t, role.APIBootBundleCreate().Interact(tests.Context(), tftp.APIBootBundleCreateInput{
		Name: name,
	}, &tftp.APIBootBundleOutput{}))
}

func createBootCandidate(t *testing.T, role *tftp.Role, bundle, version string, files []tftp.BootFileSpec) tftp.APIBootVersionOutput {
	t.Helper()
	if files == nil {
		files = bootManifest()
	}
	out := tftp.APIBootVersionOutput{}
	assert.NoError(t, role.APIBootVersionCreate().Interact(tests.Context(), tftp.APIBootVersionCreateInput{
		Bundle:  bundle,
		Version: version,
		Files:   files,
	}, &out))
	return out
}

func TestBootVersion_CreateIdempotentConflict(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")

	first := createBootCandidate(t, role, "b", "v1", nil)
	assert.Equal(t, "candidate", first.Version.State)
	assert.ElementsMatch(t, []string{"entry.efi", "dir/kernel"}, first.Missing)

	// Same identity + same manifest is idempotent.
	again := createBootCandidate(t, role, "b", "v1", bootManifest())
	assert.Equal(t, first.Version.CreatedAt, again.Version.CreatedAt)

	// Same identity + different manifest conflicts, stored candidate untouched.
	different := []tftp.BootFileSpec{
		{Path: "other-entry", Required: true, Entrypoint: true},
	}
	err := role.APIBootVersionCreate().Interact(ctx, tftp.APIBootVersionCreateInput{
		Bundle: "b", Version: "v1", Files: different,
	}, &tftp.APIBootVersionOutput{})
	assertBootStatus(t, err, status.AlreadyExists)

	var statusOut tftp.APIBootVersionOutput
	assert.NoError(t, role.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v1"}, &statusOut))
	assert.Len(t, statusOut.Version.Files, 3)

	// Missing bundle / invalid manifest are rejected.
	err = role.APIBootVersionCreate().Interact(ctx, tftp.APIBootVersionCreateInput{
		Bundle: "nope", Version: "v9", Files: bootManifest(),
	}, &tftp.APIBootVersionOutput{})
	assertBootStatus(t, err, status.NotFound)

	err = role.APIBootVersionCreate().Interact(ctx, tftp.APIBootVersionCreateInput{
		Bundle: "b", Version: "bad",
		Files: []tftp.BootFileSpec{{Path: "x", Required: true}},
	}, &tftp.APIBootVersionOutput{})
	assertBootStatus(t, err, status.InvalidArgument)
}

func TestBootVersion_UploadValidation(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")
	files := []tftp.BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true, SHA256: ""},
		{Path: "pinned", Required: true, SHA256: ""},
		{Path: "hashed", Required: true, SHA256: ""},
	}
	createBootCandidate(t, role, "b", "v1", files)

	putFile := func(version, path string, data []byte) error {
		return role.APIBootVersionFilePut().Interact(ctx, tftp.APIBootVersionFilePutInput{
			Bundle: "b", Version: version, Path: path, Data: data,
		}, &struct{}{})
	}

	entry := []byte("entry-binary")
	assert.NoError(t, putFile("v1", "entry.efi", entry))

	// Idempotent identical re-upload.
	assert.NoError(t, putFile("v1", "entry.efi", entry))

	// Different content on an already received path conflicts, blob untouched.
	assertBootStatus(t, putFile("v1", "entry.efi", []byte("tampered")), status.AlreadyExists)
	dl := tftp.APIBootVersionFileDownloadOutput{}
	assert.NoError(t, role.APIBootVersionFileDownload().Interact(ctx,
		tftp.APIBootVersionFileDownloadInput{Bundle: "b", Version: "v1", Path: "entry.efi"}, &dl))
	assert.Equal(t, entry, dl.Data)

	// Declared checksum must match.
	hashedSpec := []tftp.BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true},
		{Path: "h", Required: true, SHA256: sha256hex([]byte("expected"))},
	}
	createBootCandidate(t, role, "b", "vhash", hashedSpec)
	err := putFile("vhash", "h", []byte("different"))
	assertBootStatus(t, err, status.InvalidArgument)
	assert.NoError(t, putFile("vhash", "h", []byte("expected")))

	// Unknown path / illegal path / oversize are rejected without writes.
	assertBootStatus(t, putFile("v1", "not-in-manifest", []byte("x")), status.InvalidArgument)
	assertBootStatus(t, putFile("v1", "../escape", []byte("x")), status.InvalidArgument)
	big := make([]byte, 10*1024*1024)
	assertBootStatus(t, putFile("v1", "pinned", big), status.InvalidArgument)

	// Upload to a missing version / missing bundle.
	assertBootStatus(t, putFile("ghost", "entry.efi", entry), status.NotFound)

	// Upload to a cancelled candidate is rejected.
	createBootCandidate(t, role, "b", "vc", nil)
	assert.NoError(t, role.APIBootVersionCancel().Interact(ctx,
		tftp.APIBootVersionCancelInput{Bundle: "b", Version: "vc"}, &tftp.APIBootVersionOutput{}))
	assertBootStatus(t, putFile("vc", "entry.efi", entry), status.FailedPrecondition)
}

func TestBootVersion_Status(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")
	createBootCandidate(t, role, "b", "v1", nil)

	put := func(path string, data []byte) {
		assert.NoError(t, role.APIBootVersionFilePut().Interact(ctx, tftp.APIBootVersionFilePutInput{
			Bundle: "b", Version: "v1", Path: path, Data: data,
		}, &struct{}{}))
	}
	put("entry.efi", []byte("e"))

	var out tftp.APIBootVersionOutput
	assert.NoError(t, role.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v1"}, &out))
	assert.ElementsMatch(t, []string{"dir/kernel"}, out.Missing)
	assert.Equal(t, sha256hex([]byte("e")), out.Version.Received["entry.efi"].SHA256)
	assert.Equal(t, 1, out.Version.Received["entry.efi"].Size)

	// Optional files never appear as missing; listing shows every version.
	put("readme", []byte("r"))
	assert.NoError(t, role.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v1"}, &out))
	assert.ElementsMatch(t, []string{"dir/kernel"}, out.Missing)

	var list tftp.APIBootVersionsListOutput
	assert.NoError(t, role.APIBootVersionsList().Interact(ctx,
		tftp.APIBootVersionsListInput{Bundle: "b"}, &list))
	assert.Len(t, list.Versions, 1)
}

// TestBootVersion_ListWithMetaFilePaths guards the R1 finding F-1: blob keys
// whose path ends in "meta" must not be mistaken for version meta keys.
func TestBootVersion_ListWithMetaFilePaths(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")

	manifest := []tftp.BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true},
		{Path: "meta", Required: true},       // blob key .../files/meta
		{Path: "cfg/meta", Required: true},   // blob key .../files/cfg/meta
	}
	out := tftp.APIBootVersionOutput{}
	assert.NoError(t, role.APIBootVersionCreate().Interact(ctx, tftp.APIBootVersionCreateInput{
		Bundle: "b", Version: "v1", Files: manifest,
	}, &out))
	// JSON-looking content used to produce a phantom entry; binary content used
	// to make the listing fail with Internal entirely.
	assert.NoError(t, role.APIBootVersionFilePut().Interact(ctx, tftp.APIBootVersionFilePutInput{
		Bundle: "b", Version: "v1", Path: "entry.efi", Data: []byte("entry"),
	}, &struct{}{}))
	assert.NoError(t, role.APIBootVersionFilePut().Interact(ctx, tftp.APIBootVersionFilePutInput{
		Bundle: "b", Version: "v1", Path: "meta", Data: []byte(`{"x":1}`),
	}, &struct{}{}))
	assert.NoError(t, role.APIBootVersionFilePut().Interact(ctx, tftp.APIBootVersionFilePutInput{
		Bundle: "b", Version: "v1", Path: "cfg/meta", Data: []byte{0x00, 0x01, 0x02, 0xff},
	}, &struct{}{}))
	publishBoot(t, role, "b", "v1")

	// A second (incomplete) candidate keeps the listing interesting.
	assert.NoError(t, role.APIBootVersionCreate().Interact(ctx, tftp.APIBootVersionCreateInput{
		Bundle: "b", Version: "v2", Files: manifest,
	}, &tftp.APIBootVersionOutput{}))

	list := tftp.APIBootVersionsListOutput{}
	assert.NoError(t, role.APIBootVersionsList().Interact(ctx,
		tftp.APIBootVersionsListInput{Bundle: "b"}, &list))
	assert.Len(t, list.Versions, 2)
	ids := map[string]string{}
	for _, v := range list.Versions {
		ids[v.Version.ID] = v.Version.State
		assert.Len(t, v.Version.Files, 3)
	}
	assert.Equal(t, "published", ids["v1"])
	assert.Equal(t, "candidate", ids["v2"])

	// Bundle listing is unaffected by blob names too.
	bundles := tftp.APIBootBundlesListOutput{}
	assert.NoError(t, role.APIBootBundlesList().Interact(ctx, struct{}{}, &bundles))
	assert.Len(t, bundles.Bundles, 1)
	assert.Equal(t, "v1", bundles.Bundles[0].CurrentVersionID)
}

func TestBootVersion_CancelState(t *testing.T) {
	role, ctx, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")
	createBootCandidate(t, role, "b", "v1", nil)

	out := tftp.APIBootVersionOutput{}
	assert.NoError(t, role.APIBootVersionCancel().Interact(ctx,
		tftp.APIBootVersionCancelInput{Bundle: "b", Version: "v1"}, &out))
	assert.Equal(t, "cancelled", out.Version.State)

	// Cancelled remains queryable.
	var statusOut tftp.APIBootVersionOutput
	assert.NoError(t, role.APIBootVersionStatus().Interact(ctx,
		tftp.APIBootVersionLookupInput{Bundle: "b", Version: "v1"}, &statusOut))
	assert.Equal(t, "cancelled", statusOut.Version.State)

	// Second cancel is rejected; uploads and publishes are rejected too.
	assertBootStatus(t,
		role.APIBootVersionCancel().Interact(ctx,
			tftp.APIBootVersionCancelInput{Bundle: "b", Version: "v1"}, &tftp.APIBootVersionOutput{}),
		status.FailedPrecondition)
	assertBootStatus(t,
		role.APIBootVersionFilePut().Interact(ctx, tftp.APIBootVersionFilePutInput{
			Bundle: "b", Version: "v1", Path: "entry.efi", Data: []byte("x"),
		}, &struct{}{}),
		status.FailedPrecondition)

	// Bundle listing survives and shows no current pointer.
	var bundles tftp.APIBootBundlesListOutput
	assert.NoError(t, role.APIBootBundlesList().Interact(ctx, struct{}{}, &bundles))
	assert.Len(t, bundles.Bundles, 1)
	assert.Empty(t, bundles.Bundles[0].CurrentVersionID)
}
