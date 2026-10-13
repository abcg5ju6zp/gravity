package tftp

import (
	"strings"
	"testing"

	"beryju.io/gravity/pkg/storage"
	"github.com/stretchr/testify/assert"
)

func newKeyTestClient() *storage.Client {
	// Key construction never touches the embedded etcd connection.
	return &storage.Client{}
}

func TestBootKeyLayout(t *testing.T) {
	kv := newKeyTestClient()
	assert.Equal(t,
		"/tftp/boot/bundles/b/meta",
		bootBundleMetaKey(kv, "b").String())
	assert.Equal(t,
		"/tftp/boot/bundles/b/versions/v1/meta",
		bootVersionMetaKey(kv, "b", "v1").String())
	assert.Equal(t,
		"/tftp/boot/bundles/b/versions/v1/files/dir/kernel",
		bootVersionFileKey(kv, "b", "v1", "dir/kernel").String())

	filesPrefix := bootVersionFilesPrefixKey(kv, "b", "v1")
	assert.True(t, filesPrefix.IsPrefix())
	assert.Equal(t,
		"/tftp/boot/bundles/b/versions/v1/files/",
		filesPrefix.String())

	// The version v1 files prefix must not cover v2 files.
	v2 := bootVersionFileKey(kv, "b", "v2", "dir/kernel").String()
	assert.NotContains(t, v2, filesPrefix.String())

	// All boot keys stay outside the legacy files namespace.
	assert.False(t,
		strings.HasPrefix(bootVersionFileKey(kv, "b", "v1", "x").String(), "/tftp/files/"))
}

func TestValidateBootName(t *testing.T) {
	valid := []string{"b", "ubuntu-24.04", "a.b_c-1", "A1"}
	for _, n := range valid {
		assert.NoError(t, validateBootName(n), n)
	}
	invalid := []string{"", "a/b", ".dot", "-dash", "a b", "ünicode"}
	for _, n := range invalid {
		assert.Error(t, validateBootName(n), n)
	}
}

func TestValidateBootRelativePath(t *testing.T) {
	valid := []string{"vmlinuz", "dir/initrd", "pxe/pxelinux.0", "a/b/c-1.efi"}
	for _, p := range valid {
		assert.NoError(t, validateBootRelativePath(p), p)
	}
	invalid := []string{
		"", "/abs", "../x", "a/../b", "a//b", "a/./b", "a/b/",
		`dir\\x`, "boot/x", ".",
	}
	for _, p := range invalid {
		assert.Error(t, validateBootRelativePath(p), p)
	}
	// A nested "boot" segment is allowed, only the top-level prefix is reserved.
	assert.NoError(t, validateBootRelativePath("x/boot/y"))
}

func TestValidateBootManifest(t *testing.T) {
	assert.NoError(t, validateBootManifest([]BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true, SHA256: ""},
		{Path: "kernel", Required: true},
		{Path: "readme", Required: false},
	}))

	// Multiple entrypoints (BIOS/UEFI) allowed as long as they are required.
	assert.NoError(t, validateBootManifest([]BootFileSpec{
		{Path: "pxelinux.0", Required: true, Entrypoint: true},
		{Path: "ipxe.efi", Required: true, Entrypoint: true},
	}))

	assert.Error(t, validateBootManifest(nil))
	assert.Error(t, validateBootManifest([]BootFileSpec{{Path: "kernel", Required: true}}))
	assert.Error(t, validateBootManifest([]BootFileSpec{
		{Path: "e", Required: false, Entrypoint: true},
	}))
	assert.Error(t, validateBootManifest([]BootFileSpec{
		{Path: "e", Required: true, Entrypoint: true},
		{Path: "e", Required: true},
	}))
	assert.Error(t, validateBootManifest([]BootFileSpec{
		{Path: "e", Required: true, Entrypoint: true, SHA256: "abc"},
	}))
	assert.NoError(t, validateBootManifest([]BootFileSpec{
		{Path: "e", Required: true, Entrypoint: true, SHA256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"},
	}))
}

func TestSplitBootFilename(t *testing.T) {
	b, p, ok := splitBootFilename("boot/ubuntu/vmlinuz-6.8")
	assert.True(t, ok)
	assert.Equal(t, "ubuntu", b)
	assert.Equal(t, "vmlinuz-6.8", p)

	for _, f := range []string{"foo", "boot/", "boot//x", "boot/b/", "boot/b", "boot/../x", "shared/boot/x"} {
		_, _, ok := splitBootFilename(f)
		assert.False(t, ok, f)
	}
}
