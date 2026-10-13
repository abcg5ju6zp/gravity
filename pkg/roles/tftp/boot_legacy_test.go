package tftp_test

import (
	"bytes"
	"testing"

	"beryju.io/gravity/pkg/roles/tftp"
	"beryju.io/gravity/pkg/roles/tftp/types"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/stretchr/testify/assert"
)

// TestBootLegacy_Compatibility verifies that the boot bundle feature is
// strictly additive: single-file APIs keep their behavior and namespace,
// plain TFTP writes/reads are unaffected, and boot/ TFTP writes are rejected
// without writing anywhere.
func TestBootLegacy_Compatibility(t *testing.T) {
	role, ctx, inst := newBootTestRole(t)

	// A TFTP write into the reserved prefix must be rejected and must leave
	// neither a bundle key nor a legacy per-host key behind.
	err := role.Writer("boot/evil/x", IncomingTransfer{bytes.NewBufferString("xx"), "1.2.3.4"})
	assert.Error(t, err)

	bootRoot, err := inst.KV().Get(ctx,
		inst.KV().Key(types.KeyRole, types.KeyBoot).Prefix(true).String(), clientv3.WithPrefix())
	assert.NoError(t, err)
	assert.Empty(t, bootRoot.Kvs, "rejected boot write must not create bundle keys")

	legacyBootAttempt, err := inst.KV().Get(ctx,
		inst.KV().Key(types.KeyRole, types.KeyFiles, "1.2.3.4", "boot").Prefix(true).String(),
		clientv3.WithPrefix())
	assert.NoError(t, err)
	assert.Empty(t, legacyBootAttempt.Kvs, "rejected boot write must not fall back to legacy storage")

	// Legacy single-file API still works end to end.
	legacyData := []byte("legacy-config")
	assert.NoError(t, role.APIFilesPut().Interact(ctx, tftp.APIFilesPutInput{
		Name: "plain.cfg", Host: "legacyhost", Data: legacyData,
	}, &struct{}{}))

	var list tftp.APIFilesGetOutput
	assert.NoError(t, role.APIFilesGet().Interact(ctx, struct{}{}, &list))
	assert.Len(t, list.Files, 1)
	assert.Equal(t, "legacyhost", list.Files[0].Host)
	assert.Equal(t, "plain.cfg", list.Files[0].Name)
	assert.Equal(t, len(legacyData), list.Files[0].SizeBytes)

	var dl tftp.APIFilesDownloadOutput
	assert.NoError(t, role.APIFilesDownload().Interact(ctx,
		tftp.APIFilesDownloadInput{Host: "legacyhost", Name: "plain.cfg"}, &dl))
	assert.Equal(t, legacyData, dl.Data)

	// Publish boot material; it must never appear through the legacy listing.
	createBootBundle(t, role, "b")
	m1, _ := bootReadManifests()
	createCompleteBootVersion(t, role, "b", "v1", "v1", m1)
	publishBoot(t, role, "b", "v1")

	listAfterBoot := tftp.APIFilesGetOutput{}
	assert.NoError(t, role.APIFilesGet().Interact(ctx, struct{}{}, &listAfterBoot))
	assert.Len(t, listAfterBoot.Files, 1)
	assert.Equal(t, "plain.cfg", listAfterBoot.Files[0].Name)

	// Legacy TFTP write + read path for ordinary per-host files is unchanged.
	assert.NoError(t, role.Writer("plain.cfg",
		IncomingTransfer{bytes.NewBufferString("tftp-plain"), "1.2.3.4"}))
	buf := bytes.NewBuffer(nil)
	assert.NoError(t, role.Reader("plain.cfg", OutgoingTransfer{buf}))
	assert.Equal(t, []byte("tftp-plain"), buf.Bytes())

	// Deleting the legacy file does not touch any boot bundle key.
	assert.NoError(t, role.APIFilesDelete().Interact(ctx,
		tftp.APIFilesDeleteInput{Host: "legacyhost", Name: "plain.cfg"}, &struct{}{}))
	data, err := readBootTFTP(t, role, "9.9.9.9", "entry.efi")
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry.efi@v1"), data)
}
