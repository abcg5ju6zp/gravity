package tftp_test

import (
	"bytes"
	"net"
	"testing"

	"beryju.io/gravity/pkg/roles/tftp"
	"github.com/stretchr/testify/assert"
)

// ipTransfer is a TFTP OutgoingTransfer mock with a configurable client IP.
type ipTransfer struct {
	*bytes.Buffer
	ip string
}

func (ot ipTransfer) RemoteAddr() net.UDPAddr {
	return net.UDPAddr{IP: net.ParseIP(ot.ip), Port: 69}
}

func (ot ipTransfer) SetSize(_ int64) {}

func readBootTFTP(t *testing.T, role *tftp.Role, ip, file string) ([]byte, error) {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	err := role.Reader("boot/b/"+file, ipTransfer{Buffer: buf, ip: ip})
	return buf.Bytes(), err
}

func bootReadManifests() (v1, v2 []tftp.BootFileSpec) {
	v1 = []tftp.BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true},
		{Path: "dir/kernel", Required: true},
	}
	v2 = []tftp.BootFileSpec{
		{Path: "entry.efi", Required: true, Entrypoint: true},
		{Path: "dir/kernel", Required: true},
		{Path: "extra", Required: true},
	}
	return v1, v2
}

func TestBootRead_SessionPinnedAcrossPublish(t *testing.T) {
	role, _, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")
	m1, m2 := bootReadManifests()
	createCompleteBootVersion(t, role, "b", "v1", "v1", m1)
	publishBoot(t, role, "b", "v1")

	// C1 enters through the entrypoint and gets pinned to v1.
	data, err := readBootTFTP(t, role, "9.9.9.1", "entry.efi")
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry.efi@v1"), data)

	// v2 publishes while C1 is mid-boot.
	createCompleteBootVersion(t, role, "b", "v2", "v2", m2)
	publishBoot(t, role, "b", "v2")

	// C1 keeps reading v1, including the immutability guarantee on missing paths.
	data, err = readBootTFTP(t, role, "9.9.9.1", "dir/kernel")
	assert.NoError(t, err)
	assert.Equal(t, []byte("dir/kernel@v1"), data)
	_, err = readBootTFTP(t, role, "9.9.9.1", "extra")
	assert.Error(t, err, "pinned client must not mix in files from v2")

	// A new client entering after the switch is pinned to v2 entirely.
	data, err = readBootTFTP(t, role, "9.9.9.2", "entry.efi")
	assert.NoError(t, err)
	assert.Equal(t, []byte("entry.efi@v2"), data)
	data, err = readBootTFTP(t, role, "9.9.9.2", "dir/kernel")
	assert.NoError(t, err)
	assert.Equal(t, []byte("dir/kernel@v2"), data)
	data, err = readBootTFTP(t, role, "9.9.9.2", "extra")
	assert.NoError(t, err)
	assert.Equal(t, []byte("extra@v2"), data)
}

func TestBootRead_NoPublishedVersion(t *testing.T) {
	role, _, _ := newBootTestRole(t)
	createBootBundle(t, role, "b")

	_, err := readBootTFTP(t, role, "9.9.9.3", "entry.efi")
	assert.Error(t, err)
}
