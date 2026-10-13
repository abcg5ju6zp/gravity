package discovery_test

import (
	"encoding/json"
	"testing"
	"time"

	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles/dhcp"
	dhcpTypes "beryju.io/gravity/pkg/roles/dhcp/types"
	"beryju.io/gravity/pkg/roles/discovery"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/roles/dns"
	dnsTypes "beryju.io/gravity/pkg/roles/dns/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type previewFixture struct {
	rootInst *instance.Instance
	role     *discovery.Role
	scanID   string
	subnet   string
	ip       string
	mac      string
}

func putFinishedScan(t *testing.T, f *previewFixture) {
	t.Helper()
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	meta := discovery.Scan{
		Subnet:         f.subnet,
		CIDR:           "192.0.2.0/24",
		SourceRevision: 100,
		Status:         discoveryTypes.ScanStatusFinished,
		StartedAt:      time.Now().Add(-time.Minute),
		FinishedAt:     time.Now(),
		DeviceCount:    1,
	}
	raw, err := json.Marshal(&meta)
	require.NoError(t, err)
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyScans, f.subnet, f.scanID).String(),
		string(raw),
	))
	snap := discovery.ScanDevice{
		Identifier:  f.ip,
		Hostname:    "test",
		IP:          f.ip,
		MAC:         f.mac,
		Subnet:      f.subnet,
		ScanID:      f.scanID,
		SourceRev:   100,
		ObservedAt:  time.Now(),
		ContentHash: discovery.DeviceContentHash("test", f.ip, f.mac, f.subnet),
	}
	sraw, err := json.Marshal(&snap)
	require.NoError(t, err)
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyScans, f.subnet, f.scanID, "devices", f.ip).String(),
		string(sraw),
	))
}

func putLiveDevice(t *testing.T, f *previewFixture, dev discovery.Device) {
	t.Helper()
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	if dev.IP == "" {
		dev.IP = f.ip
	}
	raw, err := json.Marshal(&dev)
	require.NoError(t, err)
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, f.ip).String(),
		string(raw),
	))
}

func newPreviewFixture(t *testing.T, withRoles bool, zones ...string) *previewFixture {
	t.Helper()
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	kv := rootInst.ForRole("test", ctx).KV()

	f := &previewFixture{
		rootInst: rootInst,
		role:     discovery.New(rootInst.ForRole("discovery", ctx)),
		scanID:   tests.RandomString("scan"),
		subnet:   "subnet-a",
		ip:       "192.0.2.10",
		mac:      "aa:bb:cc:00:00:10",
	}

	if withRoles {
		tests.PanicIfError(kv.Put(
			ctx,
			kv.Key(dhcpTypes.KeyRole, dhcpTypes.KeyScopes, "test").String(),
			tests.MustJSON(dhcp.Scope{
				SubnetCIDR: "192.0.2.0/24",
				IPAM:       map[string]string{"type": "internal", "range_start": "192.0.2.1", "range_end": "192.0.2.250"},
				DNS:        &dhcp.ScopeDNS{Zone: "gravity.beryju.io."},
			}),
		))
		for _, z := range zones {
			tests.PanicIfError(kv.Put(
				ctx,
				kv.Key(dnsTypes.KeyRole, dnsTypes.KeyZones, z).String(),
				tests.MustJSON(dns.Zone{}),
			))
		}
		dhcpRole := dhcp.New(rootInst.ForRole("dhcp", ctx))
		require.NoError(t, dhcpRole.Start(ctx, []byte(tests.MustJSON(dhcp.RoleConfig{Port: 0}))))
		t.Cleanup(dhcpRole.Stop)
		dnsRole := dns.New(rootInst.ForRole("dns", ctx))
		require.NoError(t, dnsRole.Start(ctx, []byte(tests.MustJSON(dns.RoleConfig{Port: 0}))))
		t.Cleanup(dnsRole.Stop)
	}

	putFinishedScan(t, f)
	putLiveDevice(t, f, discovery.Device{
		Hostname:       "test",
		MAC:            f.mac,
		Source:         discoveryTypes.DeviceSourceScan,
		ScanID:         f.scanID,
		SourceRevision: 100,
	})
	return f
}

func previewInput(scanID string) discovery.APIAppliesPreviewInput {
	return discovery.APIAppliesPreviewInput{
		ScanID:    scanID,
		Targets:   []string{discoveryTypes.TargetDHCP, discoveryTypes.TargetDNS},
		DHCPScope: "test",
		DNSZone:   "gravity.beryju.io.",
	}
}

func currentRevision(t *testing.T, f *previewFixture) int64 {
	t.Helper()
	ctx := tests.Context()
	kv := f.rootInst.ForRole("test", ctx).KV()
	res, err := kv.Get(ctx, "/", clientv3.WithPrefix())
	require.NoError(t, err)
	return res.Header.Revision
}

func TestApplyPreviewGreen(t *testing.T) {
	f := newPreviewFixture(t, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx := tests.Context()

	revBefore := currentRevision(t, f)
	var out discovery.APIAppliesPreviewOutput
	require.NoError(t, f.role.APIAppliesPreview().Interact(ctx, previewInput(f.scanID), &out))
	revAfter := currentRevision(t, f)
	assert.Equal(t, revBefore, revAfter, "preview is read-only")

	require.Len(t, out.Items, 1)
	item := out.Items[0]
	assert.True(t, item.Executable, "green item must be executable: %v", item.Reasons)
	assert.Empty(t, item.Reasons)
	assert.Positive(t, item.DeviceRevision)
	assert.Equal(t, discovery.DeviceContentHash("test", f.ip, f.mac, f.subnet), item.ContentHash)

	byName := map[string]discovery.APIApplyPreviewStep{}
	for _, s := range item.Steps {
		byName[s.Name] = s
	}
	require.Contains(t, byName, discoveryTypes.StepNameDHCPReservation)
	require.Contains(t, byName, discoveryTypes.StepNameDNSForward)
	require.Contains(t, byName, discoveryTypes.StepNameDNSReverse)
	for name, s := range byName {
		assert.Equal(t, "create", s.Action, name)
		assert.Empty(t, s.Reason, name)
		assert.Equal(t, int64(0), s.CreateRev, name)
		assert.Equal(t, int64(0), s.ModRev, name)
		assert.NotEmpty(t, s.TargetKey, name)
	}
	assert.Contains(t, byName[discoveryTypes.StepNameDHCPReservation].TargetKey, f.mac)
	assert.Contains(t, byName[discoveryTypes.StepNameDNSForward].TargetKey, "/test/A/"+f.ip)
	assert.Contains(t, byName[discoveryTypes.StepNameDNSReverse].TargetKey, "/10.2/PTR/"+f.ip)
}

func TestApplyPreviewReasons(t *testing.T) {
	// Reverse zone missing.
	f := newPreviewFixture(t, true, "gravity.beryju.io.")
	ctx := tests.Context()
	var out discovery.APIAppliesPreviewOutput
	require.NoError(t, f.role.APIAppliesPreview().Interact(ctx, previewInput(f.scanID), &out))
	require.Len(t, out.Items, 1)
	assert.False(t, out.Items[0].Executable)
	assert.Contains(t, out.Items[0].Reasons, discoveryTypes.ReasonZoneMissing)

	// Newer scan touched the live device.
	f2 := newPreviewFixture(t, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx = tests.Context()
	putLiveDevice(t, f2, discovery.Device{
		Hostname:       "test",
		MAC:            f2.mac,
		Source:         discoveryTypes.DeviceSourceScan,
		ScanID:         tests.RandomString("newer-scan"),
		SourceRevision: 999,
	})
	var out2 discovery.APIAppliesPreviewOutput
	require.NoError(t, f2.role.APIAppliesPreview().Interact(ctx, previewInput(f2.scanID), &out2))
	require.Len(t, out2.Items, 1)
	assert.False(t, out2.Items[0].Executable)
	assert.Contains(t, out2.Items[0].Reasons, discoveryTypes.ReasonDeviceNewerScan)

	// MAC missing for DHCP target: overwrite the snapshot to a MAC-less device.
	f3 := newPreviewFixture(t, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx = tests.Context()
	snapNoMAC := discovery.ScanDevice{
		Identifier:  f3.ip,
		Hostname:    "test",
		IP:          f3.ip,
		Subnet:      f3.subnet,
		ScanID:      f3.scanID,
		SourceRev:   100,
		ObservedAt:  time.Now(),
		ContentHash: discovery.DeviceContentHash("test", f3.ip, "", f3.subnet),
	}
	sraw, err := json.Marshal(&snapNoMAC)
	require.NoError(t, err)
	tests.PanicIfError(f3.rootInst.ForRole("test", ctx).KV().Put(
		ctx,
		f3.rootInst.ForRole("test", ctx).KV().Key(discoveryTypes.KeyRole, discoveryTypes.KeyScans, f3.subnet, f3.scanID, "devices", f3.ip).String(),
		string(sraw),
	))
	putLiveDevice(t, f3, discovery.Device{
		Hostname: "test",
		Source:   discoveryTypes.DeviceSourceScan,
		ScanID:   f3.scanID,
	})
	in := previewInput(f3.scanID)
	var out3 discovery.APIAppliesPreviewOutput
	require.NoError(t, f3.role.APIAppliesPreview().Interact(ctx, in, &out3))
	require.Len(t, out3.Items, 1)
	assert.False(t, out3.Items[0].Executable)
	assert.Contains(t, out3.Items[0].Reasons, discoveryTypes.ReasonMACRequired)

	// Claimed device.
	f4 := newPreviewFixture(t, true, "gravity.beryju.io.", "0.192.in-addr.arpa.")
	ctx = tests.Context()
	putLiveDevice(t, f4, discovery.Device{
		Hostname:    "test",
		MAC:         f4.mac,
		Source:      discoveryTypes.DeviceSourceScan,
		ScanID:      f4.scanID,
		ApplyID:     "apply-existing",
		ApplyStatus: discoveryTypes.StepStatusConfirmed,
	})
	var out4 discovery.APIAppliesPreviewOutput
	require.NoError(t, f4.role.APIAppliesPreview().Interact(ctx, previewInput(f4.scanID), &out4))
	require.Len(t, out4.Items, 1)
	assert.Contains(t, out4.Items[0].Reasons, discoveryTypes.ReasonDeviceClaimed)
}

func TestApplyPreviewRolesUnavailable(t *testing.T) {
	// No dhcp/dns roles started: steps report role_unavailable but the
	// request itself succeeds.
	f := newPreviewFixture(t, false)
	ctx := tests.Context()
	var out discovery.APIAppliesPreviewOutput
	require.NoError(t, f.role.APIAppliesPreview().Interact(ctx, previewInput(f.scanID), &out))
	require.Len(t, out.Items, 1)
	assert.False(t, out.Items[0].Executable)
	for _, s := range out.Items[0].Steps {
		assert.Equal(t, "skip", s.Action)
		assert.Equal(t, discoveryTypes.ReasonRoleUnavailable, s.Reason)
	}
}

func TestApplyPreviewValidation(t *testing.T) {
	f := newPreviewFixture(t, false)
	ctx := tests.Context()

	// Unknown scan.
	err := f.role.APIAppliesPreview().Interact(ctx, discovery.APIAppliesPreviewInput{
		ScanID:  "does-not-exist",
		Targets: []string{discoveryTypes.TargetDHCP},
	}, &discovery.APIAppliesPreviewOutput{})
	assert.Error(t, err)

	// Empty targets.
	err = f.role.APIAppliesPreview().Interact(ctx, discovery.APIAppliesPreviewInput{
		ScanID: f.scanID,
	}, &discovery.APIAppliesPreviewOutput{})
	assert.Error(t, err)

	// Invalid target.
	err = f.role.APIAppliesPreview().Interact(ctx, discovery.APIAppliesPreviewInput{
		ScanID:  f.scanID,
		Targets: []string{"tftp"},
	}, &discovery.APIAppliesPreviewOutput{})
	assert.Error(t, err)
}
