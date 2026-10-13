package discovery_test

import (
	"testing"
	"time"

	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles/discovery"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func newApplyJobFixture(t *testing.T) (*discovery.Role, *instance.Instance, string) {
	t.Helper()
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	role := discovery.New(rootInst.ForRole("discovery", ctx))

	scanID := tests.RandomString("scan")
	subnet := "subnet-a"
	ip := "192.0.2.10"
	kv := rootInst.ForRole("test", ctx).KV()
	meta := discovery.Scan{
		Subnet:         subnet,
		CIDR:           "192.0.2.0/24",
		SourceRevision: 100,
		Status:         discoveryTypes.ScanStatusFinished,
		StartedAt:      time.Now().Add(-time.Minute),
		FinishedAt:     time.Now(),
		DeviceCount:    1,
	}
	mraw := tests.MustJSON(meta)
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyScans, subnet, scanID).String(),
		mraw,
	))
	snap := discovery.ScanDevice{
		Identifier:  ip,
		Hostname:    "test",
		IP:          ip,
		MAC:         "aa:bb:cc:00:00:10",
		Subnet:      subnet,
		ScanID:      scanID,
		SourceRev:   100,
		ObservedAt:  time.Now(),
		ContentHash: discovery.DeviceContentHash("test", ip, "aa:bb:cc:00:00:10", subnet),
	}
	sraw := tests.MustJSON(snap)
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyScans, subnet, scanID, "devices", ip).String(),
		sraw,
	))
	// Matching live device.
	devraw := tests.MustJSON(discovery.Device{
		Hostname: "test", IP: ip, MAC: "aa:bb:cc:00:00:10",
		Source: discoveryTypes.DeviceSourceScan, ScanID: scanID,
	})
	tests.PanicIfError(kv.Put(
		ctx,
		kv.Key(discoveryTypes.KeyRole, discoveryTypes.KeyDevices, ip).String(),
		devraw,
	))
	return role, rootInst, scanID
}

func withPrefixOpt() clientv3.OpOption {
	return clientv3.WithPrefix()
}

func validCreateInput(scanID string, idem string) discovery.APIAppliesCreateInput {
	return discovery.APIAppliesCreateInput{
		IdempotencyKey: idem,
		ScanID:         scanID,
		Targets:        []string{discoveryTypes.TargetDHCP, discoveryTypes.TargetDNS},
		DHCPScope:      "test",
		DNSZone:        "gravity.beryju.io.",
		Items: []discovery.APIApplyItemInput{{
			Identifier:     "192.0.2.10",
			ContentHash:    discovery.DeviceContentHash("test", "192.0.2.10", "aa:bb:cc:00:00:10", "subnet-a"),
			DeviceRevision: 1, // arbitrary pin; worker validates at execution
			Steps: []discovery.APIApplyStepPin{
				{Name: discoveryTypes.StepNameDeviceMark, ModRevision: 1},
				{Name: discoveryTypes.StepNameDHCPReservation, CreateRevision: 0},
				{Name: discoveryTypes.StepNameDNSForward, CreateRevision: 0},
				{Name: discoveryTypes.StepNameDNSReverse, CreateRevision: 0},
			},
		}},
	}
}

func TestAPIAppliesCreate(t *testing.T) {
	role, rootInst, scanID := newApplyJobFixture(t)
	ctx := tests.Context()

	var out discovery.APIApplyJob
	require.NoError(t, role.APIAppliesCreate().Interact(ctx, validCreateInput(scanID, ""), &out))
	assert.NotEmpty(t, out.ApplyID)
	assert.Equal(t, discoveryTypes.JobStatusPending, out.Status)
	assert.Equal(t, int64(1), out.FencingGeneration)
	assert.Equal(t, scanID, out.ScanID)
	if assert.Len(t, out.Items, 1) {
		item := out.Items[0]
		assert.Equal(t, "192.0.2.10", item.Identifier)
		assert.Equal(t, "test", item.Hostname)
		assert.Equal(t, "aa:bb:cc:00:00:10", item.MAC)
		assert.Len(t, item.Steps, 4)
		assert.Equal(t, discoveryTypes.StepNameDeviceMark, item.Steps[0].Name)
		for _, s := range item.Steps {
			assert.Equal(t, discoveryTypes.StepStatusPending, s.Status)
		}
	}

	// The job is persisted.
	var get discovery.APIApplyJob
	require.NoError(t, role.APIApplyGet().Interact(ctx, discovery.APIApplyGetInput{ApplyID: out.ApplyID}, &get))
	assert.Equal(t, out.ApplyID, get.ApplyID)

	// Exactly one job key exists.
	res, err := rootInst.ForRole("test", ctx).KV().Get(
		ctx,
		rootInst.ForRole("test", ctx).KV().Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies).Prefix(true).String(),
		withPrefixOpt(),
	)
	require.NoError(t, err)
	assert.Len(t, res.Kvs, 1)
}

func TestAPIAppliesCreateIdempotent(t *testing.T) {
	role, rootInst, scanID := newApplyJobFixture(t)
	ctx := tests.Context()

	var first discovery.APIApplyJob
	require.NoError(t, role.APIAppliesCreate().Interact(ctx, validCreateInput(scanID, "idem-key"), &first))
	var second discovery.APIApplyJob
	require.NoError(t, role.APIAppliesCreate().Interact(ctx, validCreateInput(scanID, "idem-key"), &second))
	assert.Equal(t, first.ApplyID, second.ApplyID)

	res, err := rootInst.ForRole("test", ctx).KV().Get(
		ctx,
		rootInst.ForRole("test", ctx).KV().Key(discoveryTypes.KeyRole, discoveryTypes.KeyApplies).Prefix(true).String(),
		withPrefixOpt(),
	)
	require.NoError(t, err)
	assert.Len(t, res.Kvs, 1, "duplicate submission must not create a second job")
	idem, err := rootInst.ForRole("test", ctx).KV().Get(
		ctx,
		rootInst.ForRole("test", ctx).KV().Key(discoveryTypes.KeyRole, discoveryTypes.KeyAppliesIdem, "idem-key").String(),
	)
	require.NoError(t, err)
	require.Len(t, idem.Kvs, 1)
	assert.Equal(t, first.ApplyID, string(idem.Kvs[0].Value))
}

func TestAPIAppliesCreateValidation(t *testing.T) {
	role, _, scanID := newApplyJobFixture(t)
	ctx := tests.Context()

	// Unknown scan.
	in := validCreateInput("does-not-exist", "")
	err := role.APIAppliesCreate().Interact(ctx, in, &discovery.APIApplyJob{})
	assert.Error(t, err)

	// Tampered content hash.
	in = validCreateInput(scanID, "")
	in.Items[0].ContentHash = "deadbeef"
	err = role.APIAppliesCreate().Interact(ctx, in, &discovery.APIApplyJob{})
	assert.Error(t, err)

	// Missing a step pin.
	in = validCreateInput(scanID, "")
	in.Items[0].Steps = in.Items[0].Steps[:3]
	err = role.APIAppliesCreate().Interact(ctx, in, &discovery.APIApplyJob{})
	assert.Error(t, err)

	// Unknown step in pins.
	in = validCreateInput(scanID, "")
	in.Items[0].Steps = append(in.Items[0].Steps, discovery.APIApplyStepPin{Name: "bogus"})
	err = role.APIAppliesCreate().Interact(ctx, in, &discovery.APIApplyJob{})
	assert.Error(t, err)

	// dhcp target without scope.
	in = validCreateInput(scanID, "")
	in.DHCPScope = ""
	err = role.APIAppliesCreate().Interact(ctx, in, &discovery.APIApplyJob{})
	assert.Error(t, err)

	// Empty items/targets.
	err = role.APIAppliesCreate().Interact(ctx, discovery.APIAppliesCreateInput{
		ScanID:  scanID,
		Targets: []string{discoveryTypes.TargetDNS},
		DNSZone: "gravity.beryju.io.",
	}, &discovery.APIApplyJob{})
	assert.Error(t, err)

	// No writes from failed validation.
	var get discovery.APIAppliesGetOutput
	require.NoError(t, role.APIAppliesGet().Interact(ctx, discovery.APIAppliesGetInput{}, &get))
	assert.Empty(t, get.Jobs)
}
