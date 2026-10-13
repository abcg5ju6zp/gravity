package discovery_test

import (
	"encoding/json"
	"testing"
	"time"

	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles/discovery"
	"beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/stretchr/testify/assert"
)

func TestAPIScansGet(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	inst := rootInst.ForRole("discovery", ctx)
	role := discovery.New(inst)

	started := time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
	finished := time.Now().UTC().Truncate(time.Second)
	meta := discovery.Scan{
		Subnet:         "subnet-a",
		CIDR:           "192.0.2.0/24",
		SourceRevision: 42,
		Status:         types.ScanStatusFinished,
		StartedAt:      started,
		FinishedAt:     finished,
		DeviceCount:    1,
	}
	raw, err := json.Marshal(&meta)
	assert.NoError(t, err)
	tests.PanicIfError(inst.KV().Put(
		ctx,
		inst.KV().Key(types.KeyRole, types.KeyScans, "subnet-a", "scan-a1").String(),
		string(raw),
	))
	running := meta
	running.Status = types.ScanStatusRunning
	running.Subnet = "subnet-a"
	running.FinishedAt = time.Time{}
	running.DeviceCount = 0
	rraw, err := json.Marshal(&running)
	assert.NoError(t, err)
	tests.PanicIfError(inst.KV().Put(
		ctx,
		inst.KV().Key(types.KeyRole, types.KeyScans, "subnet-a", "scan-a2").String(),
		string(rraw),
	))
	metaB := meta
	metaB.Subnet = "subnet-b"
	braw, err := json.Marshal(&metaB)
	assert.NoError(t, err)
	tests.PanicIfError(inst.KV().Put(
		ctx,
		inst.KV().Key(types.KeyRole, types.KeyScans, "subnet-b", "scan-b1").String(),
		string(braw),
	))

	// All scans.
	var all discovery.APIScansGetOutput
	assert.NoError(t, role.APIScansGet().Interact(ctx, discovery.APIScansGetInput{}, &all))
	assert.Len(t, all.Scans, 3)

	// Subnet filter.
	var filtered discovery.APIScansGetOutput
	assert.NoError(t, role.APIScansGet().Interact(ctx, discovery.APIScansGetInput{Subnet: "subnet-a"}, &filtered))
	if assert.Len(t, filtered.Scans, 2) {
		for _, sc := range filtered.Scans {
			assert.Equal(t, "subnet-a", sc.Subnet)
		}
	}
}

func TestAPIScanDevicesGet(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	inst := rootInst.ForRole("discovery", ctx)
	role := discovery.New(inst)

	observed := time.Now().UTC().Truncate(time.Second)
	snap := discovery.ScanDevice{
		Identifier:  "192.0.2.1",
		Hostname:    "host-a",
		IP:          "192.0.2.1",
		MAC:         "aa:bb:cc",
		Subnet:      "subnet-a",
		ScanID:      "scan-a1",
		SourceRev:   77,
		ObservedAt:  observed,
		ContentHash: discovery.DeviceContentHash("host-a", "192.0.2.1", "aa:bb:cc", "subnet-a"),
	}
	raw, err := json.Marshal(&snap)
	assert.NoError(t, err)
	tests.PanicIfError(inst.KV().Put(
		ctx,
		inst.KV().Key(types.KeyRole, types.KeyScans, "subnet-a", "scan-a1", "devices", "192.0.2.1").String(),
		string(raw),
	))

	// Lookup with subnet hint.
	var out discovery.APIScanDevicesGetOutput
	assert.NoError(t, role.APIScanDevicesGet().Interact(ctx, discovery.APIScanDevicesGetInput{
		ScanID: "scan-a1",
		Subnet: "subnet-a",
	}, &out))
	if assert.Len(t, out.Devices, 1) {
		assert.Equal(t, "host-a", out.Devices[0].Hostname)
		assert.Equal(t, int64(77), out.Devices[0].SourceRevision)
		assert.NotEmpty(t, out.Devices[0].ContentHash)
	}

	// Lookup without subnet (scan id is globally unique).
	var out2 discovery.APIScanDevicesGetOutput
	assert.NoError(t, role.APIScanDevicesGet().Interact(ctx, discovery.APIScanDevicesGetInput{
		ScanID: "scan-a1",
	}, &out2))
	assert.Len(t, out2.Devices, 1)

	// Missing scan is an empty result, missing id is invalid.
	var out3 discovery.APIScanDevicesGetOutput
	assert.NoError(t, role.APIScanDevicesGet().Interact(ctx, discovery.APIScanDevicesGetInput{
		ScanID: "does-not-exist",
	}, &out3))
	assert.Empty(t, out3.Devices)
}

func TestAPIDevicesGetProvenanceFieldsOptional(t *testing.T) {
	tests.Setup(t)
	rootInst := instance.New()
	ctx := tests.Context()
	inst := rootInst.ForRole("discovery", ctx)
	role := discovery.New(inst)

	// Legacy-shaped device: only core fields, new JSON fields omitted.
	legacy, err := json.Marshal(discovery.Device{
		Hostname: "legacy",
		IP:       "192.0.2.10",
		MAC:      "aa:bb:cc",
	})
	assert.NoError(t, err)
	assert.NotContains(t, string(legacy), "scanId")
	tests.PanicIfError(inst.KV().Put(
		ctx,
		inst.KV().Key(types.KeyRole, types.KeyDevices, "192.0.2.10").String(),
		string(legacy),
	))

	// New-shaped device with scan/apply provenance.
	modern, err := json.Marshal(map[string]interface{}{
		"hostname":       "modern",
		"ip":             "192.0.2.20",
		"mac":            "dd:ee:ff",
		"source":         types.DeviceSourceScan,
		"scanId":         "scan-1",
		"sourceRevision": 9,
		"applyId":        "apply-1",
		"applyStatus":    types.StepStatusConfirmed,
	})
	assert.NoError(t, err)
	tests.PanicIfError(inst.KV().Put(
		ctx,
		inst.KV().Key(types.KeyRole, types.KeyDevices, "192.0.2.20").String(),
		string(modern),
	))

	var out discovery.APIDevicesGetOutput
	assert.NoError(t, role.APIDevicesGet().Interact(ctx, discovery.APIDevicesGetInput{}, &out))
	if assert.Len(t, out.Devices, 2) {
		byID := map[string]discovery.APIDevice{}
		for _, d := range out.Devices {
			byID[d.Identifier] = d
		}
		assert.Empty(t, byID["192.0.2.10"].ScanID)
		assert.Empty(t, byID["192.0.2.10"].ApplyID)
		assert.Equal(t, "scan-1", byID["192.0.2.20"].ScanID)
		assert.Equal(t, "apply-1", byID["192.0.2.20"].ApplyID)
		assert.Equal(t, types.StepStatusConfirmed, byID["192.0.2.20"].ApplyStatus)
	}
}
