package discovery

import (
	"context"
	"strings"
	"time"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/roles/discovery/types"
	"github.com/google/uuid"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// APIApplyStepPin is the client-echoed pinned revision for one step.
type APIApplyStepPin struct {
	Name           string `json:"name" required:"true"`
	TargetKey      string `json:"targetKey,omitempty"`
	CreateRevision int64  `json:"createRevision"`
	ModRevision    int64  `json:"modRevision"`
}

// APIApplyItemInput is one selected device with the preview pins.
type APIApplyItemInput struct {
	Identifier  string `json:"identifier" required:"true"`
	ContentHash string `json:"contentHash" required:"true"`
	// DeviceRevision pins the live device key revision.
	DeviceRevision int64             `json:"deviceRevision"`
	Steps          []APIApplyStepPin `json:"steps" required:"true"`
}

// APIAppliesCreateInput creates a batch apply job.
type APIAppliesCreateInput struct {
	IdempotencyKey string   `json:"idempotencyKey,omitempty"`
	ScanID         string   `json:"scanId" required:"true"`
	Targets        []string `json:"targets" required:"true"`

	DHCPScope string `json:"dhcpScope,omitempty"`
	DNSZone   string `json:"dnsZone,omitempty"`

	Items []APIApplyItemInput `json:"items" required:"true"`
}

// APIApplyJob is the wire representation of a stored job.
type APIApplyJob struct {
	ApplyID string `json:"applyId" required:"true"`

	IdempotencyKey string   `json:"idempotencyKey,omitempty"`
	Subnet         string   `json:"subnet" required:"true"`
	ScanID         string   `json:"scanId" required:"true"`
	SourceRevision int64    `json:"sourceRevision"`
	Targets        []string `json:"targets" required:"true"`
	DHCPScope      string   `json:"dhcpScope,omitempty"`
	DNSZone        string   `json:"dnsZone,omitempty"`

	Status            string `json:"status" required:"true"`
	FencingGeneration int64  `json:"fencingGeneration"`
	NodeID            string `json:"nodeId,omitempty"`

	CreatedAt  time.Time  `json:"createdAt" required:"true"`
	StartedAt  *time.Time `json:"startedAt,omitempty"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`

	Items []ApplyItem `json:"items" required:"true"`
}

func apiJobFromModel(job *ApplyJob) APIApplyJob {
	return APIApplyJob{
		ApplyID:           job.ID,
		IdempotencyKey:    job.IdempotencyKey,
		Subnet:            job.Subnet,
		ScanID:            job.ScanID,
		SourceRevision:    job.SourceRevision,
		Targets:           job.Targets,
		DHCPScope:         job.DHCPScope,
		DNSZone:           job.DNSZone,
		Status:            job.Status,
		FencingGeneration: job.FencingGeneration,
		NodeID:            job.NodeID,
		CreatedAt:         job.CreatedAt,
		StartedAt:         job.StartedAt,
		FinishedAt:        job.FinishedAt,
		Items:             job.Items,
	}
}

func (r *Role) APIAppliesCreate() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIAppliesCreateInput, output *APIApplyJob) error {
		if input.ScanID == "" || len(input.Targets) == 0 || len(input.Items) == 0 {
			return status.Wrap(errInvalidJobInput, status.InvalidArgument)
		}
		targets, err := normalizeTargets(input.Targets)
		if err != nil {
			return status.Wrap(err, status.InvalidArgument)
		}
		if contains(targets, types.TargetDHCP) && input.DHCPScope == "" {
			return status.Wrap(errInvalidJobInput, status.InvalidArgument)
		}

		sc, err := r.findScan(ctx, input.ScanID)
		if err == errScanNotFound {
			return status.Wrap(err, status.NotFound)
		}
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		if sc.Status != types.ScanStatusFinished {
			return status.Wrap(errScanNotFinished, status.InvalidArgument)
		}
		snapshots, err := r.listScanDevices(ctx, sc.Subnet, sc.Identifier)
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		snapshotByID := map[string]ScanDevice{}
		for _, snap := range snapshots {
			snapshotByID[snap.Identifier] = snap
		}

		job := r.newApplyJob()
		job.ID = uuid.New().String()
		job.IdempotencyKey = input.IdempotencyKey
		job.Subnet = sc.Subnet
		job.ScanID = sc.Identifier
		job.SourceRevision = sc.SourceRevision
		job.Targets = targets
		job.DHCPScope = input.DHCPScope
		job.DNSZone = input.DNSZone
		job.Status = types.JobStatusPending
		job.FencingGeneration = 1
		job.NodeID = extconfig.Get().Instance.Identifier
		job.CreatedAt = time.Now().UTC()
		job.Items = make([]ApplyItem, 0, len(input.Items))

		for _, in := range input.Items {
			snap, ok := snapshotByID[in.Identifier]
			if !ok {
				return status.Wrap(errInvalidJobInput, status.InvalidArgument)
			}
			if in.ContentHash != snap.ContentHash {
				return status.Wrap(errInvalidJobInput, status.InvalidArgument)
			}
			steps, err := buildPinnedSteps(targets, in.Steps, in.DeviceRevision)
			if err != nil {
				return status.Wrap(err, status.InvalidArgument)
			}
			job.Items = append(job.Items, ApplyItem{
				Identifier:     snap.Identifier,
				Hostname:       snap.Hostname,
				IP:             snap.IP,
				MAC:            snap.MAC,
				Subnet:         snap.Subnet,
				ContentHash:    snap.ContentHash,
				DeviceRevision: in.DeviceRevision,
				Result:         types.ItemResultPending,
				Steps:          steps,
			})
		}

		raw, err := job.marshal()
		if err != nil {
			return status.Wrap(err, status.Internal)
		}

		if input.IdempotencyKey != "" {
			idemKey := r.idempotencyKey(input.IdempotencyKey)
			res, err := r.i.KV().Txn(ctx).
				If(clientv3.Compare(clientv3.CreateRevision(idemKey), "=", 0)).
				Then(
					clientv3.OpPut(idemKey, job.ID),
					clientv3.OpPut(r.jobKey(job.ID), string(raw)),
				).
				Else(clientv3.OpGet(idemKey)).
				Commit()
			if err != nil {
				return status.Wrap(err, status.Internal)
			}
			if !res.Succeeded {
				// Duplicate submission: return the existing job unchanged.
				rangeRes := res.Responses[0].GetResponseRange()
				if rangeRes == nil || len(rangeRes.Kvs) == 0 {
					return status.Wrap(errIdempotencyDrift, status.AlreadyExists)
				}
				existingID := string(rangeRes.Kvs[0].Value)
				existing, err := r.getJob(ctx, existingID)
				if err != nil {
					return status.Wrap(err, status.Internal)
				}
				out := apiJobFromModel(existing)
				*output = out
				return nil
			}
		} else {
			_, err = r.i.KV().Put(ctx, r.jobKey(job.ID), string(raw))
			if err != nil {
				return status.Wrap(err, status.Internal)
			}
		}

		// Kick the asynchronous worker (no-op until the worker is wired).
		r.enqueueApplyJob(job.ID)

		out := apiJobFromModel(job)
		*output = out
		return nil
	})
	u.SetName("discovery.create_apply")
	u.SetTitle("Create discovery apply job")
	u.SetTags("roles/discovery")
	u.SetExpectedErrors(status.InvalidArgument, status.NotFound, status.AlreadyExists, status.Internal)
	return u
}

// buildPinnedSteps validates that the submitted pins match exactly the steps
// implied by the targets and returns initialized pending steps.
func buildPinnedSteps(targets []string, pins []APIApplyStepPin, deviceRev int64) ([]ApplyStep, error) {
	expected := map[string]struct{}{
		types.StepNameDeviceMark: {},
	}
	for _, t := range targets {
		switch t {
		case types.TargetDHCP:
			expected[types.StepNameDHCPReservation] = struct{}{}
		case types.TargetDNS:
			expected[types.StepNameDNSForward] = struct{}{}
			expected[types.StepNameDNSReverse] = struct{}{}
		}
	}
	byName := map[string]APIApplyStepPin{}
	for _, pin := range pins {
		if _, ok := expected[pin.Name]; !ok {
			return nil, errInvalidJobInput
		}
		if _, dup := byName[pin.Name]; dup {
			return nil, errInvalidJobInput
		}
		byName[pin.Name] = pin
	}
	if len(byName) != len(expected) {
		return nil, errInvalidJobInput
	}

	// Deterministic step order.
	names := []string{types.StepNameDeviceMark}
	if contains(targets, types.TargetDHCP) {
		names = append(names, types.StepNameDHCPReservation)
	}
	if contains(targets, types.TargetDNS) {
		names = append(names, types.StepNameDNSForward, types.StepNameDNSReverse)
	}
	steps := make([]ApplyStep, 0, len(names))
	for _, name := range names {
		pin := byName[name]
		step := ApplyStep{
			Name:                   name,
			TargetKey:              pin.TargetKey,
			ExpectedCreateRevision: pin.CreateRevision,
			ExpectedModRevision:    pin.ModRevision,
			Status:                 types.StepStatusPending,
		}
		if name == types.StepNameDeviceMark {
			// The device step is pinned via its live revision directly.
			step.ExpectedModRevision = deviceRev
		}
		steps = append(steps, step)
	}
	return steps, nil
}

func normalizeTargets(in []string) ([]string, error) {
	out := make([]string, 0, 2)
	for _, t := range in {
		t = strings.ToLower(strings.TrimSpace(t))
		if t != types.TargetDHCP && t != types.TargetDNS {
			return nil, errInvalidJobInput
		}
		if !contains(out, t) {
			out = append(out, t)
		}
	}
	return out, nil
}

func contains(haystack []string, needle string) bool {
	for _, h := range haystack {
		if h == needle {
			return true
		}
	}
	return false
}

type APIAppliesGetInput struct {
	ScanID string `query:"scanId"`
	Status string `query:"status"`
}

type APIAppliesGetOutput struct {
	Jobs []APIApplyJob `json:"jobs" required:"true"`
}

func (r *Role) APIAppliesGet() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIAppliesGetInput, output *APIAppliesGetOutput) error {
		res, err := r.i.KV().Get(ctx, r.i.KV().Key(types.KeyRole, types.KeyApplies).Prefix(true).String(), clientv3.WithPrefix())
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		output.Jobs = []APIApplyJob{}
		for _, kv := range res.Kvs {
			job, err := r.jobFromKV(kv)
			if err != nil {
				r.log.Warn("failed to parse apply job", zap.Error(err))
				continue
			}
			if input.ScanID != "" && job.ScanID != input.ScanID {
				continue
			}
			if input.Status != "" && job.Status != input.Status {
				continue
			}
			output.Jobs = append(output.Jobs, apiJobFromModel(job))
		}
		return nil
	})
	u.SetName("discovery.get_applies")
	u.SetTitle("Discovery apply jobs")
	u.SetTags("roles/discovery")
	u.SetExpectedErrors(status.Internal)
	return u
}

type APIApplyGetInput struct {
	ApplyID string `query:"applyId" required:"true"`
}

func (r *Role) APIApplyGet() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIApplyGetInput, output *APIApplyJob) error {
		job, err := r.getJob(ctx, input.ApplyID)
		if err == errJobNotFound {
			return status.NotFound
		}
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		*output = apiJobFromModel(job)
		return nil
	})
	u.SetName("discovery.get_apply")
	u.SetTitle("Discovery apply job")
	u.SetTags("roles/discovery")
	u.SetExpectedErrors(status.NotFound, status.Internal)
	return u
}
