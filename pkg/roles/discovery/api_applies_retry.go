package discovery

import (
	"context"

	"beryju.io/gravity/pkg/roles"
	"beryju.io/gravity/pkg/roles/discovery/types"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// APIApplyRetry re-validates open steps against the current store and
// restarts an unfinished job. Confirmed steps (and their objects) are never
// touched.
func (r *Role) APIApplyRetry() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIApplyGetInput, output *APIApplyJob) error {
		job, err := r.getJob(ctx, input.ApplyID)
		if err == errJobNotFound {
			return status.NotFound
		}
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		switch job.Status {
		case types.JobStatusPartial, types.JobStatusFailed, types.JobStatusCancelled:
			// Re-runnable.
		case types.JobStatusCompleted:
			return status.Wrap(errJobTerminal, status.FailedPrecondition)
		default:
			return status.Wrap(errJobBusy, status.FailedPrecondition)
		}

		// Re-pin every open step against the current state. Confirmed steps
		// remain frozen.
		anyOpen := false
		for idx := range job.Items {
			item := &job.Items[idx]
			for sIdx := range item.Steps {
				step := &item.Steps[sIdx]
				if step.Status == types.StepStatusConfirmed {
					continue
				}
				r.repinStep(ctx, job, item, step)
				if step.Status == types.StepStatusPending {
					anyOpen = true
				}
			}
			item.setResult()
		}

		job.FencingGeneration++
		job.FinishedAt = nil
		job.Status = types.JobStatusPending

		if !anyOpen {
			// Nothing left to execute: terminate through the same
			// lease-backed path as the worker, so a terminal key never
			// loses its retention lease.
			job.Status = job.recalculate()
			if err := r.persistTerminalWithRetention(ctx, job); err != nil {
				return status.Wrap(err, status.FailedPrecondition)
			}
			*output = apiJobFromModel(job)
			return nil
		}

		raw, err := job.marshal()
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		key := r.jobKey(job.ID)
		res, err := r.i.KV().Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(key), "=", job.modRev)).
			Then(clientv3.OpPut(key, string(raw))).
			Else(clientv3.OpGet(key)).
			Commit()
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		if !res.Succeeded {
			return status.Wrap(errJobBusy, status.FailedPrecondition)
		}
		if len(res.Responses) > 0 && res.Responses[0].GetResponsePut() != nil {
			job.modRev = res.Responses[0].GetResponsePut().Header.Revision
		}

		r.enqueueApplyJob(job.ID)
		*output = apiJobFromModel(job)
		return nil
	})
	u.SetName("discovery.retry_apply")
	u.SetTitle("Retry discovery apply job")
	u.SetTags("roles/discovery")
	u.SetExpectedErrors(status.NotFound, status.FailedPrecondition, status.Internal)
	return u
}

// repinStep refreshes the expected revisions of one open step from the
// current store and returns it to pending when claimable. Confirmed steps
// are never passed here.
func (r *Role) repinStep(ctx context.Context, job *ApplyJob, item *ApplyItem, step *ApplyStep) {
	switch step.Name {
	case types.StepNameDeviceMark:
		r.repinDeviceStep(ctx, job, item, step)
	case types.StepNameDHCPReservation, types.StepNameDNSForward, types.StepNameDNSReverse:
		r.repinTargetStep(ctx, job, item, step)
	}
}

func (r *Role) repinDeviceStep(ctx context.Context, job *ApplyJob, item *ApplyItem, step *ApplyStep) {
	key := r.i.KV().Key(types.KeyRole, types.KeyDevices, item.Identifier).String()
	res, err := r.i.KV().Get(ctx, key)
	if err != nil {
		step.Status = types.StepStatusFailed
		step.Reason = ""
		step.ErrorMessage = err.Error()
		return
	}
	if len(res.Kvs) == 0 {
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceMissing
		return
	}
	dev := r.deviceFromKV(res.Kvs[0])
	if dev == nil {
		step.Status = types.StepStatusFailed
		step.ErrorMessage = "failed to parse live device"
		return
	}
	switch {
	case dev.ApplyID == job.ID:
		// The mark is already there: reconcile as confirmed.
		step.Status = types.StepStatusConfirmed
		step.Reconciled = true
		step.Reason = ""
		step.ErrorMessage = ""
		step.TargetKey = key
		step.ExpectedCreateRevision = res.Kvs[0].CreateRevision
		step.ExpectedModRevision = res.Kvs[0].ModRevision
		step.ModRev = res.Kvs[0].ModRevision
	case dev.Source == types.DeviceSourceManual:
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceManual
	case dev.ApplyID != "":
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceClaimed
	case dev.SourceRevision > job.SourceRevision:
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceNewerScan
	default:
		step.Status = types.StepStatusPending
		step.Reason = ""
		step.ErrorMessage = ""
		step.TargetKey = key
		step.ExpectedCreateRevision = res.Kvs[0].CreateRevision
		step.ExpectedModRevision = res.Kvs[0].ModRevision
	}
}

func (r *Role) repinTargetStep(ctx context.Context, job *ApplyJob, item *ApplyItem, step *ApplyStep) {
	topic, data, ok := r.buildEnsurePayload(job, item, step)
	if !ok {
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonZoneMissing
		return
	}
	// dry-run re-validation uses no expected pin itself.
	data[types.EventFieldDryRun] = true
	ev := roles.NewEvent(ctx, data)
	r.i.DispatchEvent(topic, ev)
	outcomes := ev.Outcomes()
	if len(outcomes) == 0 {
		// Role still unavailable: keep it executable so a future retry with
		// the role enabled claims it; the worker records the skip again.
		step.Status = types.StepStatusPending
		step.Reason = types.ReasonRoleUnavailable
		step.ExpectedCreateRevision = 0
		step.ExpectedModRevision = 0
		return
	}
	o := outcomes[0]
	step.TargetKey = o.TargetKey
	step.Siblings = o.Siblings
	step.ErrorMessage = ""
	switch o.Action {
	case roles.EnsureActionCreate:
		step.Status = types.StepStatusPending
		step.Reason = ""
		step.ExpectedCreateRevision = 0
		step.ExpectedModRevision = 0
	case roles.EnsureActionKeep:
		// Object exists (already ours or identical data): pin the current
		// revisions; execution confirms without writing.
		step.Status = types.StepStatusPending
		step.Reason = o.Reason
		step.ExpectedCreateRevision = o.CreateRevision
		step.ExpectedModRevision = o.ModRevision
	default:
		step.Status = types.StepStatusConflict
		step.Reason = o.Reason
		if o.ErrorMessage != "" {
			step.Status = types.StepStatusFailed
			step.ErrorMessage = o.ErrorMessage
		}
	}
}
