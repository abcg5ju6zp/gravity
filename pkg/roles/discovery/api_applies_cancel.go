package discovery

import (
	"context"

	"beryju.io/gravity/pkg/roles/discovery/types"
	"github.com/swaggest/usecase"
	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (r *Role) APIApplyCancel() usecase.Interactor {
	u := usecase.NewInteractor(func(ctx context.Context, input APIApplyGetInput, output *APIApplyJob) error {
		job, err := r.getJob(ctx, input.ApplyID)
		if err == errJobNotFound {
			return status.NotFound
		}
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		if terminalStatus(job.Status) {
			return status.Wrap(errJobNotCancellable, status.FailedPrecondition)
		}

		// CAS into cancelling. If the worker moved the revision concurrently
		// (pending -> running), retry once with the fresh revision. A job that
		// already terminalized between read and write is reported back.
		for attempt := 0; attempt < 2; attempt++ {
			// A cancelling job is a successful idempotent no-op for a racing
			// duplicate cancel request.
			if job.Status == types.JobStatusCancelling || job.Status == types.JobStatusCancelled {
				*output = apiJobFromModel(job)
				return nil
			}
			if job.Status != types.JobStatusPending && job.Status != types.JobStatusRunning {
				return status.Wrap(errJobNotCancellable, status.FailedPrecondition)
			}
			job.Status = types.JobStatusCancelling
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
			if res.Succeeded {
				if len(res.Responses) > 0 && res.Responses[0].GetResponsePut() != nil {
					job.modRev = res.Responses[0].GetResponsePut().Header.Revision
				}
				// Ensure an executor observes the cancellation. The lock
				// makes the enqueue a no-op when a worker is already active.
				r.enqueueApplyJob(job.ID)
				*output = apiJobFromModel(job)
				return nil
			}
			rangeRes := res.Responses[0].GetResponseRange()
			if rangeRes == nil || len(rangeRes.Kvs) == 0 {
				return status.Wrap(errJobNotFound, status.NotFound)
			}
			fresh, err := r.jobFromKV(rangeRes.Kvs[0])
			if err != nil {
				return status.Wrap(err, status.Internal)
			}
			if terminalStatus(fresh.Status) {
				*output = apiJobFromModel(fresh)
				return nil
			}
			job = fresh
		}
		return status.Wrap(errJobBusy, status.FailedPrecondition)
	})
	u.SetName("discovery.cancel_apply")
	u.SetTitle("Cancel discovery apply job")
	u.SetTags("roles/discovery")
	u.SetExpectedErrors(status.NotFound, status.FailedPrecondition, status.Internal)
	return u
}
