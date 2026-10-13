package discovery

import (
	"context"
	"time"

	"beryju.io/gravity/pkg/roles/discovery/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// applySweepInterval is how often the role checks for jobs whose owner
// disappeared (crash/restart). The per-job lock with its short session is
// the primary takeover mechanism; the sweep is a safety net.
const applySweepInterval = 30 * time.Second

// recoverApplies re-attaches workers to all open jobs. The per-job etcd lock
// makes enqueueing idempotent across nodes.
func (r *Role) recoverApplies(ctx context.Context) {
	r.enqueueOpenJobs(ctx)
}

// startApplySweep periodically rescues open jobs whose owner stopped
// heart-beating.
func (r *Role) startApplySweep(ctx context.Context) {
	ticker := time.NewTicker(applySweepInterval)
	go func() {
		for {
			select {
			case <-ticker.C:
				r.enqueueOpenJobs(ctx)
			case <-ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (r *Role) enqueueOpenJobs(ctx context.Context) {
	res, err := r.i.KV().Get(
		ctx,
		r.i.KV().Key(types.KeyRole, types.KeyApplies).Prefix(true).String(),
		clientv3.WithPrefix(),
	)
	if err != nil {
		r.log.Warn("failed to list apply jobs for recovery", zap.Error(err))
		return
	}
	for _, kv := range res.Kvs {
		job, err := r.jobFromKV(kv)
		if err != nil {
			continue
		}
		switch job.Status {
		case types.JobStatusPending, types.JobStatusRunning, types.JobStatusCancelling:
			r.enqueueApplyJob(job.ID)
		}
	}
}
