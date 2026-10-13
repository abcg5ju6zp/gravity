package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"beryju.io/gravity/pkg/roles"
	"beryju.io/gravity/pkg/roles/discovery/types"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
)

// ApplyStep tracks one object change of one apply item. Steps are the unit
// of confirmation: a retry only executes steps that never reached
// confirmed.
type ApplyStep struct {
	Name      string `json:"name"`
	TargetKey string `json:"targetKey,omitempty"`

	// Pinned revisions echoed from the preview.
	ExpectedCreateRevision int64 `json:"expectedCreateRevision"`
	ExpectedModRevision    int64 `json:"expectedModRevision"`

	Status       string     `json:"status"`
	Reason       string     `json:"reason,omitempty"`
	ConfirmedAt  *time.Time `json:"confirmedAt,omitempty"`
	CreateRev    int64      `json:"createRevision,omitempty"`
	ModRev       int64      `json:"modRevision,omitempty"`
	Siblings     int64      `json:"siblings,omitempty"`
	Reconciled   bool       `json:"reconciled,omitempty"`
	ErrorMessage string     `json:"errorMessage,omitempty"`
}

// ApplyItem tracks one device through the job.
type ApplyItem struct {
	Identifier string `json:"identifier"`

	// Pinned snapshot attributes (validated server-side against the
	// immutable scan snapshots).
	Hostname    string `json:"hostname"`
	IP          string `json:"ip"`
	MAC         string `json:"mac,omitempty"`
	Subnet      string `json:"subnet"`
	ContentHash string `json:"contentHash"`

	// Pinned live device revision.
	DeviceRevision int64 `json:"deviceRevision"`

	Result string `json:"result"`
	Reason string `json:"reason,omitempty"`

	Steps []ApplyStep `json:"steps"`
}

// ApplyJob is the durable state machine for one batch apply.
type ApplyJob struct {
	inst roles.Instance `json:"-"`
	log  *zap.Logger    `json:"-"`

	ID string `json:"-"`

	IdempotencyKey string   `json:"idempotencyKey,omitempty"`
	Subnet         string   `json:"subnet"`
	ScanID         string   `json:"scanId"`
	SourceRevision int64    `json:"sourceRevision"`
	Targets        []string `json:"targets"`
	DHCPScope      string   `json:"dhcpScope,omitempty"`
	DNSZone        string   `json:"dnsZone,omitempty"`

	Status            string `json:"status"`
	FencingGeneration int64  `json:"fencingGeneration"`
	NodeID            string `json:"nodeId,omitempty"`

	CreatedAt  time.Time  `json:"createdAt"`
	StartedAt  *time.Time `json:"startedAt,omitempty"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`

	Items []ApplyItem `json:"items"`

	etcdKey string `json:"-"`
	// modRev is the last known ModRevision of the job key, used as the
	// worker fencing token.
	modRev int64 `json:"-"`
}

func (r *Role) newApplyJob() *ApplyJob {
	return &ApplyJob{
		inst: r.i,
		log:  r.log,
	}
}

func (r *Role) jobKey(id string) string {
	return r.i.KV().Key(types.KeyRole, types.KeyApplies, id).String()
}

func (r *Role) idempotencyKey(key string) string {
	return r.i.KV().Key(types.KeyRole, types.KeyAppliesIdem, key).String()
}

func (r *Role) applyLockKey(id string) string {
	return r.i.KV().Key(types.KeyRole, types.KeyApplyLocks, id).String()
}

func (r *Role) jobFromKV(kv *mvccpb.KeyValue) (*ApplyJob, error) {
	job := r.newApplyJob()
	if err := json.Unmarshal(kv.Value, job); err != nil {
		return nil, err
	}
	prefix := r.i.KV().Key(types.KeyRole, types.KeyApplies).Prefix(true).String()
	job.ID = trimKey(string(kv.Key), prefix)
	job.etcdKey = string(kv.Key)
	job.modRev = kv.ModRevision
	return job, nil
}

func (r *Role) getJob(ctx context.Context, id string) (*ApplyJob, error) {
	res, err := r.i.KV().Get(ctx, r.jobKey(id))
	if err != nil {
		return nil, err
	}
	if len(res.Kvs) == 0 {
		return nil, errJobNotFound
	}
	return r.jobFromKV(res.Kvs[0])
}

func (j *ApplyJob) marshal() ([]byte, error) {
	return json.Marshal(j)
}

// terminalStatus reports whether a job status no longer accepts execution.
func terminalStatus(status string) bool {
	switch status {
	case types.JobStatusCompleted, types.JobStatusPartial, types.JobStatusFailed, types.JobStatusCancelled:
		return true
	}
	return false
}

// recalculate derives the item result and terminal job status from step
// states. It never mutates steps.
func (j *ApplyJob) recalculate() string {
	anyApplied := false
	anyOpen := false
	anyBlocked := false
	allSkipped := true
	for idx := range j.Items {
		item := &j.Items[idx]
		confirmed, open, blocked, skipped := item.tally()
		if confirmed {
			anyApplied = true
		}
		if open {
			anyOpen = true
		}
		if blocked {
			anyBlocked = true
		}
		if !skipped {
			allSkipped = false
		}
	}
	if j.Status == types.JobStatusCancelling {
		return types.JobStatusCancelled
	}
	if anyOpen {
		return types.JobStatusRunning
	}
	if allSkipped {
		return types.JobStatusCompleted
	}
	if anyBlocked && !anyApplied {
		return types.JobStatusFailed
	}
	if anyBlocked {
		return types.JobStatusPartial
	}
	return types.JobStatusCompleted
}

// tally summarizes the item's steps: any confirmed, any still open, any
// blocked (conflict/failed/cancelled), all skipped.
func (i *ApplyItem) tally() (confirmed, open, blocked, allSkipped bool) {
	allSkipped = len(i.Steps) > 0
	for idx := range i.Steps {
		s := &i.Steps[idx]
		switch s.Status {
		case types.StepStatusConfirmed:
			confirmed = true
			allSkipped = false
		case types.StepStatusSkipped:
			// no change
		case types.StepStatusPending:
			open = true
			allSkipped = false
		default:
			blocked = true
			allSkipped = false
		}
	}
	return
}

func (i *ApplyItem) setResult() {
	confirmed, open, blocked, allSkipped := i.tally()
	switch {
	case open:
		i.Result = types.ItemResultPending
	case allSkipped:
		i.Result = types.ItemResultSkipped
	case blocked && confirmed:
		i.Result = types.ItemResultConflict
	case blocked:
		// Distinguish hard failure (cancelled/failed) from conflicts where
		// possible; conflict statuses carry reasons on their steps.
		i.Result = types.ItemResultConflict
		for idx := range i.Steps {
			if i.Steps[idx].Status == types.StepStatusCancelled {
				i.Result = types.ItemResultCancelled
				break
			}
			if i.Steps[idx].Status == types.StepStatusFailed {
				i.Result = types.ItemResultFailed
			}
		}
	case confirmed:
		i.Result = types.ItemResultApplied
	}
}

var (
	errJobNotFound       = errors.New("apply job not found")
	errJobTerminal       = errors.New("apply job already in a terminal status")
	errJobBusy           = errors.New("apply job is currently running")
	errJobNotCancellable = errors.New("apply job cannot be cancelled in its current status")
	errInvalidJobInput   = errors.New("invalid apply job input")
	errIdempotencyDrift  = errors.New("idempotency key reused with different parameters")
)

// enqueueApplyJob starts asynchronous execution of a stored job. The
// fenced worker implementation is wired in apply_worker.go.
func (r *Role) enqueueApplyJob(applyID string) {
	go r.runApplyJob(r.ctx, applyID)
}
