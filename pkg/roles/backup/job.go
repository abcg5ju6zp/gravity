package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/roles/backup/types"
	"github.com/google/uuid"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	// Backup trigger sources, recorded with every backup identity.
	SourceManual = "manual"
	SourceCron   = "cron"
	SourceEvent  = "event"
)

const (
	// Phases a backup job moves through. They are persisted after every
	// successful step so an interrupted run can resume from the last
	// durable stage instead of starting over.
	PhaseStarted     = "started"
	PhaseSnapshotted = "snapshotted"
	PhaseVerified    = "verified"
	PhaseUploaded    = "uploaded"
	PhaseManifested  = "manifested"
	PhaseSucceeded   = "succeeded"
	PhaseFailed      = "failed"
)

const (
	dirSnapshots = "snapshots"
	dirTemp      = "tmp"
	// LocalLatestEntry is the well-known recoverable entry pointing at the
	// newest verified snapshot. It is kept compatible with the restore
	// workflow (gravity cli snapshot restore).
	LocalLatestEntry = "gravity-local-snapshot"
	// manifestSuffix is appended to an immutable remote object key to store
	// the object's verification manifest. It is only written after the
	// object itself is fully uploaded and confirmed.
	manifestSuffix = ".manifest.json"
	// legacyTempPattern matches unpublished temp files left behind by
	// older gravity versions.
	legacyTempPattern = "temp-gravity-snapshot.*.etcd"
)

// jobRecord is the durable, per-trigger identity of a backup. It pins the
// cluster revision captured in the snapshot, the immutable local and remote
// target locations and the trigger source. A record is written before any
// potentially destructive work and is updated after every completed stage.
type jobRecord struct {
	ID       string `json:"id"`
	Instance string `json:"instance"`
	Version  string `json:"version"`
	Source   string `json:"source"`

	// Pinned target locations. They never change for the lifetime of the
	// job, even if the role configuration is changed while it is pending.
	Bucket    string `json:"bucket,omitempty"`
	RemoteKey string `json:"remoteKey,omitempty"`
	LocalPath string `json:"localPath"`
	TempPath  string `json:"tempPath,omitempty"`
	FileName  string `json:"fileName"`

	StartedAt  time.Time  `json:"startedAt"`
	UpdatedAt  time.Time  `json:"updatedAt"`
	FinishedAt *time.Time `json:"finishedAt,omitempty"`
	Phase      string     `json:"phase"`

	// Verified content metadata.
	Size         int64  `json:"size,omitempty"`
	SHA256       string `json:"sha256,omitempty"`
	Revision     int64  `json:"revision,omitempty"`
	SnapshotHash uint32 `json:"snapshotHash,omitempty"`
	TotalKeys    int    `json:"totalKeys,omitempty"`

	// Error describes the target that still needs retrying when the job is
	// not in a terminal state.
	Error    string `json:"error,omitempty"`
	Attempts int    `json:"attempts"`
}

// remoteManifest is published next to the remote snapshot object. It is the
// remote-side success marker and allows a downloaded object to be verified
// against size and digest.
type remoteManifest struct {
	ID         string    `json:"id"`
	Instance   string    `json:"instance"`
	Version    string    `json:"gravityVersion"`
	Source     string    `json:"source"`
	ObjectKey  string    `json:"objectKey"`
	Size       int64     `json:"size"`
	SHA256     string    `json:"sha256"`
	Revision   int64     `json:"revision"`
	Hash       uint32    `json:"etcdHash"`
	TotalKeys  int       `json:"totalKeys"`
	CreatedAt  time.Time `json:"createdAt"`
}

func (r *Role) jobsPrefix() string {
	return r.i.KV().Key(
		types.KeyRole,
		types.KeyJobs,
		extconfig.Get().Instance.Identifier,
	).Prefix(true).String()
}

func (r *Role) jobKey(id string) string {
	return r.i.KV().Key(
		types.KeyRole,
		types.KeyJobs,
		extconfig.Get().Instance.Identifier,
		id,
	).String()
}

// newJobID returns a sortable, unique and non-reusable backup identifier.
func newJobID() string {
	raw := strings.ReplaceAll(uuid.NewString(), "-", "")
	return fmt.Sprintf("%s-%s",
		time.Now().UTC().Format("20060102T150405.000000000Z"),
		raw[:12],
	)
}

func jobFileName(id string) string {
	return fmt.Sprintf("gravity-snapshot-%s.etcd", id)
}

func (r *Role) newRecord(id, source string) *jobRecord {
	backupDir := extconfig.Get().Dirs().BackupDir
	fileName := jobFileName(id)
	rec := &jobRecord{
		ID:        id,
		Instance:  extconfig.Get().Instance.Identifier,
		Version:   r.version,
		Source:    source,
		FileName:  fileName,
		LocalPath: path.Join(backupDir, dirSnapshots, fileName),
		TempPath:  path.Join(backupDir, dirTemp, id+".partial"),
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
		Phase:     PhaseStarted,
	}
	if r.cfg != nil && r.cfg.Endpoint != "" {
		rec.Bucket = r.cfg.Bucket
		rec.RemoteKey = fmt.Sprintf("%s/gravity/%s/%s/%s",
			r.cfg.Path,
			r.version,
			extconfig.Get().Instance.Identifier,
			fileName,
		)
	}
	return rec
}

// claimJob atomically creates the job record if it does not exist yet. When
// the record already exists (duplicate request or a previous attempt that was
// interrupted), the stored record is returned so callers reconcile against
// the original identity instead of creating a competing attempt.
func (r *Role) claimJob(ctx context.Context, id, source string) (rec *jobRecord, existed bool) {
	rec = r.newRecord(id, source)
	raw, err := json.Marshal(rec)
	if err != nil {
		r.log.Warn("failed to marshal backup job", zap.Error(err))
		return rec, false
	}
	key := r.jobKey(id)
	txn, err := r.i.KV().Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(raw))).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		// KV is required for taking a snapshot anyway; log and continue, the
		// driving stage will fail in a recoverable way.
		r.log.Warn("failed to claim backup job", zap.String("id", id), zap.Error(err))
		return rec, false
	}
	if txn.Succeeded {
		return rec, false
	}
	for _, op := range txn.Responses {
		if gr := op.GetResponseRange(); gr != nil && len(gr.Kvs) > 0 {
			var existing jobRecord
			if err := json.Unmarshal(gr.Kvs[0].Value, &existing); err != nil {
				r.log.Warn("failed to unmarshal existing backup job", zap.String("id", id), zap.Error(err))
				return rec, false
			}
			return &existing, true
		}
	}
	return rec, true
}

func (r *Role) persistJob(ctx context.Context, rec *jobRecord) error {
	rec.UpdatedAt = time.Now()
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	_, err = r.i.KV().Put(ctx, r.jobKey(rec.ID), string(raw))
	return err
}

func (r *Role) listJobs(ctx context.Context) ([]jobRecord, error) {
	res, err := r.i.KV().Get(ctx, r.jobsPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	jobs := make([]jobRecord, 0, len(res.Kvs))
	for _, kv := range res.Kvs {
		var rec jobRecord
		if err := json.Unmarshal(kv.Value, &rec); err != nil {
			r.log.Warn("failed to unmarshal backup job", zap.Error(err))
			continue
		}
		jobs = append(jobs, rec)
	}
	return jobs, nil
}

func (rec *jobRecord) remoteTarget() string {
	if rec.Bucket == "" {
		return ""
	}
	return fmt.Sprintf("s3://%s/%s", rec.Bucket, rec.RemoteKey)
}

func (rec *jobRecord) terminal() bool {
	return rec.Phase == PhaseSucceeded || rec.Phase == PhaseFailed
}

// activeJob tracks an in-process execution so concurrent triggers with the
// same identity attach to the same run instead of racing each other.
type activeJob struct {
	id     string
	rec    *jobRecord
	owner  bool
	done   chan struct{}
	status *BackupStatus
}

// beginBackup claims the job identity and, when a run for that identity is
// already executing in this process, attaches to it. A non-nil terminal
// status indicates the identity already reached a final state and no
// execution is required.
func (r *Role) beginBackup(ctx context.Context, source, requestID string) (rec *jobRecord, run *activeJob, terminal *BackupStatus) {
	id := requestID
	if id == "" {
		id = newJobID()
	}
	r.jobsM.Lock()
	if current, ok := r.jobs[id]; ok {
		r.jobsM.Unlock()
		return current.rec, current, nil
	}
	run = &activeJob{
		id:    id,
		owner: true,
		done:  make(chan struct{}),
	}
	r.jobs[id] = run
	r.jobsM.Unlock()

	existing, existed := r.claimJob(ctx, id, source)
	run.rec = existing
	if existed && existing.terminal() {
		terminal := r.statusFromRecord(existing)
		r.complete(run, terminal)
		return existing, run, terminal
	}
	return existing, run, nil
}

// complete publishes an execution's final status and unregisters it. Waiters
// attached to the same identity observe the status exactly once.
func (r *Role) complete(run *activeJob, status *BackupStatus) {
	r.jobsM.Lock()
	defer r.jobsM.Unlock()
	run.status = status
	if cur, ok := r.jobs[run.id]; ok && cur == run {
		delete(r.jobs, run.id)
		close(run.done)
	}
}

// executeBackup runs the job pipeline when this caller owns the execution, or
// waits for the owning execution when attached to a concurrent trigger.
func (r *Role) executeBackup(ctx context.Context, rec *jobRecord, run *activeJob) *BackupStatus {
	if !run.owner {
		select {
		case <-run.done:
			if run.status != nil {
				return run.status
			}
		case <-ctx.Done():
			id := run.id
			source := SourceManual
			if rec != nil {
				id = rec.ID
				source = rec.Source
			}
			return &BackupStatus{
				ID:     id,
				Time:   time.Now(),
				Status: BackupStatusStarted,
				Source: source,
			}
		}
		// The owner unregistered without writing a status; reconcile the
		// identity in this caller instead of trusting an unknown outcome.
		if rec != nil {
			return r.driveBackup(ctx, rec)
		}
		return run.status
	}
	status := r.driveBackup(ctx, rec)
	r.complete(run, status)
	return status
}

// StartBackup triggers a backup with an explicit source and optional
// client-provided idempotency identifier. It is the single entry point for
// API, cron and event triggers.
func (r *Role) StartBackup(ctx context.Context, source, requestID string) *BackupStatus {
	rec, run, terminal := r.beginBackup(ctx, source, requestID)
	if terminal != nil {
		return terminal
	}
	return r.executeBackup(ctx, rec, run)
}
