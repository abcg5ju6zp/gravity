package backup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/roles/backup/types"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
)

type BackupStatus struct {
	ID        string    `json:"id,omitempty"`
	Time      time.Time `json:"time" required:"true"`
	Status    string    `json:"status,omitempty" required:"true"`
	Error     string    `json:"error,omitempty" required:"true"`
	Filename  string    `json:"filename,omitempty" required:"true"`
	LocalPath string    `json:"localPath,omitempty"`
	Size      int64     `json:"size,omitempty" required:"true"`
	Duration  int64     `json:"duration,omitempty" required:"true"`
	Source    string    `json:"source,omitempty"`
	Revision  int64     `json:"revision,omitempty"`
	SHA256    string    `json:"sha256,omitempty"`
	Attempts  int       `json:"attempts,omitempty"`
}

const (
	BackupStatusSuccess    = "success"
	BackupStatusStarted    = "started"
	BackupStatusFailed     = "failed"
	BackupStatusIncomplete = "incomplete"
)

func (r *Role) setStatus(ctx context.Context, status *BackupStatus) {
	// Reset all backup statuses to 0
	for _, st := range []string{BackupStatusSuccess, BackupStatusStarted, BackupStatusFailed, BackupStatusIncomplete} {
		backupStatus.WithLabelValues(st).Set(0)
	}
	backupStatus.WithLabelValues(status.Status).Set(1)

	if status.Status == BackupStatusSuccess || status.Status == BackupStatusIncomplete {
		backupSize.Set(float64(status.Size))
		backupDuration.Set(float64(status.Duration))
	}
	jstatus, err := json.Marshal(status)
	if err != nil {
		r.log.Warn("failed to marshal status", zap.Error(err))
		return
	}
	_, err = r.i.KV().Put(
		ctx,
		r.i.KV().Key(
			types.KeyRole,
			types.KeyStatus,
			extconfig.Get().Instance.Identifier,
		).String(),
		string(jstatus),
	)
	if err != nil {
		r.log.Warn("failed to save status", zap.Error(err))
	}
}

func fileExists(p string) bool {
	if p == "" {
		return false
	}
	_, err := os.Stat(p)
	return err == nil
}

func (r *Role) applyMeta(rec *jobRecord, meta *snapshotMeta) {
	rec.Size = meta.size
	rec.SHA256 = meta.sha256
	rec.Revision = meta.revision
	rec.SnapshotHash = meta.hash
	rec.TotalKeys = meta.totalKey
}

// ensureLocalSnapshot drives the local half of the pipeline to completion and
// is safe to call from any recorded phase. It verifies existing content
// first, promotes a valid staging file on resume, falls back to the confirmed
// remote object, and only takes a fresh snapshot for an attempt that never
// produced verified local content.
func (r *Role) ensureLocalSnapshot(ctx context.Context, rec *jobRecord) (*snapshotMeta, error) {
	if fileExists(rec.LocalPath) {
		meta, err := r.verifySnapshotFile(rec.LocalPath, rec.Size, rec.SHA256)
		if err == nil {
			r.pointLatestLocal(rec.LocalPath)
			return meta, nil
		}
		// A published immutable file is invalid. Refuse to overwrite it; a
		// confirmed remote copy can still restore this identity.
		r.log.Error("published snapshot failed verification", zap.String("id", rec.ID), zap.String("path", rec.LocalPath), zap.Error(err))
		if r.mc != nil && rec.Bucket != "" {
			return r.restoreLocalFromRemote(ctx, rec)
		}
		return nil, err
	}
	if fileExists(rec.TempPath) {
		meta, err := r.verifySnapshotFile(rec.TempPath, rec.Size, rec.SHA256)
		if err == nil {
			if err := r.publishLocal(rec); err != nil {
				return nil, err
			}
			r.log.Info("recovered unpublished staging snapshot", zap.String("id", rec.ID))
			return meta, nil
		}
		r.log.Warn("discarding invalid staging snapshot", zap.String("id", rec.ID), zap.Error(err))
		if err := os.Remove(rec.TempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			r.log.Warn("failed to remove invalid staging snapshot", zap.Error(err))
		}
	}
	// Only treat the remote copy as a recovery source when this identity
	// had already produced verified content. A fresh attempt must snapshot
	// locally first instead of depending on remote state.
	if r.mc != nil && rec.Bucket != "" && (rec.Phase == PhaseVerified || rec.Phase == PhaseUploaded || rec.Phase == PhaseManifested || rec.Phase == PhaseSucceeded) {
		if meta, err := r.restoreLocalFromRemote(ctx, rec); err == nil {
			return meta, nil
		} else {
			r.log.Warn("could not restore local snapshot from remote", zap.String("id", rec.ID), zap.Error(err))
		}
	}
	// Take a fresh snapshot only when this identity never reached verified
	// content. A verified identity that lost both copies is a hard failure
	// with an explicit target instead of silently taking new data under the
	// old name.
	if rec.Phase != PhaseVerified && rec.Phase != PhaseUploaded && rec.Phase != PhaseManifested && rec.Phase != PhaseSucceeded {
		if err := r.stageSnapshot(ctx, rec); err != nil {
			return nil, err
		}
		meta, err := r.verifySnapshotFile(rec.TempPath, 0, "")
		if err != nil {
			_ = os.Remove(rec.TempPath)
			return nil, err
		}
		if err := r.publishLocal(rec); err != nil {
			return nil, err
		}
		return meta, nil
	}
	return nil, fmt.Errorf("local snapshot for backup %s is missing and no staging or remote copy is available; retry target: %s", rec.ID, rec.LocalPath)
}

func (r *Role) restoreLocalFromRemote(ctx context.Context, rec *jobRecord) (*snapshotMeta, error) {
	ok, err := r.remoteManifestMatches(ctx, rec)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no verified remote object for backup %s at %s", rec.ID, rec.remoteTarget())
	}
	meta, err := r.downloadRemote(ctx, rec)
	if err != nil {
		return nil, err
	}
	if err := r.publishLocal(rec); err != nil {
		return nil, err
	}
	r.log.Info("restored local snapshot from remote", zap.String("id", rec.ID), zap.String("target", rec.remoteTarget()))
	return meta, nil
}

// ensureRemote drives the remote half idempotently: missing objects are
// uploaded, fully uploaded objects whose response was lost are reused, and
// the manifest sidecar is (re)written as the final success marker.
func (r *Role) ensureRemote(ctx context.Context, rec *jobRecord) error {
	if r.mc == nil || rec.Bucket == "" {
		return nil
	}
	if err := r.publishRemote(ctx, rec); err != nil {
		return err
	}
	ok, err := r.remoteManifestMatches(ctx, rec)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("manifest missing for %s after upload", rec.remoteTarget())
	}
	return nil
}

func (r *Role) failJob(ctx context.Context, rec *jobRecord, cause error) *BackupStatus {
	now := time.Now()
	rec.Phase = PhaseFailed
	rec.FinishedAt = &now
	rec.Error = cause.Error()
	if err := r.persistJob(ctx, rec); err != nil {
		r.log.Warn("failed to persist failed backup job", zap.String("id", rec.ID), zap.Error(err))
	}
	_ = os.Remove(rec.TempPath)
	status := r.statusFromRecord(rec)
	r.setStatus(ctx, status)
	return status
}

// partialJob records a resumable stage. The local snapshot is confirmed good,
// so the outcome is reported as incomplete with an explicit retry target
// rather than as a failed backup.
func (r *Role) partialJob(ctx context.Context, rec *jobRecord, cause error) *BackupStatus {
	rec.Error = fmt.Sprintf("retry target: %s: %s", rec.remoteTarget(), cause.Error())
	if err := r.persistJob(ctx, rec); err != nil {
		r.log.Warn("failed to persist incomplete backup job", zap.String("id", rec.ID), zap.Error(err))
	}
	r.log.Warn("backup incomplete, stage preserved for retry", zap.String("id", rec.ID), zap.String("stage", rec.Phase), zap.Error(cause))
	status := r.statusFromRecord(rec)
	status.Status = BackupStatusIncomplete
	status.Error = rec.Error
	r.setStatus(ctx, status)
	return status
}

func (r *Role) succeedJob(ctx context.Context, rec *jobRecord) *BackupStatus {
	now := time.Now()
	rec.Phase = PhaseSucceeded
	rec.FinishedAt = &now
	rec.Error = ""
	_ = os.Remove(rec.TempPath)
	if err := r.persistJob(ctx, rec); err != nil {
		// The content is confirmed everywhere; losing only this marker is
		// reconciled on the next start.
		r.log.Warn("failed to persist succeeded backup job, will recover on next start", zap.String("id", rec.ID), zap.Error(err))
	}
	status := r.statusFromRecord(rec)
	r.setStatus(ctx, status)
	return status
}

func (r *Role) statusFromRecord(rec *jobRecord) *BackupStatus {
	end := time.Now()
	if rec.FinishedAt != nil {
		end = *rec.FinishedAt
	}
	status := BackupStatusSuccess
	switch rec.Phase {
	case PhaseFailed:
		status = BackupStatusFailed
	case PhaseSucceeded:
		status = BackupStatusSuccess
	default:
		status = BackupStatusStarted
	}
	filename := rec.RemoteKey
	if filename == "" {
		filename = rec.FileName
	}
	return &BackupStatus{
		ID:        rec.ID,
		Time:      rec.StartedAt,
		Status:    status,
		Error:     rec.Error,
		Filename:  filename,
		LocalPath: rec.LocalPath,
		Size:      rec.Size,
		Duration:  int64(end.Sub(rec.StartedAt).Seconds()),
		Source:    rec.Source,
		Revision:  rec.Revision,
		SHA256:    rec.SHA256,
		Attempts:  rec.Attempts,
	}
}

// startBackupSpan opens tracing for a backup run. When the trigger context
// already carries a transaction (for example a request or test span), the
// run becomes a child span instead of another transaction: StartTransaction
// would otherwise reuse and mutate the caller's span and finish it here.
func startBackupSpan(ctx context.Context) (context.Context, func()) {
	if txn := sentry.TransactionFromContext(ctx); txn != nil {
		span := txn.StartChild("gravity.backup.snapshot")
		return span.Context(), span.Finish
	}
	tr := sentry.StartTransaction(ctx, "gravity.backup.snapshot")
	tr.Op = "gravity.backup.snapshot"
	return tr.Context(), tr.Finish
}

// driveBackup executes the full pipeline for a job identity and is idempotent
// across retries, duplicate requests and process restarts: every stage first
// reconciles observed local and remote state instead of assuming it failed.
func (r *Role) driveBackup(ctx context.Context, rec *jobRecord) *BackupStatus {
	spanCtx, finishSpan := startBackupSpan(ctx)
	defer finishSpan()
	ctx = spanCtx

	rec.Attempts++
	meta, err := r.ensureLocalSnapshot(ctx, rec)
	if err != nil {
		return r.failJob(ctx, rec, err)
	}
	r.applyMeta(rec, meta)
	if rec.Phase != PhaseSucceeded {
		rec.Phase = PhaseVerified
	}
	rec.Error = ""
	if err := r.persistJob(ctx, rec); err != nil {
		r.log.Warn("failed to persist backup job stage", zap.String("id", rec.ID), zap.Error(err))
	}

	if r.mc != nil && rec.Bucket != "" {
		if err := r.ensureRemote(ctx, rec); err != nil {
			return r.partialJob(ctx, rec, err)
		}
		rec.Phase = PhaseManifested
		if err := r.persistJob(ctx, rec); err != nil {
			r.log.Warn("failed to persist backup job stage", zap.String("id", rec.ID), zap.Error(err))
		}
	}
	return r.succeedJob(ctx, rec)
}

// SaveSnapshot keeps the historical single-argument entry point used by
// existing callers and tests. It runs a manually triggered backup with a
// fresh identity.
func (r *Role) SaveSnapshot(ctx context.Context) *BackupStatus {
	return r.StartBackup(ctx, SourceManual, "")
}
