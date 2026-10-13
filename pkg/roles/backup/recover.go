package backup

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"sort"
	"time"

	"beryju.io/gravity/pkg/extconfig"
	"go.uber.org/zap"
)

// recoveryJobLimit bounds how many terminal job records are kept; older
// terminal records are pruned on start so the journal cannot grow forever.
const recoveryJobLimit = 20

// recoveryTimeout bounds how long startup reconciliation may take. Each
// individual upload is resumable, so a start that runs out of time leaves
// records in their current stage for the next start or trigger.
var recoveryTimeout = 30 * time.Minute

type recoveryReport struct {
	recovered  []string
	incomplete []string
	failed     []string
	pruned     int
}

// recoverJobs reconciles durable job records with the actual local files and
// remote objects on startup. Confirmable results are recovered and reported
// as successful; targets that still need a retry are kept in their resumable
// stage and logged explicitly. Unpublished temporary content with no record
// is removed.
func (r *Role) recoverJobs(parent context.Context) {
	ctx, cancel := context.WithTimeout(parent, recoveryTimeout)
	defer cancel()

	jobs, err := r.listJobs(ctx)
	if err != nil {
		r.log.Warn("failed to list backup jobs for recovery", zap.Error(err))
		return
	}
	knownTemp := make(map[string]struct{})
	for _, j := range jobs {
		if j.TempPath != "" {
			knownTemp[filepath.Base(j.TempPath)] = struct{}{}
		}
	}

	report := recoveryReport{}
	for idx := range jobs {
		rec := jobs[idx]
		if ctx.Err() != nil {
			break
		}
		if rec.Phase == PhaseFailed {
			// A failed snapshot is retried by a new trigger, never
			// silently re-snapshotted under the old identity.
			continue
		}
		if rec.Phase == PhaseSucceeded && !r.succeededNeedsRepair(ctx, &rec) {
			continue
		}
		status := r.resumeJob(ctx, &rec)
		switch status.Status {
		case BackupStatusSuccess:
			report.recovered = append(report.recovered, rec.ID)
		case BackupStatusIncomplete:
			report.incomplete = append(report.incomplete, rec.ID)
		case BackupStatusFailed:
			report.failed = append(report.failed, rec.ID)
		}
	}

	report.pruned = r.pruneJobs(ctx, jobs)
	r.cleanupStaging(knownTemp)
	r.repairLatestEntry()
	r.reflectLatestStatus(ctx, jobs)

	fields := []zap.Field{
		zap.Strings("recovered", report.recovered),
		zap.Strings("incomplete", report.incomplete),
		zap.Strings("failed", report.failed),
		zap.Int("pruned", report.pruned),
	}
	if len(report.incomplete) > 0 || len(report.failed) > 0 {
		r.log.Warn("backup recovery finished with pending targets", fields...)
	} else {
		r.log.Info("backup recovery finished", fields...)
	}
}

// succeededNeedsRepair reports whether a terminal-success record's actual
// local file or remote object no longer matches the recorded content.
// Transient object storage errors never downgrade a confirmed success: only a
// positively confirmed missing object or manifest triggers repair.
func (r *Role) succeededNeedsRepair(ctx context.Context, rec *jobRecord) bool {
	if !fileExists(rec.LocalPath) {
		return true
	}
	if _, err := r.verifySnapshotFile(rec.LocalPath, rec.Size, rec.SHA256); err != nil {
		r.log.Warn("successful snapshot failed local verification", zap.String("id", rec.ID), zap.Error(err))
		return true
	}
	if r.mc == nil || rec.Bucket == "" {
		return false
	}
	exists, err := r.remoteObjectMatches(ctx, rec)
	if err != nil {
		r.log.Warn("cannot confirm remote snapshot during recovery, keeping recorded success", zap.String("id", rec.ID), zap.Error(err))
		return false
	}
	if !exists {
		return true
	}
	manifestOK, err := r.remoteManifestMatches(ctx, rec)
	if err != nil {
		r.log.Warn("cannot confirm remote manifest during recovery, keeping recorded success", zap.String("id", rec.ID), zap.Error(err))
		return false
	}
	return !manifestOK
}

// resumeJob drives a recorded identity during startup reconciliation.
// Unlike beginBackup it never short-circuits terminal records: a previously
// successful backup whose local file or remote object disappeared must be
// repaired or explicitly reported.
func (r *Role) resumeJob(ctx context.Context, rec *jobRecord) *BackupStatus {
	r.jobsM.Lock()
	if current, ok := r.jobs[rec.ID]; ok {
		r.jobsM.Unlock()
		select {
		case <-current.done:
			if current.status != nil {
				return current.status
			}
		case <-ctx.Done():
			return r.statusFromRecord(rec)
		}
	} else {
		run := &activeJob{
			id:    rec.ID,
			rec:   rec,
			owner: true,
			done:  make(chan struct{}),
		}
		r.jobs[rec.ID] = run
		r.jobsM.Unlock()
		status := r.driveBackup(ctx, rec)
		r.complete(run, status)
		return status
	}
	// An in-process run vanished without a status; reconcile directly.
	return r.driveBackup(ctx, rec)
}

// pruneJobs removes terminal records beyond the newest recoveryJobLimit. All
// non-terminal records are kept until they reach a final state.
func (r *Role) pruneJobs(ctx context.Context, jobs []jobRecord) int {
	terminal := make([]jobRecord, 0, len(jobs))
	for _, j := range jobs {
		if j.Phase == PhaseSucceeded || j.Phase == PhaseFailed {
			terminal = append(terminal, j)
		}
	}
	if len(terminal) <= recoveryJobLimit {
		return 0
	}
	sort.Slice(terminal, func(i, j int) bool {
		return terminal[i].StartedAt.After(terminal[j].StartedAt)
	})
	pruned := 0
	for _, j := range terminal[recoveryJobLimit:] {
		if _, err := r.i.KV().Delete(ctx, r.jobKey(j.ID)); err != nil {
			r.log.Warn("failed to prune backup job", zap.String("id", j.ID), zap.Error(err))
			continue
		}
		pruned++
	}
	return pruned
}

// cleanupStaging removes unpublished temporary content that no job record
// references, including staging files left by older gravity versions.
func (r *Role) cleanupStaging(knownTemp map[string]struct{}) {
	backupDir := extconfig.Get().Dirs().BackupDir
	tempDir := path.Join(backupDir, dirTemp)
	entries, err := os.ReadDir(tempDir)
	if err != nil {
		if !os.IsNotExist(err) {
			r.log.Warn("failed to read staging directory", zap.Error(err))
		}
	} else {
		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			if _, ok := knownTemp[entry.Name()]; ok {
				continue
			}
			if err := os.Remove(path.Join(tempDir, entry.Name())); err != nil {
				r.log.Warn("failed to remove orphan staging file", zap.String("file", entry.Name()), zap.Error(err))
			} else {
				r.log.Info("removed orphan staging file", zap.String("file", entry.Name()))
			}
		}
	}
	// Legacy temp files created directly in the backup directory.
	legacies, err := filepath.Glob(path.Join(backupDir, legacyTempPattern))
	if err != nil {
		return
	}
	for _, legacy := range legacies {
		if err := os.Remove(legacy); err != nil {
			r.log.Warn("failed to remove legacy staging file", zap.String("file", legacy), zap.Error(err))
		} else {
			r.log.Info("removed legacy staging file", zap.String("file", legacy))
		}
	}
}

// repairLatestEntry makes sure the well-known recoverable entry exists and
// points at the newest verified immutable snapshot, even when no job record
// survived.
func (r *Role) repairLatestEntry() {
	backupDir := extconfig.Get().Dirs().BackupDir
	linkPath := path.Join(backupDir, LocalLatestEntry)
	if info, err := os.Lstat(linkPath); err == nil && info.Mode()&os.ModeSymlink == 0 {
		// Legacy regular file: leave the existing restore entry untouched.
		return
	}
	if target, err := os.Readlink(linkPath); err == nil {
		if fileExists(path.Join(backupDir, target)) {
			return
		}
	}
	entries, err := os.ReadDir(path.Join(backupDir, dirSnapshots))
	if err != nil {
		return
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(names)))
	for _, name := range names {
		full := path.Join(backupDir, dirSnapshots, name)
		if _, err := r.verifySnapshotFile(full, 0, ""); err != nil {
			r.log.Warn("ignoring invalid snapshot while repairing latest entry", zap.String("file", name), zap.Error(err))
			continue
		}
		r.pointLatestLocal(full)
		return
	}
}

// reflectLatestStatus makes the metrics and legacy status key describe the
// most recent known job after reconciliation.
func (r *Role) reflectLatestStatus(ctx context.Context, _ []jobRecord) {
	jobs, err := r.listJobs(ctx)
	if err != nil || len(jobs) == 0 {
		return
	}
	sort.Slice(jobs, func(i, j int) bool {
		return jobs[i].StartedAt.After(jobs[j].StartedAt)
	})
	latest := jobs[0]
	if latest.Phase == PhaseFailed {
		return
	}
	status := r.statusFromRecord(&latest)
	if latest.Phase != PhaseSucceeded {
		status.Status = BackupStatusIncomplete
	}
	r.setStatus(ctx, status)
}
