package backup

import (
	"context"

	"beryju.io/gravity/pkg/storage"
	"github.com/minio/minio-go/v7"
)

// Test-only accessors for the external backup_test package.

type JobRecord = jobRecord

const (
	ManifestSuffixExport = manifestSuffix
	DirTempExport        = dirTemp
	DirSnapshotsExport   = dirSnapshots
)

func (r *Role) TestMC() *minio.Client { return r.mc }

func (r *Role) TestKV() *storage.Client { return r.i.KV() }

func (r *Role) TestListJobs(ctx context.Context) ([]JobRecord, error) {
	return r.listJobs(ctx)
}

func (r *Role) TestNewRecord(id, source string) *JobRecord {
	return r.newRecord(id, source)
}

func (r *Role) TestPersistJob(ctx context.Context, rec *JobRecord) error {
	return r.persistJob(ctx, rec)
}

func (r *Role) TestRecoverJobs(ctx context.Context) {
	r.recoverJobs(ctx)
}

func (r *Role) TestVerifySnapshotFile(filePath string, expectedSize int64, expectedSHA string) (int64, string, int64, error) {
	meta, err := r.verifySnapshotFile(filePath, expectedSize, expectedSHA)
	if err != nil {
		return 0, "", 0, err
	}
	return meta.size, meta.sha256, meta.revision, nil
}

func (r *Role) TestRemoteManifestMatches(ctx context.Context, rec *JobRecord) (bool, error) {
	return r.remoteManifestMatches(ctx, rec)
}

func (r *Role) TestBeginBackup(ctx context.Context, source, requestID string) (*JobRecord, string, *BackupStatus) {
	rec, _, terminal := r.beginBackup(ctx, source, requestID)
	terminalID := ""
	if terminal != nil {
		terminalID = terminal.ID
	}
	return rec, terminalID, terminal
}
