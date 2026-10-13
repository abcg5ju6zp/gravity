package backup_test

import (
	"context"
	"os"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"beryju.io/gravity/pkg/extconfig"
	"beryju.io/gravity/pkg/instance"
	"beryju.io/gravity/pkg/roles"
	"beryju.io/gravity/pkg/roles/backup"
	"beryju.io/gravity/pkg/roles/backup/types"
	"beryju.io/gravity/pkg/tests"
	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	newTestAccessKey = "gravity"
	newTestSecretKey = "gravity-key"
)

func newTestRole(t *testing.T, configured bool, remotePath string) *backup.Role {
	t.Helper()
	rootInst := instance.New()
	ctx := tests.Context()
	inst := rootInst.ForRole("backup", ctx)
	role := backup.New(inst)
	if !configured {
		require.Equal(t, roles.ErrRoleNotConfigured, role.Start(ctx, []byte{}))
		return role
	}
	cfg := tests.MustJSON(&backup.RoleConfig{
		Endpoint:  "http://localhost:9001",
		AccessKey: newTestAccessKey,
		SecretKey: newTestSecretKey,
		Bucket:    "gravity",
		Path:      remotePath,
		CronExpr:  "",
	})
	require.NoError(t, role.Start(ctx, []byte(cfg)))
	t.Cleanup(role.Stop)
	return role
}

func countRemoteObjects(t *testing.T, role *backup.Role, prefix string) int {
	t.Helper()
	mc := role.TestMC()
	if mc == nil {
		return 0
	}
	count := 0
	for obj := range mc.ListObjects(context.Background(), "gravity", minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		require.NoError(t, obj.Err)
		if strings.HasSuffix(obj.Key, backup.ManifestSuffixExport) {
			continue
		}
		count++
	}
	return count
}

func latestJob(t *testing.T, role *backup.Role) *backup.JobRecord {
	t.Helper()
	jobs, err := role.TestListJobs(tests.Context())
	require.NoError(t, err)
	require.NotEmpty(t, jobs)
	latest := jobs[0]
	for idx := range jobs {
		if jobs[idx].StartedAt.After(latest.StartedAt) {
			latest = jobs[idx]
		}
	}
	return &latest
}

func TestLocalOnlySnapshotPublished(t *testing.T) {
	tests.Setup(t)
	role := newTestRole(t, false, "")

	status := role.SaveSnapshot(tests.Context())
	assert.Equal(t, backup.BackupStatusSuccess, status.Status, status.Error)
	assert.NotEmpty(t, status.ID)
	assert.Greater(t, status.Size, int64(0))
	assert.Len(t, status.SHA256, 64)
	assert.Greater(t, status.Revision, int64(0))
	assert.Equal(t, backup.SourceManual, status.Source)

	info, err := os.Stat(status.LocalPath)
	require.NoError(t, err)
	assert.Equal(t, status.Size, info.Size())

	link := path.Join(extconfig.Get().Dirs().BackupDir, backup.LocalLatestEntry)
	target, err := os.Readlink(link)
	require.NoError(t, err)
	resolved := path.Join(extconfig.Get().Dirs().BackupDir, target)
	assert.Equal(t, status.LocalPath, resolved)
	size, digest, revision, err := role.TestVerifySnapshotFile(link, 0, "")
	require.NoError(t, err)
	assert.Equal(t, status.SHA256, digest)
	assert.Equal(t, status.Size, size)
	assert.Equal(t, status.Revision, revision)

	entries, err := os.ReadDir(path.Join(extconfig.Get().Dirs().BackupDir, backup.DirTempExport))
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestSameDayBackupsDoNotOverwrite(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("same-day")
	role := newTestRole(t, true, remotePath)

	first := role.SaveSnapshot(tests.Context())
	second := role.SaveSnapshot(tests.Context())
	require.Equal(t, backup.BackupStatusSuccess, first.Status, first.Error)
	require.Equal(t, backup.BackupStatusSuccess, second.Status, second.Error)
	assert.NotEqual(t, first.ID, second.ID)
	assert.NotEqual(t, first.Filename, second.Filename)
	assert.NotEqual(t, first.LocalPath, second.LocalPath)

	entries, err := os.ReadDir(path.Join(extconfig.Get().Dirs().BackupDir, backup.DirSnapshotsExport))
	require.NoError(t, err)
	var localCount int
	for _, entry := range entries {
		if strings.Contains(entry.Name(), first.ID) || strings.Contains(entry.Name(), second.ID) {
			localCount++
		}
	}
	assert.Equal(t, 2, localCount)
	assert.Equal(t, 2, countRemoteObjects(t, role, remotePath+"/"))
}

func TestDuplicateRequestReusesIdentity(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("idempotent")
	role := newTestRole(t, true, remotePath)

	var first, second backup.BackupStatus
	require.NoError(t, role.APIBackupStart().Interact(tests.Context(), backup.APIBackupStartInput{
		Wait:      true,
		RequestID: "ops-change-1234",
	}, &first))
	require.NoError(t, role.APIBackupStart().Interact(tests.Context(), backup.APIBackupStartInput{
		Wait:      true,
		RequestID: "ops-change-1234",
	}, &second))
	assert.Equal(t, backup.BackupStatusSuccess, first.Status, first.Error)
	assert.Equal(t, first.ID, second.ID)
	assert.Equal(t, first.SHA256, second.SHA256)
	assert.Equal(t, 1, countRemoteObjects(t, role, remotePath+"/"))
}

func TestConcurrentBackupsAreIndependent(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("concurrent")
	role := newTestRole(t, true, remotePath)

	results := make([]*backup.BackupStatus, 2)
	var wg sync.WaitGroup
	for idx := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i] = role.SaveSnapshot(tests.Context())
		}(idx)
	}
	wg.Wait()
	for _, result := range results {
		require.Equal(t, backup.BackupStatusSuccess, result.Status, result.Error)
	}
	assert.NotEqual(t, results[0].ID, results[1].ID)
	assert.Equal(t, 2, countRemoteObjects(t, role, remotePath+"/"))
}

func TestCorruptStagingIsDiscarded(t *testing.T) {
	tests.Setup(t)
	role := newTestRole(t, false, "")

	rec := role.TestNewRecord("20200102T000000.000000000Z-aaaaaaaaaaaa", backup.SourceManual)
	require.NoError(t, os.MkdirAll(path.Dir(rec.TempPath), os.ModePerm))
	require.NoError(t, os.WriteFile(rec.TempPath, []byte("definitely not an etcd snapshot"), 0o600))
	require.NoError(t, role.TestPersistJob(tests.Context(), rec))

	status := role.StartBackup(tests.Context(), backup.SourceManual, rec.ID)
	assert.Equal(t, backup.BackupStatusSuccess, status.Status, status.Error)
	_, err := os.Stat(rec.TempPath)
	assert.True(t, os.IsNotExist(err))
}

func TestJobRecordPinsIdentity(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("identity")
	role := newTestRole(t, true, remotePath)

	status := role.StartBackup(tests.Context(), backup.SourceCron, "")
	assert.Equal(t, backup.BackupStatusSuccess, status.Status, status.Error)
	assert.Equal(t, backup.SourceCron, status.Source)
	assert.True(t, strings.HasPrefix(status.Filename, remotePath+"/gravity/"))
	assert.True(t, strings.HasSuffix(status.Filename, status.ID+".etcd"))
}

func TestFinishedIdentityIsReturnedAsIs(t *testing.T) {
	tests.Setup(t)
	role := newTestRole(t, false, "")
	rec := role.TestNewRecord("20200103T000000.000000000Z-bbbbbbbbbbbb", backup.SourceEvent)
	finished := time.Now()
	rec.Phase = backup.PhaseSucceeded
	rec.FinishedAt = &finished
	require.NoError(t, role.TestPersistJob(tests.Context(), rec))

	_, terminalID, terminal := role.TestBeginBackup(tests.Context(), backup.SourceEvent, rec.ID)
	require.NotNil(t, terminal)
	assert.Equal(t, backup.BackupStatusSuccess, terminal.Status)
	assert.Equal(t, rec.ID, terminalID)
}

func TestStatusKeyWritten(t *testing.T) {
	tests.Setup(t)
	role := newTestRole(t, false, "")
	status := role.SaveSnapshot(tests.Context())

	res, err := role.TestKV().Get(tests.Context(), role.TestKV().Key(
		types.KeyRole,
		types.KeyStatus,
		extconfig.Get().Instance.Identifier,
	).String())
	require.NoError(t, err)
	require.Len(t, res.Kvs, 1)
	assert.Contains(t, string(res.Kvs[0].Value), status.ID)
}

func removeRemote(t *testing.T, role *backup.Role, rec *backup.JobRecord, includeObject bool) {
	t.Helper()
	mc := role.TestMC()
	if includeObject {
		require.NoError(t, mc.RemoveObject(tests.Context(), rec.Bucket, rec.RemoteKey, minio.RemoveObjectOptions{}))
	}
	require.NoError(t, mc.RemoveObject(tests.Context(), rec.Bucket, rec.RemoteKey+backup.ManifestSuffixExport, minio.RemoveObjectOptions{}))
}

func resetRecordToVerified(rec *backup.JobRecord) {
	rec.Phase = backup.PhaseVerified
	rec.FinishedAt = nil
	rec.Error = ""
}

func TestRecoverInterruptedUpload(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("recover-upload")
	role := newTestRole(t, true, remotePath)

	status := role.SaveSnapshot(tests.Context())
	require.Equal(t, backup.BackupStatusSuccess, status.Status, status.Error)
	rec := latestJob(t, role)
	removeRemote(t, role, rec, true)
	resetRecordToVerified(rec)
	require.NoError(t, role.TestPersistJob(tests.Context(), rec))

	restarted := newTestRole(t, true, remotePath)
	recovered := latestJob(t, restarted)
	assert.Equal(t, rec.ID, recovered.ID)
	assert.Equal(t, backup.PhaseSucceeded, recovered.Phase)
	assert.Equal(t, rec.RemoteKey, recovered.RemoteKey)
	assert.Equal(t, rec.SHA256, recovered.SHA256)
	assert.Equal(t, 1, countRemoteObjects(t, restarted, remotePath+"/"))

	ok, err := restarted.TestRemoteManifestMatches(tests.Context(), recovered)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestRecoverLostManifestWrite(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("recover-manifest")
	role := newTestRole(t, true, remotePath)

	status := role.SaveSnapshot(tests.Context())
	require.Equal(t, backup.BackupStatusSuccess, status.Status, status.Error)
	rec := latestJob(t, role)
	removeRemote(t, role, rec, false)
	resetRecordToVerified(rec)
	require.NoError(t, role.TestPersistJob(tests.Context(), rec))

	restarted := newTestRole(t, true, remotePath)
	recovered := latestJob(t, restarted)
	assert.Equal(t, backup.PhaseSucceeded, recovered.Phase)
	assert.Equal(t, rec.ID, recovered.ID)
	ok, err := restarted.TestRemoteManifestMatches(tests.Context(), recovered)
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestRecoverLocalFileFromRemote(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("recover-local")
	role := newTestRole(t, true, remotePath)

	status := role.SaveSnapshot(tests.Context())
	require.Equal(t, backup.BackupStatusSuccess, status.Status, status.Error)
	rec := latestJob(t, role)
	require.NoError(t, os.Remove(rec.LocalPath))

	restarted := newTestRole(t, true, remotePath)
	recovered := latestJob(t, restarted)
	assert.Equal(t, backup.PhaseSucceeded, recovered.Phase)
	size, digest, revision, err := restarted.TestVerifySnapshotFile(recovered.LocalPath, recovered.Size, recovered.SHA256)
	require.NoError(t, err)
	assert.Equal(t, rec.Size, size)
	assert.Equal(t, rec.SHA256, digest)
	assert.Equal(t, rec.Revision, revision)

	link := path.Join(extconfig.Get().Dirs().BackupDir, backup.LocalLatestEntry)
	target, err := os.Readlink(link)
	require.NoError(t, err)
	assert.Contains(t, target, rec.ID)
}

func TestRecoverKeepsExplicitRetryTarget(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("recover-pending")
	role := newTestRole(t, true, remotePath)

	status := role.SaveSnapshot(tests.Context())
	require.Equal(t, backup.BackupStatusSuccess, status.Status, status.Error)
	rec := latestJob(t, role)
	resetRecordToVerified(rec)
	rec.Bucket = "bucket-that-does-not-exist-9d4f"
	require.NoError(t, role.TestPersistJob(tests.Context(), rec))

	restarted := newTestRole(t, true, remotePath)
	recovered := latestJob(t, restarted)
	assert.Equal(t, backup.PhaseVerified, recovered.Phase)
	assert.Contains(t, recovered.Error, "s3://bucket-that-does-not-exist-9d4f")
	assert.NotEmpty(t, recovered.LocalPath)
}

func TestCleanupOrphanStaging(t *testing.T) {
	tests.Setup(t)
	backupDir := extconfig.Get().Dirs().BackupDir
	orphan := path.Join(backupDir, backup.DirTempExport, "no-record.partial")
	require.NoError(t, os.MkdirAll(path.Dir(orphan), os.ModePerm))
	require.NoError(t, os.WriteFile(orphan, []byte("partial"), 0o600))
	legacy := path.Join(backupDir, "temp-gravity-snapshot.123.etcd")
	require.NoError(t, os.WriteFile(legacy, []byte("legacy"), 0o600))

	_ = newTestRole(t, false, "")

	_, err := os.Stat(orphan)
	assert.True(t, os.IsNotExist(err))
	_, err = os.Stat(legacy)
	assert.True(t, os.IsNotExist(err))
}

func TestTrackedStagingIsReconciled(t *testing.T) {
	tests.Setup(t)
	role := newTestRole(t, false, "")
	rec := role.TestNewRecord("20200104T000000.000000000Z-cccccccccccc", backup.SourceCron)
	require.NoError(t, role.TestPersistJob(tests.Context(), rec))
	require.NoError(t, os.MkdirAll(path.Dir(rec.TempPath), os.ModePerm))
	require.NoError(t, os.WriteFile(rec.TempPath, []byte("partial-but-tracked"), 0o600))

	role.TestRecoverJobs(context.Background())

	jobs, err := role.TestListJobs(tests.Context())
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, backup.PhaseSucceeded, jobs[0].Phase)
	assert.Equal(t, rec.ID, jobs[0].ID)
	size, _, _, err := role.TestVerifySnapshotFile(jobs[0].LocalPath, jobs[0].Size, jobs[0].SHA256)
	require.NoError(t, err)
	assert.Greater(t, size, int64(0))
}

func TestStatusReflectsRecoveredBackup(t *testing.T) {
	tests.Setup(t)
	remotePath := tests.RandomString("recover-status")
	role := newTestRole(t, true, remotePath)
	status := role.SaveSnapshot(tests.Context())
	require.Equal(t, backup.BackupStatusSuccess, status.Status, status.Error)
	rec := latestJob(t, role)
	removeRemote(t, role, rec, true)
	resetRecordToVerified(rec)
	require.NoError(t, role.TestPersistJob(tests.Context(), rec))

	restarted := newTestRole(t, true, remotePath)

	res, err := restarted.TestKV().Get(tests.Context(), restarted.TestKV().Key(
		types.KeyRole,
		types.KeyStatus,
		extconfig.Get().Instance.Identifier,
	).String())
	require.NoError(t, err)
	require.Len(t, res.Kvs, 1)
	assert.Contains(t, string(res.Kvs[0].Value), `"status":"success"`)
	assert.Contains(t, string(res.Kvs[0].Value), rec.ID)
}
