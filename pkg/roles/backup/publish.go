package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"

	"beryju.io/gravity/pkg/extconfig"
	"github.com/minio/minio-go/v7"
	"go.uber.org/zap"
)

// snapshotMeta holds the verified content metadata of a snapshot file.
type snapshotMeta struct {
	// size is the physical file size (database bytes plus an optional
	// checksum trailer), used for upload and remote size comparison.
	size     int64
	sha256   string
	revision int64
	hash     uint32
	totalKey int
}

var errSnapshotMismatch = errors.New("snapshot verification failed")

// hasChecksum reports whether an etcd snapshot file carries the 32-byte
// sha256 trailer that servers append since v3.6. It mirrors the official
// client's check: the file size modulo the minimum sector size is 32.
func hasChecksum(n int64) bool {
	return (n % 512) == sha256.Size
}

// verifyStreamChecksum confirms the trailing checksum of a server-provided
// snapshot matches the sha256 of the preceding database bytes, and returns
// that digest.
func verifyStreamChecksum(filePath string, physicalSize int64) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	trailer := make([]byte, sha256.Size)
	if _, err := file.ReadAt(trailer, physicalSize-sha256.Size); err != nil {
		return "", err
	}
	sum := sha256.New()
	if _, err := io.CopyN(sum, file, physicalSize-sha256.Size); err != nil {
		return "", err
	}
	digest := hex.EncodeToString(sum.Sum(nil))
	if digest != hex.EncodeToString(trailer) {
		return "", fmt.Errorf("%w: stream checksum mismatch: computed %s, trailer says %s",
			errSnapshotMismatch, digest, hex.EncodeToString(trailer))
	}
	return digest, nil
}

// stageSnapshot streams an etcd snapshot into an isolated staging file. The
// file is fsynced before it is returned, and it is removed on any error so an
// interrupted stream never leaves partial published content behind.
func (r *Role) stageSnapshot(ctx context.Context, rec *jobRecord) error {
	reader, err := r.i.KV().Snapshot(ctx)
	if err != nil {
		return fmt.Errorf("creating etcd snapshot: %w", err)
	}
	defer reader.Close()
	if err := os.MkdirAll(path.Dir(rec.TempPath), os.ModePerm); err != nil {
		return fmt.Errorf("creating staging directory: %w", err)
	}
	file, err := os.OpenFile(rec.TempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating staging file: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(rec.TempPath)
		}
	}()
	if _, err := io.Copy(file, reader); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing snapshot: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("syncing snapshot: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing snapshot: %w", err)
	}
	cleanup = false
	return nil
}

// verifySnapshotFile confirms that a snapshot file is complete and valid:
// the file exists with the expected size, etcd can open and verify its
// contents (hash and revision) and its sha256 digest matches when one was
// previously recorded. Nothing is trusted from file names or phases.
func (r *Role) verifySnapshotFile(filePath string, expectedSize int64, expectedSHA string) (*snapshotMeta, error) {
	stat, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("stating snapshot: %w", err)
	}
	if stat.IsDir() {
		return nil, fmt.Errorf("%w: %s is a directory", errSnapshotMismatch, filePath)
	}
	if expectedSize > 0 && stat.Size() != expectedSize {
		return nil, fmt.Errorf("%w: size %d does not match recorded %d", errSnapshotMismatch, stat.Size(), expectedSize)
	}
	status, err := r.snap.Status(filePath)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errSnapshotMismatch, err)
	}
	physicalSize := stat.Size()
	dbSize := physicalSize
	var sum string
	if hasChecksum(physicalSize) {
		// Current etcd servers append a 32-byte sha256 trailer after the
		// database bytes. Verify the trailer against the content and record
		// the content digest; etcd's reported size covers the database only.
		sum, err = verifyStreamChecksum(filePath, physicalSize)
		if err != nil {
			return nil, err
		}
		dbSize = physicalSize - sha256.Size
	} else {
		sum, err = hashFileSHA256(filePath)
		if err != nil {
			return nil, fmt.Errorf("hashing snapshot: %w", err)
		}
	}
	if status.TotalSize != dbSize {
		return nil, fmt.Errorf("%w: etcd reports %d database bytes but file holds %d", errSnapshotMismatch, status.TotalSize, dbSize)
	}
	if expectedSHA != "" && expectedSHA != sum {
		return nil, fmt.Errorf("%w: digest %s does not match recorded %s", errSnapshotMismatch, sum, expectedSHA)
	}
	return &snapshotMeta{
		size:     physicalSize,
		sha256:   sum,
		revision: status.Revision,
		hash:     status.Hash,
		totalKey: status.TotalKey,
	}, nil
}

func hashFileSHA256(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	sum := sha256.New()
	if _, err := io.Copy(sum, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(sum.Sum(nil)), nil
}

// publishLocal promotes a verified staging file to its immutable local name
// and points the well-known recoverable entry at it. Both operations use
// same-filesystem atomic renames, so an observer either sees the previous
// valid snapshot or the new one, never a half-written file.
func (r *Role) publishLocal(rec *jobRecord) error {
	if err := os.MkdirAll(path.Dir(rec.LocalPath), os.ModePerm); err != nil {
		return fmt.Errorf("creating snapshot directory: %w", err)
	}
	if _, err := os.Stat(rec.LocalPath); err == nil {
		// Immutable name already published (duplicate trigger or resume);
		// never overwrite it.
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("checking local snapshot: %w", err)
	}
	if err := os.Rename(rec.TempPath, rec.LocalPath); err != nil {
		return fmt.Errorf("publishing local snapshot: %w", err)
	}
	syncDir(path.Dir(rec.LocalPath))
	r.pointLatestLocal(rec.LocalPath)
	return nil
}

// pointLatestLocal atomically replaces the well-known recoverable entry with
// a symlink to the given snapshot. Failures are non-fatal: the immutable
// snapshot file remains usable even if the convenience entry cannot be
// updated.
func (r *Role) pointLatestLocal(localPath string) {
	backupDir := extconfig.Get().Dirs().BackupDir
	linkPath := path.Join(backupDir, LocalLatestEntry)
	target, err := filepath.Rel(backupDir, localPath)
	if err != nil {
		r.log.Warn("failed to compute relative snapshot path", zap.Error(err))
		return
	}
	tmpLink := linkPath + ".tmp"
	_ = os.Remove(tmpLink)
	if err := os.Symlink(target, tmpLink); err != nil {
		r.log.Warn("failed to create snapshot link", zap.Error(err))
		return
	}
	if err := os.Rename(tmpLink, linkPath); err != nil {
		_ = os.Remove(tmpLink)
		r.log.Warn("failed to publish snapshot link", zap.Error(err))
		return
	}
	syncDir(backupDir)
}

// syncDir best-effort fsyncs a directory so renames survive a crash.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	_ = d.Sync()
	_ = d.Close()
}

// remoteObjectMatches reports whether the immutable remote object exists and
// matches the local snapshot size. A different size under the same immutable
// name is a hard error: retries must never overwrite another attempt's
// content.
func (r *Role) remoteObjectMatches(ctx context.Context, rec *jobRecord) (exists bool, err error) {
	if r.mc == nil || rec.Bucket == "" {
		return false, nil
	}
	info, statErr := r.mc.StatObject(ctx, rec.Bucket, rec.RemoteKey, minio.StatObjectOptions{})
	if statErr != nil {
		if minio.ToErrorResponse(statErr).Code == "NoSuchKey" {
			return false, nil
		}
		return false, statErr
	}
	if rec.Size > 0 && info.Size != rec.Size {
		return true, fmt.Errorf("%w: remote object %s has size %d, recorded size %d",
			errSnapshotMismatch, rec.remoteTarget(), info.Size, rec.Size)
	}
	return true, nil
}

func (r *Role) buildManifest(rec *jobRecord) ([]byte, error) {
	m := remoteManifest{
		ID:        rec.ID,
		Instance:  rec.Instance,
		Version:   rec.Version,
		Source:    rec.Source,
		ObjectKey: rec.RemoteKey,
		Size:      rec.Size,
		SHA256:    rec.SHA256,
		Revision:  rec.Revision,
		Hash:      rec.SnapshotHash,
		TotalKeys: rec.TotalKeys,
		CreatedAt: rec.StartedAt,
	}
	return json.MarshalIndent(m, "", "  ")
}

// publishRemote uploads the verified local snapshot under its immutable
// remote object name and only then writes the verification manifest sidecar.
// Existing objects with matching size are reused so retries and lost
// responses do not re-upload or overwrite anything.
func (r *Role) publishRemote(ctx context.Context, rec *jobRecord) error {
	if r.mc == nil || rec.Bucket == "" {
		return nil
	}
	exists, err := r.remoteObjectMatches(ctx, rec)
	if err != nil {
		return err
	}
	if !exists {
		file, openErr := os.Open(rec.LocalPath)
		if openErr != nil {
			return fmt.Errorf("opening local snapshot for upload: %w", openErr)
		}
		info, putErr := r.mc.PutObject(ctx, rec.Bucket, rec.RemoteKey, file, rec.Size, minio.PutObjectOptions{})
		_ = file.Close()
		if putErr != nil {
			return fmt.Errorf("uploading snapshot to %s: %w", rec.remoteTarget(), putErr)
		}
		if rec.Size > 0 && info.Size != rec.Size {
			return fmt.Errorf("uploaded object size %d does not match local size %d", info.Size, rec.Size)
		}
		r.log.Info("uploaded snapshot", zap.String("target", rec.remoteTarget()), zap.Int64("size", rec.Size))
	}
	// Confirm the object after upload / on resume before writing the
	// manifest, which is the remote success marker.
	exists, err = r.remoteObjectMatches(ctx, rec)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("snapshot object %s disappeared after upload", rec.remoteTarget())
	}
	raw, err := r.buildManifest(rec)
	if err != nil {
		return err
	}
	_, err = r.mc.PutObject(ctx, rec.Bucket, rec.RemoteKey+manifestSuffix, bytes.NewReader(raw), int64(len(raw)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		return fmt.Errorf("writing manifest for %s: %w", rec.remoteTarget(), err)
	}
	return nil
}

// remoteManifestExists verifies the manifest sidecar is present and still
// matches the recorded content identity.
func (r *Role) remoteManifestMatches(ctx context.Context, rec *jobRecord) (bool, error) {
	if r.mc == nil || rec.Bucket == "" {
		return true, nil
	}
	obj, err := r.mc.GetObject(ctx, rec.Bucket, rec.RemoteKey+manifestSuffix, minio.GetObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).Code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	defer obj.Close()
	var m remoteManifest
	if err := json.NewDecoder(obj).Decode(&m); err != nil {
		return false, nil
	}
	if m.ID != rec.ID || (rec.SHA256 != "" && m.SHA256 != rec.SHA256) || (rec.Size > 0 && m.Size != rec.Size) {
		return false, fmt.Errorf("%w: manifest at %s does not match local content", errSnapshotMismatch, rec.remoteTarget())
	}
	return true, nil
}

// downloadRemote stages the remote object locally and verifies it against
// the remote manifest. Used during recovery when the local published file is
// gone but the remote copy is confirmed.
func (r *Role) downloadRemote(ctx context.Context, rec *jobRecord) (*snapshotMeta, error) {
	obj, err := r.mc.GetObject(ctx, rec.Bucket, rec.RemoteKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	if err := os.MkdirAll(path.Dir(rec.TempPath), os.ModePerm); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(rec.TempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(file, obj); err != nil {
		_ = file.Close()
		_ = os.Remove(rec.TempPath)
		return nil, err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(rec.TempPath)
		return nil, err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(rec.TempPath)
		return nil, err
	}
	return r.verifySnapshotFile(rec.TempPath, rec.Size, rec.SHA256)
}
