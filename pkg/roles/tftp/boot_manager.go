package tftp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"sort"
	"sync"
	"time"

	"beryju.io/gravity/pkg/storage"
	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const bootSessionTTL = 10 * time.Minute

// bootManager owns the boot bundle state machine: immutable candidate versions,
// single-pointer atomic publish, TFTP client version pins, recovery and GC.
type bootManager struct {
	kv  *storage.Client
	log *zap.Logger

	// now is indirected so tests can drive the session TTL deterministically.
	now func() time.Time

	// sessions pins a TFTP client IP to a version per bundle.
	// Map layout: clientIP -> bundle name -> pin. The critical section only
	// touches this in-memory state, never etcd IO.
	sessionsMu sync.Mutex
	sessions   map[string]map[string]bootPin

	// testBeforeCommit runs after all publish reads and immediately before the
	// CAS transaction commits. Test-only hook for deterministic arbitration.
	testBeforeCommit func()
}

type bootPin struct {
	versionID  string
	lastAccess time.Time
}

func newBootManager(kv *storage.Client, log *zap.Logger) *bootManager {
	return &bootManager{
		kv:       kv,
		log:      log,
		now:      time.Now,
		sessions: map[string]map[string]bootPin{},
	}
}

// --- low level meta helpers ---

func (m *bootManager) loadBundle(ctx context.Context, name string) (*BootBundle, int64, error) {
	res, err := m.kv.Get(ctx, bootBundleMetaKey(m.kv, name).String())
	if err != nil {
		return nil, 0, status.Wrap(err, status.Internal)
	}
	if len(res.Kvs) < 1 {
		return nil, 0, status.WithDescription(status.NotFound, "boot bundle not found")
	}
	b := &BootBundle{}
	if err := m.kv.Unmarshal(res.Kvs[0].Value, b); err != nil {
		return nil, 0, status.Wrap(err, status.Internal)
	}
	return b, res.Kvs[0].ModRevision, nil
}

func (m *bootManager) loadVersion(ctx context.Context, bundle, id string) (*BootVersion, int64, error) {
	res, err := m.kv.Get(ctx, bootVersionMetaKey(m.kv, bundle, id).String())
	if err != nil {
		return nil, 0, status.Wrap(err, status.Internal)
	}
	if len(res.Kvs) < 1 {
		return nil, 0, status.WithDescription(status.NotFound, "boot version not found")
	}
	v := &BootVersion{}
	if err := m.kv.Unmarshal(res.Kvs[0].Value, v); err != nil {
		return nil, 0, status.Wrap(err, status.Internal)
	}
	if v.Received == nil {
		v.Received = map[string]BootReceivedFile{}
	}
	return v, res.Kvs[0].ModRevision, nil
}

// --- bundles ---

// createBundle is idempotent: it returns the existing bundle unchanged when
// the name is already taken.
func (m *bootManager) createBundle(ctx context.Context, name string) (*BootBundle, error) {
	if err := validateBootName(name); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	key := bootBundleMetaKey(m.kv, name)
	now := m.now().UTC()
	b := &BootBundle{
		Name:      name,
		CreatedAt: now,
		UpdatedAt: now,
	}
	raw, err := m.kv.Marshal(b)
	if err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	res, err := m.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key.String()), "=", 0)).
		Then(clientv3.OpPut(key.String(), string(raw))).
		Else(clientv3.OpGet(key.String())).
		Commit()
	if err != nil {
		return nil, status.Wrap(err, status.Internal)
	}
	if !res.Succeeded {
		existing := &BootBundle{}
		if err := m.kv.Unmarshal(res.Responses[0].GetResponseRange().Kvs[0].Value, existing); err != nil {
			return nil, status.Wrap(err, status.Internal)
		}
		return existing, nil
	}
	m.log.Info("boot bundle created", zap.String("bundle", name))
	return b, nil
}

func (m *bootManager) listBundles(ctx context.Context) ([]BootBundle, error) {
	res, err := m.kv.Get(ctx, bootBundlesRootKey(m.kv).Prefix(true).String(), clientv3.WithPrefix())
	if err != nil {
		return nil, status.Wrap(err, status.Internal)
	}
	bundles := []BootBundle{}
	for _, kv := range res.Kvs {
		// Structural match: blob keys (even ones named "meta") must not be
		// parsed as bundle metadata.
		name, ok := parseBundleMetaKey(string(kv.Key))
		if !ok {
			continue
		}
		b := BootBundle{}
		if err := m.kv.Unmarshal(kv.Value, &b); err != nil {
			m.log.Warn("skipping corrupt boot bundle meta", zap.String("bundle", name), zap.Error(err))
			continue
		}
		bundles = append(bundles, b)
	}
	sort.Slice(bundles, func(i, j int) bool { return bundles[i].Name < bundles[j].Name })
	return bundles, nil
}

// --- candidate versions ---

// createVersion creates a candidate with a client-supplied stable identity.
// Re-submitting the same (bundle, id) with the same manifest is idempotent;
// the same id with a different manifest conflicts and leaves the stored
// candidate untouched.
func (m *bootManager) createVersion(ctx context.Context, bundle, id string, files []BootFileSpec) (*BootVersion, error) {
	if err := validateBootName(bundle); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	if err := validateBootName(id); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	if err := validateBootManifest(files); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	if _, _, err := m.loadBundle(ctx, bundle); err != nil {
		return nil, err
	}
	key := bootVersionMetaKey(m.kv, bundle, id)
	now := m.now().UTC()
	v := &BootVersion{
		Bundle:    bundle,
		ID:        id,
		State:     bootStateCandidate,
		Files:     files,
		Received:  map[string]BootReceivedFile{},
		CreatedAt: now,
		UpdatedAt: now,
	}
	raw, err := m.kv.Marshal(v)
	if err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	res, err := m.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key.String()), "=", 0)).
		Then(clientv3.OpPut(key.String(), string(raw))).
		Else(clientv3.OpGet(key.String())).
		Commit()
	if err != nil {
		return nil, status.Wrap(err, status.Internal)
	}
	if !res.Succeeded {
		existing := &BootVersion{}
		if err := m.kv.Unmarshal(res.Responses[0].GetResponseRange().Kvs[0].Value, existing); err != nil {
			return nil, status.Wrap(err, status.Internal)
		}
		if !reflect.DeepEqual(existing.Files, files) {
			return nil, status.WithDescription(status.AlreadyExists,
				"boot version already exists with a different manifest")
		}
		return existing, nil
	}
	m.log.Info("boot candidate version created",
		zap.String("bundle", bundle), zap.String("version", id))
	return v, nil
}

func (m *bootManager) listVersions(ctx context.Context, bundle string) ([]BootVersion, error) {
	if _, _, err := m.loadBundle(ctx, bundle); err != nil {
		return nil, err
	}
	res, err := m.kv.Get(ctx, bootVersionsPrefixKey(m.kv, bundle).String(), clientv3.WithPrefix())
	if err != nil {
		return nil, status.Wrap(err, status.Internal)
	}
	out := []BootVersion{}
	for _, kv := range res.Kvs {
		// Exact structural match for .../<bundle>/versions/<id>/meta; file
		// blobs whose path ends in "meta" (e.g. files/cfg/meta) are ignored.
		_, id, ok := parseVersionMetaKey(string(kv.Key))
		if !ok {
			continue
		}
		v := BootVersion{}
		if err := m.kv.Unmarshal(kv.Value, &v); err != nil {
			m.log.Warn("skipping corrupt boot version meta",
				zap.String("bundle", bundle), zap.String("version", id), zap.Error(err))
			continue
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// putVersionFile writes one file blob into an open candidate. Blobs are
// write-once: identical re-uploads are idempotent, different content for an
// already received path conflicts without touching the stored blob.
func (m *bootManager) putVersionFile(ctx context.Context, bundle, id, path string, data []byte) error {
	if err := validateBootName(bundle); err != nil {
		return status.Wrap(err, status.InvalidArgument)
	}
	if err := validateBootName(id); err != nil {
		return status.Wrap(err, status.InvalidArgument)
	}
	if err := validateBootRelativePath(path); err != nil {
		return status.Wrap(err, status.InvalidArgument)
	}
	if len(data) >= etcdMaxSize {
		return status.WithDescription(status.InvalidArgument, "file too big")
	}
	sum := sha256.Sum256(data)
	digest := hex.EncodeToString(sum[:])

	// Meta CAS loop: concurrent uploads to different paths of the same
	// candidate must not lose each other's Received updates.
	for {
		ver, rev, err := m.loadVersion(ctx, bundle, id)
		if err != nil {
			return err
		}
		if ver.State != bootStateCandidate {
			return status.WithDescription(status.FailedPrecondition,
				"boot version is not an open candidate")
		}
		spec, ok := ver.specFor(path)
		if !ok {
			return status.WithDescription(status.InvalidArgument,
				"path is not part of the version manifest")
		}
		if spec.SHA256 != "" && spec.SHA256 != digest {
			return status.WithDescription(status.InvalidArgument,
				"uploaded content does not match the declared sha256")
		}
		if existing, ok := ver.Received[path]; ok {
			if existing.SHA256 == digest {
				return nil
			}
			return status.WithDescription(status.AlreadyExists,
				"path already received with different content")
		}

		if err := m.claimBlob(ctx, bundle, id, path, data, digest); err != nil {
			return err
		}

		next := *ver
		next.Received = cloneReceived(ver.Received)
		next.Received[path] = BootReceivedFile{Size: len(data), SHA256: digest}
		next.UpdatedAt = m.now().UTC()
		raw, err := m.kv.Marshal(&next)
		if err != nil {
			return status.Wrap(err, status.InvalidArgument)
		}
		metaKey := bootVersionMetaKey(m.kv, bundle, id)
		txn, err := m.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(metaKey.String()), "=", rev)).
			Then(clientv3.OpPut(metaKey.String(), string(raw))).
			Commit()
		if err != nil {
			return status.Wrap(err, status.Internal)
		}
		if txn.Succeeded {
			m.log.Debug("boot candidate file received",
				zap.String("bundle", bundle), zap.String("version", id), zap.String("path", path))
			return nil
		}
		// Meta moved (concurrent upload); re-validate against the new state.
	}
}

// claimBlob writes the immutable blob key, treating an identical existing blob
// as success and a different existing blob as a content conflict.
func (m *bootManager) claimBlob(ctx context.Context, bundle, id, path string, data []byte, digest string) error {
	blobKey := bootVersionFileKey(m.kv, bundle, id, path)
	res, err := m.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(blobKey.String()), "=", 0)).
		Then(clientv3.OpPut(blobKey.String(), string(data))).
		Else(clientv3.OpGet(blobKey.String())).
		Commit()
	if err != nil {
		return status.Wrap(err, status.Internal)
	}
	if res.Succeeded {
		return nil
	}
	kvs := res.Responses[0].GetResponseRange().Kvs
	if len(kvs) < 1 {
		return status.WithDescription(status.Internal, "boot blob conflict without value")
	}
	existing := sha256.Sum256(kvs[0].Value)
	if hex.EncodeToString(existing[:]) != digest {
		return status.WithDescription(status.AlreadyExists,
			"path already stored with different content")
	}
	return nil
}

func (m *bootManager) downloadVersionFile(ctx context.Context, bundle, id, path string) ([]byte, error) {
	if err := validateBootName(bundle); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	if err := validateBootName(id); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	if err := validateBootRelativePath(path); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	ver, _, err := m.loadVersion(ctx, bundle, id)
	if err != nil {
		return nil, err
	}
	if _, ok := ver.specFor(path); !ok {
		return nil, status.WithDescription(status.NotFound, "path is not part of the version manifest")
	}
	res, err := m.kv.Get(ctx, bootVersionFileKey(m.kv, bundle, id, path).String())
	if err != nil {
		return nil, status.Wrap(err, status.Internal)
	}
	if len(res.Kvs) < 1 {
		return nil, status.WithDescription(status.NotFound, "file not received yet")
	}
	return res.Kvs[0].Value, nil
}

// cancelVersion cancels an open candidate. Published/superseded versions can
// not be cancelled; the serving version can never change through cancel.
func (m *bootManager) cancelVersion(ctx context.Context, bundle, id string) (*BootVersion, error) {
	for {
		ver, rev, err := m.loadVersion(ctx, bundle, id)
		if err != nil {
			return nil, err
		}
		if ver.State != bootStateCandidate {
			return nil, status.WithDescription(status.FailedPrecondition,
				"only candidate versions can be cancelled")
		}
		next := *ver
		next.State = bootStateCancelled
		next.UpdatedAt = m.now().UTC()
		raw, err := m.kv.Marshal(&next)
		if err != nil {
			return nil, status.Wrap(err, status.InvalidArgument)
		}
		metaKey := bootVersionMetaKey(m.kv, bundle, id)
		txn, err := m.kv.Txn(ctx).
			If(clientv3.Compare(clientv3.ModRevision(metaKey.String()), "=", rev)).
			Then(clientv3.OpPut(metaKey.String(), string(raw))).
			Commit()
		if err != nil {
			return nil, status.Wrap(err, status.Internal)
		}
		if txn.Succeeded {
			m.log.Info("boot candidate cancelled",
				zap.String("bundle", bundle), zap.String("version", id))
			return &next, nil
		}
	}
}

func cloneReceived(in map[string]BootReceivedFile) map[string]BootReceivedFile {
	out := make(map[string]BootReceivedFile, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}
