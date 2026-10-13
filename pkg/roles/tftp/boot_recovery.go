package tftp

import (
	"context"
	"strings"

	"beryju.io/gravity/pkg/roles/tftp/types"
	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// APIBootDeletedVersion reports one version removed by GC.
type APIBootDeletedVersion struct {
	Bundle    string `json:"bundle" required:"true"`
	VersionID string `json:"versionId" required:"true"`
	State     string `json:"state" required:"true"`
}

// recoverState rebuilds knowledge of boot metadata after a restart. The
// authoritative state lives in etcd (candidate metas, the current pointer),
// while read-session pins are intentionally in-memory only. Recovery
// enumerates all metadata, logs the restored set and flags dangling current
// pointers instead of silently rewriting them.
func (m *bootManager) recoverState(ctx context.Context) error {
	bundles, versions, err := m.enumerate(ctx)
	if err != nil {
		return err
	}
	candidates := 0
	for _, vs := range versions {
		for _, v := range vs {
			if v.State == bootStateCandidate {
				candidates++
			}
		}
	}
	m.log.Info("boot bundle state recovered",
		zap.Int("bundles", len(bundles)),
		zap.Int("recoverableCandidates", candidates))

	for name, b := range bundles {
		if b.CurrentVersionID == "" {
			continue
		}
		if _, ok := versions[name][b.CurrentVersionID]; !ok {
			m.log.Error("current boot pointer has no recoverable version meta",
				zap.String("bundle", name), zap.String("version", b.CurrentVersionID))
			continue
		}
		if cur := versions[name][b.CurrentVersionID]; cur.State != bootStatePublished {
			m.log.Error("current boot pointer does not resolve to a published version",
				zap.String("bundle", name),
				zap.String("version", b.CurrentVersionID),
				zap.String("state", cur.State))
		}
	}
	return nil
}

// enumerate returns all bundle metas keyed by name and all version metas
// keyed by bundle name then version ID. Corrupt records are logged and
// skipped without aborting enumeration of the rest.
func (m *bootManager) enumerate(ctx context.Context) (map[string]*BootBundle, map[string]map[string]*BootVersion, error) {
	res, err := m.kv.Get(ctx, bootBundlesRootKey(m.kv).Prefix(true).String(), clientv3.WithPrefix())
	if err != nil {
		return nil, nil, status.Wrap(err, status.Internal)
	}
	bundles := map[string]*BootBundle{}
	versions := map[string]map[string]*BootVersion{}
	for _, kv := range res.Kvs {
		key := string(kv.Key)
		// Structural recognition only: a bundle file blob may legitimately be
		// named "meta" (e.g. .../versions/v1/files/meta), so suffix matching
		// would misidentify it.
		if bundle, id, ok := parseVersionMetaKey(key); ok {
			v := &BootVersion{}
			if err := m.kv.Unmarshal(kv.Value, v); err != nil {
				m.log.Warn("skipping corrupt boot version meta", zap.String("key", key), zap.Error(err))
				continue
			}
			if versions[bundle] == nil {
				versions[bundle] = map[string]*BootVersion{}
			}
			versions[bundle][id] = v
		} else if name, ok := parseBundleMetaKey(key); ok {
			b := &BootBundle{}
			if err := m.kv.Unmarshal(kv.Value, b); err != nil {
				m.log.Warn("skipping corrupt boot bundle meta", zap.String("key", key), zap.Error(err))
				continue
			}
			bundles[name] = b
		}
		// Any other key is an immutable file blob; ignore silently.
	}
	return bundles, versions, nil
}

// parseBundleMetaKey extracts the bundle name from
// /tftp/boot/bundles/<bundle>/meta.
func parseBundleMetaKey(key string) (string, bool) {
	prefix := "/" + types.KeyRole + "/" + types.KeyBoot + "/" + types.KeyBootBundles + "/"
	rest, found := strings.CutPrefix(key, prefix)
	if !found {
		return "", false
	}
	segments := strings.Split(rest, "/")
	if len(segments) != 2 || segments[1] != types.KeyBootMeta {
		return "", false
	}
	return segments[0], true
}

// parseVersionMetaKey extracts (bundle, version) from
// /tftp/boot/bundles/<bundle>/versions/<version>/meta.
func parseVersionMetaKey(key string) (bundle, id string, ok bool) {
	prefix := "/" + types.KeyRole + "/" + types.KeyBoot + "/" + types.KeyBootBundles + "/"
	rest, found := strings.CutPrefix(key, prefix)
	if !found {
		return "", "", false
	}
	segments := strings.Split(rest, "/")
	if len(segments) != 4 || segments[1] != types.KeyBootVersions || segments[3] != types.KeyBootMeta {
		return "", "", false
	}
	return segments[0], segments[2], true
}

// garbageCollect is the administrator-triggered collection: it additionally
// purges cancelled versions, which startup GC deliberately keeps queryable.
func (m *bootManager) garbageCollect(ctx context.Context) ([]APIBootDeletedVersion, error) {
	return m.collectGarbage(ctx, true)
}

// garbageCollectStartup runs after recovery. Read sessions never survive a
// restart, so unpinned superseded versions are provably unreachable to new
// boots and are reclaimed; cancelled versions are retained for query until
// an administrator runs an explicit GC.
func (m *bootManager) garbageCollectStartup(ctx context.Context) ([]APIBootDeletedVersion, error) {
	return m.collectGarbage(ctx, false)
}

// collectGarbage deletes unreferenced boot version content. A version is
// protected when it is a bundle's current published version, an open
// candidate (recoverable), or pinned by an active TFTP read session.
// Superseded versions are collected once unprotected; cancelled versions are
// only collected when purgeCancelled is set. The live-reference snapshot is
// taken before any deletion.
func (m *bootManager) collectGarbage(ctx context.Context, purgeCancelled bool) ([]APIBootDeletedVersion, error) {
	bundles, versions, err := m.enumerate(ctx)
	if err != nil {
		return nil, err
	}

	// Live-reference snapshot from read sessions.
	pinned := m.pinnedVersionSnapshot()

	protected := func(bundle string, v *BootVersion) bool {
		if v.State == bootStateCandidate || v.State == bootStatePublished {
			return true
		}
		if b, ok := bundles[bundle]; ok && b.CurrentVersionID == v.ID {
			return true
		}
		_, isPinned := pinned[bundle+"/"+v.ID]
		return isPinned
	}

	deleted := []APIBootDeletedVersion{}
	for bundle, byID := range versions {
		for id, v := range byID {
			if v.State != bootStateSuperseded &&
				!(purgeCancelled && v.State == bootStateCancelled) {
				continue
			}
			if protected(bundle, v) {
				continue
			}
			// One transaction removes the whole version: files prefix + meta.
			res, err := m.kv.Txn(ctx).
				Then(
					clientv3.OpDelete(bootVersionFilesPrefixKey(m.kv, bundle, id).String(), clientv3.WithPrefix()),
					clientv3.OpDelete(bootVersionMetaKey(m.kv, bundle, id).String()),
				).
				Commit()
			if err != nil {
				return deleted, status.Wrap(err, status.Internal)
			}
			if !res.Succeeded {
				// No If conditions: a failed commit indicates an etcd problem.
				return deleted, status.WithDescription(status.Internal, "boot gc transaction failed")
			}
			m.log.Info("boot version garbage collected",
				zap.String("bundle", bundle), zap.String("version", id), zap.String("state", v.State))
			deleted = append(deleted, APIBootDeletedVersion{
				Bundle: bundle, VersionID: id, State: v.State,
			})
		}
	}
	return deleted, nil
}
