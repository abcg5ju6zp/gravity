package tftp

import (
	"context"
	"time"

	"github.com/swaggest/usecase/status"
	"go.uber.org/zap"
)

const bootSweepInterval = 60 * time.Second

// readBootFile serves one boot/<bundle>/<path> TFTP read.
//
// Version pinning: when a client reads an entrypoint file of the current
// version, an IP-scoped pin is established. Every later read for the same
// bundle from that client is served from the pinned immutable version — even
// if a newer version gets published in the meantime. A path missing from the
// pinned version returns NotFound and never falls through to another version.
func (m *bootManager) readBootFile(ctx context.Context, clientIP, bundle, path string) ([]byte, error) {
	if versionID, ok := m.pinnedVersion(clientIP, bundle); ok {
		return m.readVersionFile(ctx, bundle, versionID, path)
	}

	bundleMeta, _, err := m.loadBundle(ctx, bundle)
	if err != nil {
		return nil, err
	}
	if bundleMeta.CurrentVersionID == "" {
		return nil, status.WithDescription(status.NotFound, "boot bundle has no published version")
	}
	versionID := bundleMeta.CurrentVersionID
	ver, _, err := m.loadVersion(ctx, bundle, versionID)
	if err != nil {
		return nil, err
	}
	if isEntrypoint(ver, path) {
		// First hit on an entrypoint fixes the version for this client. If a
		// concurrent read pinned another version first, honor the first pin.
		versionID = m.pinIfAbsent(clientIP, bundle, versionID)
		m.log.Debug("boot session pinned",
			zap.String("client", clientIP), zap.String("bundle", bundle), zap.String("version", versionID))
	}
	return m.readVersionFile(ctx, bundle, versionID, path)
}

// readVersionFile fetches an immutable blob from a specific version.
func (m *bootManager) readVersionFile(ctx context.Context, bundle, versionID, path string) ([]byte, error) {
	res, err := m.kv.Get(ctx, bootVersionFileKey(m.kv, bundle, versionID, path).String())
	if err != nil {
		return nil, status.Wrap(err, status.Internal)
	}
	if len(res.Kvs) < 1 {
		return nil, status.WithDescription(status.NotFound, "file not found in pinned boot version")
	}
	return res.Kvs[0].Value, nil
}

func isEntrypoint(ver *BootVersion, path string) bool {
	for _, f := range ver.Files {
		if f.Entrypoint && f.Path == path {
			return true
		}
	}
	return false
}

// --- in-memory pin table (critical sections never perform etcd IO) ---

// pinnedVersion returns the currently pinned version and refreshes its idle
// timer. Stale pins expire lazily on access.
func (m *bootManager) pinnedVersion(clientIP, bundle string) (string, bool) {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	pins, ok := m.sessions[clientIP]
	if !ok {
		return "", false
	}
	pin, ok := pins[bundle]
	if !ok {
		return "", false
	}
	if m.now().Sub(pin.lastAccess) > bootSessionTTL {
		delete(pins, bundle)
		if len(pins) == 0 {
			delete(m.sessions, clientIP)
		}
		return "", false
	}
	pin.lastAccess = m.now()
	pins[bundle] = pin
	return pin.versionID, true
}

// pinIfAbsent pins versionID for the client and returns the effective pin
// (an existing concurrent pin wins).
func (m *bootManager) pinIfAbsent(clientIP, bundle, versionID string) string {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	pins, ok := m.sessions[clientIP]
	if !ok {
		pins = map[string]bootPin{}
		m.sessions[clientIP] = pins
	}
	if pin, ok := pins[bundle]; ok {
		return pin.versionID
	}
	pins[bundle] = bootPin{versionID: versionID, lastAccess: m.now()}
	return versionID
}

// pinnedVersionSnapshot returns the set of "<bundle>/<version>" currently
// held by read sessions, used by GC as a live-reference set.
func (m *bootManager) pinnedVersionSnapshot() map[string]struct{} {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	out := map[string]struct{}{}
	for _, pins := range m.sessions {
		for bundle, pin := range pins {
			out[bundle+"/"+pin.versionID] = struct{}{}
		}
	}
	return out
}

// sweepExpired removes idle pins older than the TTL.
func (m *bootManager) sweepExpired() {
	m.sessionsMu.Lock()
	defer m.sessionsMu.Unlock()
	now := m.now()
	for ip, pins := range m.sessions {
		for bundle, pin := range pins {
			if now.Sub(pin.lastAccess) > bootSessionTTL {
				delete(pins, bundle)
			}
		}
		if len(pins) == 0 {
			delete(m.sessions, ip)
		}
	}
}

// runJanitor periodically evicts expired pins and returns when ctx is done.
func (m *bootManager) runJanitor(ctx context.Context) {
	ticker := time.NewTicker(bootSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweepExpired()
		}
	}
}
