package tftp

import (
	"context"
	"strings"

	"github.com/swaggest/usecase/status"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// publishVersion flips the bundle's single current-version pointer in one
// etcd transaction. It is idempotent on the stable (bundle, version) identity:
// a retried publish of the version already serving returns the original result
// without consuming a new sequence number.
//
// Arbitration: the transaction compares the ModRevision of both the bundle
// and the candidate meta. Commit order is the arbitration order — each
// successful commit occupies the next monotonic PublishSeq; a losing attempt
// gets Aborted together with the actual current pointer.
func (m *bootManager) publishVersion(ctx context.Context, bundle, id, ifCurrentVersion string) (*APIBootPublishOutput, error) {
	if err := validateBootName(bundle); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}
	if err := validateBootName(id); err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}

	ver, vRev, err := m.loadVersion(ctx, bundle, id)
	if err != nil {
		return nil, err
	}
	bundleMeta, bRev, err := m.loadBundle(ctx, bundle)
	if err != nil {
		return nil, err
	}

	// Idempotent reconciliation for an uncertain/lost publish response.
	switch ver.State {
	case bootStatePublished:
		if bundleMeta.CurrentVersionID == id {
			return &APIBootPublishOutput{
				Bundle: bundle, VersionID: id, State: bootStatePublished,
				CurrentVersionID: bundleMeta.CurrentVersionID,
				PublishSeq:       ver.PublishSeq,
			}, nil
		}
		// The version read observed "published" but the pointer names another
		// version — a newer publish may have landed between the two separate
		// reads. Re-read the version once to label it authoritatively.
		fresh, _, err := m.loadVersion(ctx, bundle, id)
		if err != nil {
			return nil, err
		}
		if fresh.State == bootStateSuperseded {
			return m.supersededResult(bundleMeta, fresh), nil
		}
		return nil, status.WithDescription(status.Internal,
			"inconsistent boot publish state: version published but not current")
	case bootStateSuperseded:
		// Overtaken by a later publish; reconcile with its history seq.
		return m.supersededResult(bundleMeta, ver), nil
	case bootStateCancelled:
		return nil, status.WithDescription(status.FailedPrecondition,
			"cancelled boot version cannot be published")
	case bootStateCandidate:
		// fall through to validation + transaction
	default:
		return nil, status.WithDescription(status.Internal, "unknown boot version state")
	}

	// Candidate precondition: completeness and checksum integrity.
	if missing := ver.Missing(); len(missing) > 0 {
		return nil, status.WithDescription(status.FailedPrecondition,
			"candidate is missing required files: "+strings.Join(missing, ","))
	}
	for _, f := range ver.Files {
		if f.SHA256 == "" {
			continue
		}
		rec, ok := ver.Received[f.Path]
		if !ok {
			continue // optional file never uploaded
		}
		if rec.SHA256 != f.SHA256 {
			return nil, status.WithDescription(status.FailedPrecondition,
				"received file does not match declared sha256: "+f.Path)
		}
	}

	// Optional compare-and-publish guard, evaluated inside the same txn.
	if ifCurrentVersion != "" && bundleMeta.CurrentVersionID != ifCurrentVersion {
		return nil, status.WithDescription(status.FailedPrecondition,
			"ifCurrentVersion does not match the current published version")
	}

	now := m.now().UTC()
	nextSeq := bundleMeta.PublishSeq + 1

	nextBundle := *bundleMeta
	nextBundle.CurrentVersionID = id
	nextBundle.PublishSeq = nextSeq
	nextBundle.UpdatedAt = now
	bundleRaw, err := m.kv.Marshal(&nextBundle)
	if err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}

	nextVersion := *ver
	nextVersion.State = bootStatePublished
	nextVersion.PublishSeq = nextSeq
	nextVersion.UpdatedAt = now
	versionRaw, err := m.kv.Marshal(&nextVersion)
	if err != nil {
		return nil, status.Wrap(err, status.InvalidArgument)
	}

	bundleKey := bootBundleMetaKey(m.kv, bundle)
	versionKey := bootVersionMetaKey(m.kv, bundle, id)

	// All mutations of the publish go into one atomic transaction: the bundle
	// pointer, the new published version, and the superseded previous version.
	thenOps := []clientv3.Op{
		clientv3.OpPut(bundleKey.String(), string(bundleRaw)),
		clientv3.OpPut(versionKey.String(), string(versionRaw)),
	}

	// Supersede the previous current version in the same transaction.
	if oldID := bundleMeta.CurrentVersionID; oldID != "" && oldID != id {
		oldVer, _, err := m.loadVersion(ctx, bundle, oldID)
		if err != nil {
			// Pointer dangles (e.g. externally tampered store); record and
			// keep the publish going rather than blocking the single switch.
			m.log.Warn("current boot pointer has no version meta",
				zap.String("bundle", bundle), zap.String("version", oldID), zap.Error(err))
		} else {
			oldNext := *oldVer
			oldNext.State = bootStateSuperseded
			oldNext.UpdatedAt = now
			oldRaw, err := m.kv.Marshal(&oldNext)
			if err != nil {
				return nil, status.Wrap(err, status.InvalidArgument)
			}
			thenOps = append(thenOps,
				clientv3.OpPut(bootVersionMetaKey(m.kv, bundle, oldID).String(), string(oldRaw)))
		}
	}

	if m.testBeforeCommit != nil {
		m.testBeforeCommit()
	}

	res, err := m.kv.Txn(ctx).
		If(
			clientv3.Compare(clientv3.ModRevision(bundleKey.String()), "=", bRev),
			clientv3.Compare(clientv3.ModRevision(versionKey.String()), "=", vRev),
		).
		Then(thenOps...).
		Else(clientv3.OpGet(bundleKey.String())).
		Commit()
	if err != nil {
		return nil, status.Wrap(err, status.Internal)
	}
	if !res.Succeeded {
		// Lost the arbitration race; surface the actual current pointer so the
		// caller can safely retry with the same identity.
		actual := &BootBundle{}
		kvs := res.Responses[0].GetResponseRange().Kvs
		if len(kvs) > 0 {
			_ = m.kv.Unmarshal(kvs[0].Value, actual)
		}
		return nil, status.WithDescription(status.Aborted,
			"boot publish lost arbitration; current="+actual.CurrentVersionID)
	}

	m.log.Info("boot version published",
		zap.String("bundle", bundle),
		zap.String("version", id),
		zap.Int64("publishSeq", nextSeq))
	return &APIBootPublishOutput{
		Bundle: bundle, VersionID: id, State: bootStatePublished,
		CurrentVersionID: id,
		PublishSeq:       nextSeq,
	}, nil
}

func (m *bootManager) supersededResult(b *BootBundle, v *BootVersion) *APIBootPublishOutput {
	return &APIBootPublishOutput{
		Bundle: b.Name, VersionID: v.ID, State: v.State,
		CurrentVersionID: b.CurrentVersionID,
		PublishSeq:       v.PublishSeq,
	}
}
