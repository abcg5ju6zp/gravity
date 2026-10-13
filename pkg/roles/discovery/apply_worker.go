package discovery

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"beryju.io/gravity/pkg/roles"
	dhcptypes "beryju.io/gravity/pkg/roles/dhcp/types"
	"beryju.io/gravity/pkg/roles/discovery/types"
	dnstypes "beryju.io/gravity/pkg/roles/dns/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"go.uber.org/zap"
)

var (
	errJobFenceMoved = errors.New("apply job revision moved")
	errStepPermanent = errors.New("step permanently blocked")
)

// applySessionTTL is the etcd session TTL for the per-job lock. A dead
// worker's lock releases within this window, allowing takeover.
const applySessionTTL = 10

// runApplyJob executes one job under the per-job distributed lock. If the
// lock is already held (another node, or a newer attempt), it returns
// silently; the lock owner drives the job.
func (r *Role) runApplyJob(ctx context.Context, applyID string) {
	session, err := concurrency.NewSession(r.i.KV().Client,
		concurrency.WithTTL(applySessionTTL),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		r.log.Warn("failed to create apply session", zap.String("apply", applyID), zap.Error(err))
		return
	}
	defer session.Close()
	mutex := concurrency.NewMutex(session, r.applyLockKey(applyID))
	if err := mutex.TryLock(ctx); err != nil {
		if errors.Is(err, concurrency.ErrLocked) {
			return
		}
		r.log.Warn("failed to acquire apply lock", zap.String("apply", applyID), zap.Error(err))
		return
	}
	defer func() {
		_ = mutex.Unlock(context.Background())
	}()

	if err := r.executeJob(ctx, applyID); err != nil {
		r.log.Warn("apply job execution stopped", zap.String("apply", applyID), zap.Error(err))
	}
}

// executeJob runs the item loop against the current job revision. All
// progress is persisted through compare-and-swap puts on the job key, so a
// stale worker can never overwrite newer job state.
func (r *Role) executeJob(ctx context.Context, applyID string) error {
	job, err := r.getJob(ctx, applyID)
	if err != nil {
		return err
	}
	switch job.Status {
	case types.JobStatusCancelling:
		return r.finalizeCancellation(ctx, job)
	case types.JobStatusPending:
		now := time.Now().UTC()
		job.Status = types.JobStatusRunning
		job.StartedAt = &now
		if err := r.commitJob(ctx, job, nil); err != nil {
			return err
		}
	case types.JobStatusRunning:
		// Resumed after restart: continue directly.
	default:
		// Terminal.
		return nil
	}

	for itemIdx := range job.Items {
		// Fresh fence check at every item boundary.
		moved, err := r.refreshJob(ctx, job)
		if err != nil {
			return err
		}
		if moved {
			if job.Status == types.JobStatusCancelling {
				return r.finalizeCancellation(ctx, job)
			}
			return nil
		}

		if err := r.executeItem(ctx, job, itemIdx); err != nil {
			if errors.Is(err, errJobFenceMoved) {
				if job.Status == types.JobStatusCancelling {
					return r.finalizeCancellation(ctx, job)
				}
				// A newer generation (retry) or another owner supersedes us.
				return nil
			}
			return err
		}
	}

	return r.finalizeJob(ctx, job)
}

// refreshJob re-reads the job key and refreshes the in-memory copy. It
// returns moved=true whenever the status or fencing generation changed.
func (r *Role) refreshJob(ctx context.Context, job *ApplyJob) (bool, error) {
	fresh, err := r.getJob(ctx, job.ID)
	if err != nil {
		return false, err
	}
	moved := fresh.Status != job.Status || fresh.FencingGeneration != job.FencingGeneration
	*job = *fresh
	return moved, nil
}

// executeItem runs every non-confirmed step of one item, persisting progress
// after each step. The device mark is the item's claim gate: when it cannot
// be claimed, remaining target steps are marked conflict without writing
// (a conflicting device must never receive a partial DHCP/DNS set). Target
// steps are independent of each other. A blocked item never aborts other
// items.
func (r *Role) executeItem(ctx context.Context, job *ApplyJob, itemIdx int) error {
	item := &job.Items[itemIdx]
	var deviceGateReason string
	for stepIdx := range item.Steps {
		step := &item.Steps[stepIdx]
		if step.Name == types.StepNameDeviceMark {
			switch step.Status {
			case types.StepStatusConfirmed:
				deviceGateReason = ""
			case types.StepStatusConflict, types.StepStatusFailed, types.StepStatusCancelled:
				deviceGateReason = step.Reason
				if deviceGateReason == "" {
					deviceGateReason = types.ReasonDeviceClaimed
				}
			}
		}
		if step.Status == types.StepStatusConfirmed {
			continue
		}
		if step.Name == types.StepNameDeviceMark && step.Status != types.StepStatusPending {
			continue
		}
		if step.Name != types.StepNameDeviceMark && deviceGateReason != "" {
			step.Status = types.StepStatusConflict
			step.Reason = deviceGateReason
			item.setResult()
			if job.Status != types.JobStatusCancelling {
				job.Status = types.JobStatusRunning
			}
			if err := r.commitJob(ctx, job, nil); err != nil {
				latest, getErr := r.getJob(ctx, job.ID)
				if getErr != nil {
					return err
				}
				*job = *latest
				return errJobFenceMoved
			}
			continue
		}

		// Cancellation boundary: refresh first.
		fresh, err := r.getJob(ctx, job.ID)
		if err != nil {
			return err
		}
		if fresh.Status == types.JobStatusCancelling {
			*job = *fresh
			return r.finalizeCancellation(ctx, job)
		}
		job.modRev = fresh.modRev

		r.runStep(ctx, job, item, step)
		if step.Name == types.StepNameDeviceMark &&
			(step.Status == types.StepStatusConflict || step.Status == types.StepStatusFailed) {
			deviceGateReason = step.Reason
			if deviceGateReason == "" {
				deviceGateReason = types.ReasonDeviceClaimed
			}
		}
		item.setResult()
		// Keep the job in running during the loop; the terminal status is
		// published exactly once by finalizeJob (with FinishedAt and the
		// retention lease), so observers can never see a premature terminal.
		if job.Status != types.JobStatusCancelling {
			job.Status = types.JobStatusRunning
		}

		if err := r.commitJob(ctx, job, nil); err != nil {
			// Reload the canonical state: the fence moved (cancel/retry).
			latest, getErr := r.getJob(ctx, job.ID)
			if getErr != nil {
				return err
			}
			*job = *latest
			return errJobFenceMoved
		}
	}
	return nil
}

// runStep dispatches exactly one step and records its outcome on the step.
// It never returns an error for conflicts: conflicts are state, not faults.
func (r *Role) runStep(ctx context.Context, job *ApplyJob, item *ApplyItem, step *ApplyStep) {
	now := time.Now().UTC()
	switch step.Name {
	case types.StepNameDeviceMark:
		r.stepDeviceMark(ctx, job, item, step)
	default:
		r.stepEnsureTarget(ctx, job, item, step)
	}
	if step.Status == types.StepStatusConfirmed {
		step.ConfirmedAt = &now
	}
}

func (r *Role) stepDeviceMark(ctx context.Context, job *ApplyJob, item *ApplyItem, step *ApplyStep) {
	key := r.i.KV().Key(types.KeyRole, types.KeyDevices, item.Identifier).String()
	current, err := r.i.KV().Get(ctx, key)
	if err != nil {
		step.Status = types.StepStatusFailed
		step.Reason = ""
		step.ErrorMessage = err.Error()
		return
	}
	if len(current.Kvs) == 0 {
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceMissing
		return
	}
	dev := r.deviceFromKV(current.Kvs[0])
	if dev == nil {
		step.Status = types.StepStatusFailed
		step.Reason = ""
		step.ErrorMessage = "failed to parse live device"
		return
	}
	switch {
	case dev.ApplyID == job.ID:
		// Idempotent reconciliation: our mark is already there.
		step.Status = types.StepStatusConfirmed
		step.Reconciled = true
		step.TargetKey = key
		step.CreateRev = current.Kvs[0].CreateRevision
		step.ModRev = current.Kvs[0].ModRevision
		return
	case dev.Source == types.DeviceSourceManual:
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceManual
		return
	case dev.ApplyID != "":
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceClaimed
		return
	case dev.SourceRevision > job.SourceRevision:
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceNewerScan
		return
	}

	dev.ApplyID = job.ID
	dev.ApplyStatus = types.ItemResultApplied
	applied := time.Now().UTC()
	dev.AppliedAt = &applied
	raw, err := json.Marshal(dev)
	if err != nil {
		step.Status = types.StepStatusFailed
		step.ErrorMessage = err.Error()
		return
	}

	cmps := []clientv3.Cmp{
		clientv3.Compare(clientv3.ModRevision(r.jobKey(job.ID)), "=", job.modRev),
	}
	putOpts := []clientv3.OpOption{}
	if step.ExpectedModRevision == 0 {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(key), "=", 0))
	} else {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", step.ExpectedModRevision))
		// Preserve the device TTL lease.
		if current.Kvs[0].Lease != 0 {
			putOpts = append(putOpts, clientv3.WithLease(clientv3.LeaseID(current.Kvs[0].Lease)))
		}
	}

	res, err := r.i.KV().Txn(ctx).
		If(cmps...).
		Then(clientv3.OpPut(key, string(raw), putOpts...)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		step.Status = types.StepStatusFailed
		step.ErrorMessage = err.Error()
		return
	}
	if res.Succeeded {
		newRev := res.Header.Revision
		if len(res.Responses) > 0 && res.Responses[0].GetResponsePut() != nil {
			newRev = res.Responses[0].GetResponsePut().Header.Revision
		}
		step.Status = types.StepStatusConfirmed
		step.TargetKey = key
		step.CreateRev = newRev
		step.ModRev = newRev
		return
	}

	// Txn failed: the job fence or the device pin moved. Reclassify.
	rangeRes := res.Responses[0].GetResponseRange()
	if rangeRes == nil || len(rangeRes.Kvs) == 0 {
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceMissing
		return
	}
	latest := r.deviceFromKV(rangeRes.Kvs[0])
	switch {
	case latest != nil && latest.ApplyID == job.ID:
		step.Status = types.StepStatusConfirmed
		step.Reconciled = true
		step.TargetKey = key
		step.CreateRev = rangeRes.Kvs[0].CreateRevision
		step.ModRev = rangeRes.Kvs[0].ModRevision
	case latest == nil || (step.ExpectedModRevision == 0):
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceMissing
	case latest.Source == types.DeviceSourceManual:
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceManual
	case latest.ApplyID != "":
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceClaimed
	case latest.SourceRevision > job.SourceRevision:
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceNewerScan
	case rangeRes.Kvs[0].ModRevision != step.ExpectedModRevision:
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonDeviceManual
	default:
		// Device content/revision still matches the pin: the job fence moved.
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonJobNotClaimable
	}
}

// stepEnsureTarget dispatches the dhcp/dns ensure event for one pinned target
// step and records the outcome.
func (r *Role) stepEnsureTarget(ctx context.Context, job *ApplyJob, item *ApplyItem, step *ApplyStep) {
	topic, data, ok := r.buildEnsurePayload(job, item, step)
	if !ok {
		// Missing zone for a single-label hostname is a configuration gap,
		// not an infrastructure failure.
		step.Status = types.StepStatusConflict
		step.Reason = types.ReasonZoneMissing
		return
	}
	ev := roles.NewEvent(ctx, data)
	r.i.DispatchEvent(topic, ev)
	outcomes := ev.Outcomes()
	if len(outcomes) == 0 {
		step.Status = types.StepStatusSkipped
		step.Reason = types.ReasonRoleUnavailable
		return
	}
	o := outcomes[0]
	if step.TargetKey != "" && o.TargetKey != "" && step.TargetKey != o.TargetKey {
		// The target role resolved a different key than the preview pinned.
		step.Status = types.StepStatusFailed
		step.ErrorMessage = "target key drifted from preview: " + o.TargetKey
		return
	}
	step.TargetKey = o.TargetKey
	step.CreateRev = o.CreateRevision
	step.ModRev = o.ModRevision
	step.Siblings = o.Siblings
	step.Reconciled = o.AlreadyProvisioned
	switch o.Action {
	case roles.EnsureActionCreate, roles.EnsureActionKeep:
		step.Status = types.StepStatusConfirmed
		step.Reason = o.Reason
	case roles.EnsureActionSkip:
		// A missing role is an environment-level skip; missing scope/zone
		// or MAC is a configuration conflict (job can still go partial).
		step.Reason = o.Reason
		if o.Reason == types.ReasonRoleUnavailable {
			step.Status = types.StepStatusSkipped
		} else {
			step.Status = types.StepStatusConflict
		}
	default:
		step.Status = types.StepStatusConflict
		step.Reason = o.Reason
		if o.ErrorMessage != "" {
			step.Status = types.StepStatusFailed
			step.ErrorMessage = o.ErrorMessage
		}
	}
}

func (r *Role) buildEnsurePayload(job *ApplyJob, item *ApplyItem, step *ApplyStep) (string, map[string]interface{}, bool) {
	common := map[string]interface{}{
		types.EventFieldDryRun:            false,
		types.EventFieldApplyID:           job.ID,
		types.EventFieldScanID:            job.ScanID,
		types.EventFieldFenceKey:          r.jobKey(job.ID),
		types.EventFieldFenceRev:          job.modRev,
		types.EventFieldExpectedCreateRev: step.ExpectedCreateRevision,
		types.EventFieldExpectedModRev:    step.ExpectedModRevision,
	}
	switch step.Name {
	case types.StepNameDHCPReservation:
		common[types.EventFieldMAC] = item.MAC
		common[types.EventFieldHostname] = shortHostname(item.Hostname)
		common[types.EventFieldAddress] = item.IP
		common[types.EventFieldScope] = job.DHCPScope
		return dhcptypes.EventTopicDHCPLeaseEnsure, common, true
	case types.StepNameDNSForward, types.StepNameDNSReverse:
		fqdn, hostname, err := buildFQDN(item.Hostname, job.DNSZone)
		if err != nil {
			return "", nil, false
		}
		direction := types.EventDirectionForward
		if step.Name == types.StepNameDNSReverse {
			direction = types.EventDirectionReverse
		}
		common[types.EventFieldDirection] = direction
		common[types.EventFieldFQDN] = fqdn
		common[types.EventFieldHostname] = hostname
		common[types.EventFieldAddress] = item.IP
		common[types.EventFieldIdentifier] = item.Identifier
		return dnstypes.EventTopicDNSRecordEnsure, common, true
	}
	return "", nil, false
}

// commitJob persists the job iff its revision still matches the in-memory
// fencing revision. Extra ops (e.g. retention lease attachment) can be
// appended. On success the local revision is refreshed.
func (r *Role) commitJob(ctx context.Context, job *ApplyJob, extraOps []clientv3.Op) error {
	raw, err := job.marshal()
	if err != nil {
		return err
	}
	key := r.jobKey(job.ID)
	then := append([]clientv3.Op{clientv3.OpPut(key, string(raw))}, extraOps...)
	res, err := r.i.KV().Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", job.modRev)).
		Then(then...).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		if len(res.Responses) >= 1 && res.Responses[0].GetResponseRange() != nil && len(res.Responses[0].GetResponseRange().Kvs) > 0 {
			fresh, parseErr := r.jobFromKV(res.Responses[0].GetResponseRange().Kvs[0])
			if parseErr == nil {
				job.modRev = fresh.modRev
			}
		}
		return errJobFenceMoved
	}
	if len(res.Responses) > 0 && res.Responses[0].GetResponsePut() != nil {
		job.modRev = res.Responses[0].GetResponsePut().Header.Revision
	} else {
		job.modRev = res.Header.Revision
	}
	return nil
}

// finalizeJob moves a fully processed job to its terminal status and attaches
// the retention lease when configured.
func (r *Role) finalizeJob(ctx context.Context, job *ApplyJob) error {
	job.Status = job.recalculate()
	return r.persistTerminalWithRetention(ctx, job)
}

// persistTerminalWithRetention stamps FinishedAt and commits the job with a
// fresh retention lease (job key + idempotency index). It is the single place
// through which any terminal transition - worker, cancellation or retry -
// must persist, so a terminal key can never lose its TTL lease.
func (r *Role) persistTerminalWithRetention(ctx context.Context, job *ApplyJob) error {
	if !terminalStatus(job.Status) {
		// Should not happen: normalize through recalculate.
		job.Status = job.recalculate()
	}
	finished := time.Now().UTC()
	job.FinishedAt = &finished
	return r.commitTerminal(ctx, job)
}

// finalizeCancellation reconciles open steps against the store, marks the
// rest cancelled and terminates the job. A step whose object already carries
// this job's provenance (written just before the fence moved) is confirmed
// rather than cancelled, so job state never disagrees with reality. Nothing
// is deleted.
func (r *Role) finalizeCancellation(ctx context.Context, job *ApplyJob) error {
	job.Status = types.JobStatusCancelling
	now := time.Now().UTC()
	for idx := range job.Items {
		item := &job.Items[idx]
		for sIdx := range item.Steps {
			step := &item.Steps[sIdx]
			if step.Status != types.StepStatusPending {
				continue
			}
			if r.stepAlreadyOurs(ctx, job, item, step) {
				step.Status = types.StepStatusConfirmed
				step.Reconciled = true
				step.Reason = types.ReasonAlreadyProvisioned
				step.ConfirmedAt = &now
				continue
			}
			step.Status = types.StepStatusCancelled
		}
		item.setResult()
	}
	job.Status = types.JobStatusCancelled
	return r.persistTerminalWithRetention(ctx, job)
}

// stepAlreadyOurs reports whether the object behind a pending step already
// exists with this job's provenance. Read-only dry-run dispatch is used for
// target steps so the key layout stays inside the dhcp/dns roles.
func (r *Role) stepAlreadyOurs(ctx context.Context, job *ApplyJob, item *ApplyItem, step *ApplyStep) bool {
	if step.Name == types.StepNameDeviceMark {
		key := r.i.KV().Key(types.KeyRole, types.KeyDevices, item.Identifier).String()
		res, err := r.i.KV().Get(ctx, key)
		if err != nil || len(res.Kvs) == 0 {
			return false
		}
		dev := r.deviceFromKV(res.Kvs[0])
		if dev == nil || dev.ApplyID != job.ID {
			return false
		}
		step.TargetKey = key
		step.CreateRev = res.Kvs[0].CreateRevision
		step.ModRev = res.Kvs[0].ModRevision
		return true
	}
	topic, data, ok := r.buildEnsurePayload(job, item, step)
	if !ok {
		return false
	}
	data[types.EventFieldDryRun] = true
	ev := roles.NewEvent(ctx, data)
	r.i.DispatchEvent(topic, ev)
	outcomes := ev.Outcomes()
	if len(outcomes) == 0 {
		return false
	}
	o := outcomes[0]
	if o.AlreadyProvisioned {
		step.TargetKey = o.TargetKey
		step.CreateRev = o.CreateRevision
		step.ModRev = o.ModRevision
	}
	return o.AlreadyProvisioned
}

// commitTerminal persists the final job revision and attaches the retention
// lease to both the job key and its idempotency index (when one exists).
func (r *Role) commitTerminal(ctx context.Context, job *ApplyJob) error {
	raw, err := job.marshal()
	if err != nil {
		return err
	}
	key := r.jobKey(job.ID)

	ttl := r.applyRetentionSeconds()
	leaseID := clientv3.LeaseID(0)
	if ttl > 0 {
		grant, err := r.i.KV().Grant(ctx, ttl)
		if err != nil {
			r.log.Warn("failed to grant retention lease, persisting without TTL", zap.Error(err))
		} else {
			leaseID = grant.ID
		}
	}
	leaseOpt := []clientv3.OpOption{}
	if leaseID != 0 {
		leaseOpt = append(leaseOpt, clientv3.WithLease(leaseID))
	}
	thenOps := []clientv3.Op{}
	if job.IdempotencyKey != "" && leaseID != 0 {
		idemKey := r.idempotencyKey(job.IdempotencyKey)
		thenOps = append(thenOps, clientv3.OpPut(idemKey, job.ID, clientv3.WithLease(leaseID)))
	}
	thenOps = append(thenOps, clientv3.OpPut(key, string(raw), leaseOpt...))

	res, err := r.i.KV().Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", job.modRev)).
		Then(thenOps...).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return err
	}
	if !res.Succeeded {
		if len(res.Responses) >= 1 && res.Responses[0].GetResponseRange() != nil && len(res.Responses[0].GetResponseRange().Kvs) > 0 {
			fresh, parseErr := r.jobFromKV(res.Responses[0].GetResponseRange().Kvs[0])
			if parseErr == nil {
				job.modRev = fresh.modRev
			}
		}
		return errJobFenceMoved
	}
	last := res.Responses[len(res.Responses)-1]
	if last.GetResponsePut() != nil {
		job.modRev = last.GetResponsePut().Header.Revision
	} else {
		job.modRev = res.Header.Revision
	}
	return nil
}
