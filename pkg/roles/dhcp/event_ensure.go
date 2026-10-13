package dhcp

import (
	"encoding/json"

	"beryju.io/gravity/pkg/roles"
	"beryju.io/gravity/pkg/roles/dhcp/types"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

// eventEnsureLease implements the discovery apply dry-run/ensure contract
// for DHCP reservations. It never overwrites an existing foreign lease:
// writes only happen through a fenced compare-and-swap transaction pinned
// to the revisions returned by the preview.
func (r *Role) eventEnsureLease(ev *roles.Event) {
	dryRun, _ := ev.Payload.Data[discoveryTypes.EventFieldDryRun].(bool)
	applyID, _ := ev.Payload.Data[discoveryTypes.EventFieldApplyID].(string)
	scanID, _ := ev.Payload.Data[discoveryTypes.EventFieldScanID].(string)
	mac, _ := ev.Payload.Data[discoveryTypes.EventFieldMAC].(string)
	hostname, _ := ev.Payload.Data[discoveryTypes.EventFieldHostname].(string)
	address, _ := ev.Payload.Data[discoveryTypes.EventFieldAddress].(string)
	scopeName, _ := ev.Payload.Data[discoveryTypes.EventFieldScope].(string)
	fenceKey, _ := ev.Payload.Data[discoveryTypes.EventFieldFenceKey].(string)
	fenceRev := payloadInt64(ev.Payload.Data, discoveryTypes.EventFieldFenceRev)
	expectedCreate := payloadInt64(ev.Payload.Data, discoveryTypes.EventFieldExpectedCreateRev)
	expectedMod := payloadInt64(ev.Payload.Data, discoveryTypes.EventFieldExpectedModRev)

	report := func(action, reason string, createRev, modRev int64, already bool, errMsg string) {
		ev.ReportOutcome(roles.EnsureOutcome{
			Step:               discoveryTypes.StepNameDHCPReservation,
			TargetKey:          r.i.KV().Key(types.KeyRole, types.KeyLeases, mac).String(),
			CreateRevision:     createRev,
			ModRevision:        modRev,
			Action:             action,
			Reason:             reason,
			AlreadyProvisioned: already,
			ErrorMessage:       errMsg,
		})
	}

	if mac == "" {
		report(roles.EnsureActionSkip, discoveryTypes.ReasonMACRequired, 0, 0, false, "")
		return
	}
	scope, ok := r.scopes.GetPrefix(scopeName)
	if !ok {
		report(roles.EnsureActionSkip, discoveryTypes.ReasonScopeMissing, 0, 0, false, "")
		return
	}

	leaseKey := r.i.KV().Key(types.KeyRole, types.KeyLeases, mac).String()
	current, err := r.i.KV().Get(ev.Context, leaseKey)
	if err != nil {
		report(roles.EnsureActionConflict, "", 0, 0, false, err.Error())
		return
	}

	var existing *Lease
	if len(current.Kvs) > 0 {
		existing, err = r.leaseFromKV(current.Kvs[0])
		if err != nil {
			report(roles.EnsureActionConflict, "", current.Kvs[0].CreateRevision, current.Kvs[0].ModRevision, false, err.Error())
			return
		}
	}

	currentAction, currentReason, already := classifyLease(existing, applyID, address, hostname, scope.Name)
	createRev, modRev := int64(0), int64(0)
	if existing != nil {
		createRev, modRev = current.Kvs[0].CreateRevision, current.Kvs[0].ModRevision
	}
	if dryRun {
		report(currentAction, currentReason, createRev, modRev, already, "")
		return
	}
	// A foreign, incompatible object can never be claimed or confirmed -
	// not even when the client echoes its current revisions. Report the
	// conflict without writing.
	if currentAction == roles.EnsureActionConflict {
		report(currentAction, currentReason, createRev, modRev, false, "")
		return
	}

	// Build the CAS: always validate the fence; create writes when pinned
	// absent, keep performs a fenced read when the target already exists.
	var thenOp clientv3.Op
	lease := &Lease{
		Identifier: mac,
		Hostname:   hostname,
		Address:    address,
		ScopeKey:   scope.Name,
		Expiry:     -1,
		Discovery: &discoveryTypes.Provenance{
			ApplyID: applyID,
			ScanID:  scanID,
			Source:  discoveryTypes.ProvenanceSource,
		},
		inst:  scope.inst,
		log:   scope.log.With(zap.String("lease", mac)),
		scope: scope,
	}
	if currentAction == roles.EnsureActionCreate {
		raw, err := json.Marshal(lease)
		if err != nil {
			report(roles.EnsureActionConflict, "", 0, 0, false, err.Error())
			return
		}
		thenOp = clientv3.OpPut(leaseKey, string(raw))
	} else {
		thenOp = clientv3.OpGet(leaseKey)
	}

	cmps := []clientv3.Cmp{}
	if fenceKey != "" {
		cmps = append(cmps, fenceCompare(fenceKey, fenceRev))
	}
	if expectedCreate == 0 {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(leaseKey), "=", 0))
	} else {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(leaseKey), "=", expectedMod))
	}
	res, err := r.i.KV().Txn(ev.Context).
		If(cmps...).
		Then(thenOp).
		Else(clientv3.OpGet(leaseKey)).
		Commit()
	if err != nil {
		report(roles.EnsureActionConflict, "", 0, 0, false, err.Error())
		return
	}
	if res.Succeeded {
		if currentAction == roles.EnsureActionCreate {
			newRev := res.Header.Revision
			if len(res.Responses) > 0 && res.Responses[0].GetResponsePut() != nil {
				newRev = res.Responses[0].GetResponsePut().Header.Revision
			}
			// Run the shared post-put side effects; the DNS chain is
			// suppressed because Discovery is set.
			lease.afterPut(ev.Context, -1)
			report(roles.EnsureActionCreate, "", 1, newRev, false, "")
			return
		}
		rangeRes := res.Responses[0].GetResponseRange()
		createRev, modRev := int64(0), int64(0)
		if rangeRes != nil && len(rangeRes.Kvs) > 0 {
			createRev, modRev = rangeRes.Kvs[0].CreateRevision, rangeRes.Kvs[0].ModRevision
		}
		report(roles.EnsureActionKeep, "", createRev, modRev, already, "")
		return
	}

	// Txn failed: re-classify against the fresh state from the Else branch.
	if len(res.Responses) < 1 || res.Responses[0].GetResponseRange() == nil {
		report(roles.EnsureActionConflict, discoveryTypes.ReasonJobNotClaimable, 0, 0, false, "")
		return
	}
	failed := res.Responses[0].GetResponseRange()
	if failed == nil || len(failed.Kvs) == 0 {
		if expectedCreate != 0 && fenceKey != "" {
			report(roles.EnsureActionConflict, discoveryTypes.ReasonTargetChanged, 0, 0, false, "")
			return
		}
		report(roles.EnsureActionConflict, discoveryTypes.ReasonJobNotClaimable, 0, 0, false, "")
		return
	}
	fresh, err := r.leaseFromKV(failed.Kvs[0])
	if err != nil {
		report(roles.EnsureActionConflict, discoveryTypes.ReasonTargetChanged, failed.Kvs[0].CreateRevision, failed.Kvs[0].ModRevision, false, err.Error())
		return
	}
	action, reason, already := classifyLeaseFailure(fresh, leaseKey, applyID, address, hostname, scope.Name, expectedCreate, expectedMod, fenceKey, failed.Kvs[0].ModRevision)
	report(action, reason, failed.Kvs[0].CreateRevision, failed.Kvs[0].ModRevision, already, "")
}

// classifyLeaseFailure distinguishes target drift (never claimable) from a
// stale worker fence (object still matches its pin) and from idempotent
// reconciliation (the object already belongs to this job).
func classifyLeaseFailure(fresh *Lease, _, applyID, address, hostname, scopeName string, expectedCreate, expectedMod int64, fenceKey string, freshMod int64) (action, reason string, already bool) {
	ours := fresh.Discovery != nil && fresh.Discovery.ApplyID == applyID
	contentEqual := fresh.Address == address && fresh.Hostname == hostname && fresh.ScopeKey == scopeName

	if expectedCreate == 0 {
		// Pin said absent. An object owned by this job is an idempotent
		// retry; anything else is concurrent drift.
		if ours {
			return roles.EnsureActionKeep, discoveryTypes.ReasonAlreadyProvisioned, true
		}
		return roles.EnsureActionConflict, discoveryTypes.ReasonTargetChanged, false
	}
	// Pin said present at expectedMod.
	if !contentEqual {
		return roles.EnsureActionConflict, discoveryTypes.ReasonTargetOwnedOther, false
	}
	if freshMod != expectedMod {
		if ours {
			return roles.EnsureActionKeep, discoveryTypes.ReasonAlreadyProvisioned, true
		}
		return roles.EnsureActionConflict, discoveryTypes.ReasonTargetChanged, false
	}
	// Content and revision still match the pin: the fence is what failed.
	if ours {
		return roles.EnsureActionConflict, discoveryTypes.ReasonJobNotClaimable, false
	}
	return roles.EnsureActionConflict, discoveryTypes.ReasonJobNotClaimable, false
}

// classifyLease decides the preview/dry-run action for the current lease.
func classifyLease(existing *Lease, applyID, address, hostname, scopeName string) (action, reason string, already bool) {
	if existing == nil {
		return roles.EnsureActionCreate, "", false
	}
	if existing.Discovery != nil && existing.Discovery.ApplyID == applyID {
		return roles.EnsureActionKeep, discoveryTypes.ReasonAlreadyProvisioned, true
	}
	if existing.Address == address && existing.Hostname == hostname && existing.ScopeKey == scopeName {
		return roles.EnsureActionKeep, "", false
	}
	return roles.EnsureActionConflict, discoveryTypes.ReasonTargetOwnedOther, false
}

func payloadInt64(data map[string]interface{}, key string) int64 {
	v, ok := data[key]
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	return 0
}

func fenceCompare(fenceKey string, fenceRev int64) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(fenceKey), "=", fenceRev)
}
