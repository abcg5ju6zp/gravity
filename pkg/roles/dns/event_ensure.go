package dns

import (
	"encoding/json"
	"errors"
	"net/netip"
	"strings"

	"beryju.io/gravity/pkg/roles"
	discoveryTypes "beryju.io/gravity/pkg/roles/discovery/types"
	"beryju.io/gravity/pkg/roles/dns/types"
	"beryju.io/gravity/pkg/roles/dns/utils"
	"github.com/miekg/dns"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// eventEnsureRecord implements the discovery apply dry-run/ensure contract
// for one forward (A/AAAA) or reverse (PTR) record. Writes are fenced by the
// job revision and pinned to the exact target record revisions returned by
// the preview; foreign records are never overwritten or deleted.
func (r *Role) eventEnsureRecord(ev *roles.Event) {
	dryRun, _ := ev.Payload.Data[discoveryTypes.EventFieldDryRun].(bool)
	applyID, _ := ev.Payload.Data[discoveryTypes.EventFieldApplyID].(string)
	scanID, _ := ev.Payload.Data[discoveryTypes.EventFieldScanID].(string)
	direction, _ := ev.Payload.Data[discoveryTypes.EventFieldDirection].(string)
	fqdn, _ := ev.Payload.Data[discoveryTypes.EventFieldFQDN].(string)
	hostname, _ := ev.Payload.Data[discoveryTypes.EventFieldHostname].(string)
	rawAddr, _ := ev.Payload.Data[discoveryTypes.EventFieldAddress].(string)
	uid, _ := ev.Payload.Data[discoveryTypes.EventFieldIdentifier].(string)
	fenceKey, _ := ev.Payload.Data[discoveryTypes.EventFieldFenceKey].(string)
	fenceRev := payloadEnsureInt64(ev.Payload.Data, discoveryTypes.EventFieldFenceRev)
	expectedCreate := payloadEnsureInt64(ev.Payload.Data, discoveryTypes.EventFieldExpectedCreateRev)
	expectedMod := payloadEnsureInt64(ev.Payload.Data, discoveryTypes.EventFieldExpectedModRev)

	stepName := types2StepName(direction)
	report := func(action, reason, key string, createRev, modRev, siblings int64, already bool, errMsg string) {
		ev.ReportOutcome(roles.EnsureOutcome{
			Step:               stepName,
			TargetKey:          key,
			CreateRevision:     createRev,
			ModRevision:        modRev,
			Action:             action,
			Reason:             reason,
			AlreadyProvisioned: already,
			Siblings:           siblings,
			ErrorMessage:       errMsg,
		})
	}

	ip, err := netip.ParseAddr(rawAddr)
	if err != nil {
		report(roles.EnsureActionConflict, "", "", 0, 0, 0, false, err.Error())
		return
	}

	zone, name, recType, data, err := r.resolveEnsureTarget(direction, fqdn, hostname, rawAddr, ip)
	if err != nil {
		if err == errEnsureZoneMissing {
			report(roles.EnsureActionSkip, discoveryTypes.ReasonZoneMissing, "", 0, 0, 0, false, "")
			return
		}
		report(roles.EnsureActionConflict, "", "", 0, 0, 0, false, err.Error())
		return
	}

	recordKey := r.i.KV().Key(
		types.KeyRole,
		types.KeyZones,
		strings.ToLower(zone.Name),
		strings.ToLower(name),
		recType,
		uid,
	).String()
	siblings := r.countSiblingRecords(ev, zone, name, recType, uid)

	current, err := r.i.KV().Get(ev.Context, recordKey)
	if err != nil {
		report(roles.EnsureActionConflict, "", recordKey, 0, 0, siblings, false, err.Error())
		return
	}
	var existing *Record
	if len(current.Kvs) > 0 {
		existing, err = zone.recordFromKV(current.Kvs[0])
		if err != nil {
			report(roles.EnsureActionConflict, "", recordKey, current.Kvs[0].CreateRevision, current.Kvs[0].ModRevision, siblings, false, err.Error())
			return
		}
	}

	currentAction, currentReason, already := classifyRecord(existing, applyID, data)
	createRev, modRev := int64(0), int64(0)
	if existing != nil {
		createRev, modRev = current.Kvs[0].CreateRevision, current.Kvs[0].ModRevision
	}
	if dryRun {
		report(currentAction, currentReason, recordKey, createRev, modRev, siblings, already, "")
		return
	}
	// Never claim/confirm a foreign, incompatible record, even when the
	// client pins its current revisions.
	if currentAction == roles.EnsureActionConflict {
		report(currentAction, currentReason, recordKey, createRev, modRev, siblings, false, "")
		return
	}

	// Always validate the fence; create writes when pinned absent, keep is a
	// fenced read when the target already exists.
	var thenOp clientv3.Op
	rec := zone.newRecord(name, recType)
	rec.uid = uid
	rec.Data = data
	rec.TTL = zone.DefaultTTL
	rec.Discovery = &discoveryTypes.Provenance{
		ApplyID: applyID,
		ScanID:  scanID,
		Source:  discoveryTypes.ProvenanceSource,
	}
	if currentAction == roles.EnsureActionCreate {
		raw, err := json.Marshal(rec)
		if err != nil {
			report(roles.EnsureActionConflict, "", recordKey, 0, 0, siblings, false, err.Error())
			return
		}
		thenOp = clientv3.OpPut(recordKey, string(raw))
	} else {
		thenOp = clientv3.OpGet(recordKey)
	}

	cmps := []clientv3.Cmp{}
	if fenceKey != "" {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(fenceKey), "=", fenceRev))
	}
	if expectedCreate == 0 {
		cmps = append(cmps, clientv3.Compare(clientv3.CreateRevision(recordKey), "=", 0))
	} else {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(recordKey), "=", expectedMod))
	}
	res, err := r.i.KV().Txn(ev.Context).
		If(cmps...).
		Then(thenOp).
		Else(clientv3.OpGet(recordKey)).
		Commit()
	if err != nil {
		report(roles.EnsureActionConflict, "", recordKey, 0, 0, siblings, false, err.Error())
		return
	}
	if res.Succeeded {
		if currentAction == roles.EnsureActionCreate {
			newRev := res.Header.Revision
			if len(res.Responses) > 0 && res.Responses[0].GetResponsePut() != nil {
				newRev = res.Responses[0].GetResponsePut().Header.Revision
			}
			report(roles.EnsureActionCreate, "", recordKey, 1, newRev, siblings, false, "")
			return
		}
		rangeRes := res.Responses[0].GetResponseRange()
		createRev, modRev := int64(0), int64(0)
		if rangeRes != nil && len(rangeRes.Kvs) > 0 {
			createRev, modRev = rangeRes.Kvs[0].CreateRevision, rangeRes.Kvs[0].ModRevision
		}
		report(roles.EnsureActionKeep, "", recordKey, createRev, modRev, siblings, already, "")
		return
	}

	if len(res.Responses) < 1 || res.Responses[0].GetResponseRange() == nil {
		report(roles.EnsureActionConflict, discoveryTypes.ReasonJobNotClaimable, recordKey, 0, 0, siblings, false, "")
		return
	}
	failed := res.Responses[0].GetResponseRange()
	if failed == nil || len(failed.Kvs) == 0 {
		report(roles.EnsureActionConflict, discoveryTypes.ReasonJobNotClaimable, recordKey, 0, 0, siblings, false, "")
		return
	}
	fresh, err := zone.recordFromKV(failed.Kvs[0])
	if err != nil {
		report(roles.EnsureActionConflict, discoveryTypes.ReasonTargetChanged, recordKey, failed.Kvs[0].CreateRevision, failed.Kvs[0].ModRevision, siblings, false, err.Error())
		return
	}
	action, reason, alreadyNow := classifyRecordFailure(fresh, applyID, data, expectedCreate, expectedMod, failed.Kvs[0].ModRevision)
	report(action, reason, recordKey, failed.Kvs[0].CreateRevision, failed.Kvs[0].ModRevision, siblings, alreadyNow, "")
}

// classifyRecordFailure mirrors the lease failure classification: foreign
// drift conflicts, an object owned by this job reconciles, and an unchanged
// pin means the worker fence was stale.
func classifyRecordFailure(fresh *Record, applyID, data string, expectedCreate, expectedMod, freshMod int64) (action, reason string, already bool) {
	ours := fresh.Discovery != nil && fresh.Discovery.ApplyID == applyID

	if expectedCreate == 0 {
		if ours {
			return roles.EnsureActionKeep, discoveryTypes.ReasonAlreadyProvisioned, true
		}
		return roles.EnsureActionConflict, discoveryTypes.ReasonTargetChanged, false
	}
	if fresh.Data != data {
		return roles.EnsureActionConflict, discoveryTypes.ReasonTargetOwnedOther, false
	}
	if freshMod != expectedMod {
		if ours {
			return roles.EnsureActionKeep, discoveryTypes.ReasonAlreadyProvisioned, true
		}
		return roles.EnsureActionConflict, discoveryTypes.ReasonTargetChanged, false
	}
	return roles.EnsureActionConflict, discoveryTypes.ReasonJobNotClaimable, false
}

var (
	errEnsureZoneMissing      = errors.New("no matching zone")
	errInvalidEnsureDirection = errors.New("invalid ensure direction")
)

// resolveEnsureTarget performs zone resolution and record key component
// computation the same way the legacy create handlers do, keeping the key
// layout knowledge inside the dns package.
func (r *Role) resolveEnsureTarget(direction, fqdn, hostname, rawAddr string, ip netip.Addr) (zone *Zone, name, recType, data string, err error) {
	switch direction {
	case discoveryTypes.EventDirectionForward:
		zone = r.FindZone(utils.EnsureTrailingPeriod(fqdn))
		if zone == nil {
			return nil, "", "", "", errEnsureZoneMissing
		}
		name = strings.ToLower(hostname)
		if ip.Is4() {
			recType = types.DNSRecordTypeA
		} else {
			recType = types.DNSRecordTypeAAAA
		}
		data = ip.String()
		return zone, name, recType, data, nil
	case discoveryTypes.EventDirectionReverse:
		rev, revErr := dns.ReverseAddr(ip.String())
		if revErr != nil {
			return nil, "", "", "", revErr
		}
		zone = r.FindZone(utils.EnsureTrailingPeriod(rev))
		if zone == nil {
			return nil, "", "", "", errEnsureZoneMissing
		}
		name = strings.TrimSuffix(rev, utils.EnsureLeadingPeriod(zone.Name))
		recType = types.DNSRecordTypePTR
		data = utils.EnsureTrailingPeriod(fqdn)
		return zone, name, recType, data, nil
	default:
		return nil, "", "", "", errInvalidEnsureDirection
	}
}

func (r *Role) countSiblingRecords(ev *roles.Event, zone *Zone, name, recType, uid string) int64 {
	prefix := r.i.KV().Key(
		types.KeyRole,
		types.KeyZones,
		strings.ToLower(zone.Name),
		strings.ToLower(name),
		recType,
	).Prefix(true).String()
	res, err := r.i.KV().Get(ev.Context, prefix, clientv3.WithPrefix())
	if err != nil {
		return 0
	}
	count := int64(0)
	for _, kv := range res.Kvs {
		rec, err := zone.recordFromKV(kv)
		if err != nil || rec == nil {
			continue
		}
		if rec.uid != uid {
			count++
		}
	}
	return count
}

func classifyRecord(existing *Record, applyID, data string) (action, reason string, already bool) {
	if existing == nil {
		return roles.EnsureActionCreate, "", false
	}
	if existing.Discovery != nil && existing.Discovery.ApplyID == applyID {
		return roles.EnsureActionKeep, discoveryTypes.ReasonAlreadyProvisioned, true
	}
	if existing.Data == data {
		return roles.EnsureActionKeep, "", false
	}
	return roles.EnsureActionConflict, discoveryTypes.ReasonTargetOwnedOther, false
}

func types2StepName(direction string) string {
	if direction == discoveryTypes.EventDirectionReverse {
		return discoveryTypes.StepNameDNSReverse
	}
	return discoveryTypes.StepNameDNSForward
}

func payloadEnsureInt64(data map[string]interface{}, key string) int64 {
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
