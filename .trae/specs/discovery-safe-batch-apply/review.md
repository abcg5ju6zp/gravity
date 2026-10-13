# Independent Review - discovery-safe-batch-apply

Review date: 2026-10-13
Scope: immutable scan results, version-pinned diff preview, fenced batch
applies with per-step progress, idempotent retries, recovery, and backward
compatibility of the discovery role.

Environment notes for verification:
- A local etcd must be reachable at `localhost:2379` (test default).
- Known pre-existing environment-dependent failures unrelated to this
  change: `TestRoleDNS_BlockyForwarder*`, `TestRoleDNS_IPForwarder*`
  (external DNS/docker compose services), `dhcp/oui/TestDB` (real OUI db).
- Packages must be run sequentially (`-p 1`) because the test harness resets
  a shared etcd; concurrent packages interfere (pre-existing limitation).

## Checkpoints

- [ ] **CP-1 (AC-1, rule)**: Every `RunDiscovery` writes scan metadata with
  non-empty stable scanID and the etcd source revision captured before the
  scan; status transitions running→finished/failed exactly once via CAS.
  Evidence: `pkg/roles/discovery/scan.go` (beginScan/finish),
  `TestScanLifecycleAndSnapshots`.

- [ ] **CP-2 (AC-2, rule)**: Per-device snapshots are create-only CAS and
  cannot be rewritten by a later scan/replay; snapshots are written even
  when the live projection is skipped.
  Evidence: scan.go writeSnapshot; `TestScanLifecycleAndSnapshots`,
  `TestProjectionSkipsProtectedDevices`.

- [ ] **CP-3 (AC-3, rule)**: The live device projection never overwrites
  manual, claimed (applyId), newer-scan, or concurrently-changed/deleted
  devices; existing TTL leases are preserved on update; new devices inherit
  the scan TTL.
  Evidence: scan.go projectDevice/projectable;
  `TestProjectionSkipsProtectedDevices`,
  `TestProjectionPreservesTTLAndUpdatesSameScanDevices`.

- [ ] **CP-4 (AC-4, rule)**: `GET /scans` and `GET /scans/devices` return
  immutable metadata/snapshots, filtered by subnet/scanId;
  `GET /devices` keeps its old shape with provenance fields optional.
  Evidence: api_scans.go, api_devices.go; TestAPIScansGet,
  TestAPIScanDevicesGet, TestAPIDevicesGetProvenanceFieldsOptional.

- [ ] **CP-5 (AC-5, rule)**: Preview returns per-device content hash, live
  device revision, and per-step create/mod revisions + action
  (create/keep/conflict/skip) and reason; it performs no writes.
  Evidence: api_applies_preview.go; TestApplyPreviewGreen (global etcd
  revision unchanged across call).

- [ ] **CP-6 (AC-6, rule)**: Preview classifies non-executable devices:
  missing reverse zone, missing MAC, newer-scan device, claimed device,
  missing roles. The request itself succeeds when roles are absent.
  Evidence: TestApplyPreviewReasons, TestApplyPreviewRolesUnavailable.

- [ ] **CP-7 (AC-7, rule)**: Apply creation validates finished scan and
  snapshot content hashes, requires exact planned step pins, and rejects
  invalid input with 4xx and no writes.
  Evidence: api_applies.go buildPinnedSteps; TestAPIAppliesCreateValidation.

- [ ] **CP-8 (AC-8, rule)**: The job executes target writes only while the
  device mod revision and target revisions still match pins, fenced by the
  job key revision (fencingGeneration).
  Evidence: apply_worker.go (stepDeviceMark + ensure events carry fenceKey/
  fenceRev); dhcp/event_ensure.go and dns/event_ensure.go Txn logic;
  TestEnsureDHCPLease stale-fence step, TestApplyWorkerConflictIsolation.

- [ ] **CP-9 (AC-9, rule)**: All four objects (device, lease, forward,
  reverse) carry the same applyId/scanId provenance; the device is marked in
  the same atomic claim flow and no unprovenanced half-set is created.
  Evidence: TestApplyWorkerFullExecution; Device/Lease/Record Discovery
  fields; device-mark claim gate (executeItem blocks target steps).

- [ ] **CP-10 (AC-10, rule)**: A blocked/changed step is reported per item
  and other independent items proceed; job ends partial with item/step
  details.
  Evidence: TestApplyWorkerConflictIsolation,
  TestApplyWorkerMissingReverseZonePartial.

- [ ] **CP-11 (AC-11, rule)**: Cancellation is CAS-based, observed at item/
  step boundaries; pending steps become cancelled; confirmed objects remain;
  there is no deletion of any DHCP/DNS object.
  Evidence: api_applies_cancel.go, finalizeCancellation;
  TestApplyCancelMultiDevice (confirmed keys exist, cancelled keys absent).

- [ ] **CP-12 (AC-12, rule)**: Restart/peer takeover: an interrupted running
  job with confirmed objects is recovered by a second role via the per-job
  etcd session lock; confirmed steps reconcile without rewrite and pending
  steps finish; object set stays unique.
  Evidence: apply_recovery.go (recoverApplies + 30s sweep),
  TestApplyRecoveryAfterRestart (mod revisions of pre-existing objects
  unchanged, reverse filled exactly once).

- [ ] **CP-13 (AC-13, rule)**: Concurrent different jobs targeting the same
  device: one applies, the other conflicts; object provenance is unique;
  same-job duplicate workers are lock-serialized.
  Evidence: TestApplyWorkerTwoJobsSameDevice; runApplyJob TryLock.

- [ ] **CP-14 (AC-14, rule)**: A new scan during/after an open job: the
  device-mark compares the live scanId/source revision and fails the item
  with device_changed_scan; job-scoped records are not deleted; later-created
  objects are preserved.
  Evidence: stepDeviceMark newer-scan branch;
  TestApplyWorkerConflictIsolation (device B newer-scan).

- [ ] **CP-15 (AC-15, rule)**: Without DHCP/DNS roles enabled the
  corresponding steps are skipped with role_unavailable and the job can
  complete; single target subsets work; missing scope/zone are conflicts
  (partial), not role skips.
  Evidence: TestApplyWorkerRolesUnavailableSkipped,
  TestApplyPreviewRolesUnavailable, worker skip mapping.

- [ ] **CP-16 (AC-16, rule)**: The legacy single-device
  `POST /devices/apply` and `DELETE /devices/delete` are unchanged and pass
  unmodified legacy tests.
  Evidence: api_devices.go untouched logic; TestDeviceApplyDHCP,
  TestDeviceApplyDHCPWithDNS, TestDeviceApplyDNSWithReverse,
  TestAPIDevicesDelete; legacy test files show no diff.

- [ ] **CP-17 (AC-17, rule)**: Duplicate submissions with the same
  idempotency key return the same apply job; exactly one job and one index
  key exist.
  Evidence: TestAPIAppliesCreateIdempotent.

- [ ] **CP-18 (AC-18, rubric)**: Conflict/skip taxonomy is stable at device
  and step level (reason codes), surfaced on preview and job status, and
  documented; reason codes are enums in discovery/types.
  Evidence: types/apply.go Reason* constants; docs reason table;
  tests assert each reason.

- [ ] **CP-19 (AC-19, rubric)**: Code quality: all claim writes are etcd
  transactions; no Delete against DHCP/DNS objects; key layout stays inside
  dhcp/dns packages via ensure events; new JSON fields omitempty; no import
  cycles; gofmt/vet clean; build passes; `-race` green for roles+discovery.
  Evidence: grep results in tasks.md Task 12; `go build`, `go vet`,
  `go test -race` outputs.

## Review History

### R1 (2026-10-13): fail

Two major and five minor findings (verbatim report archived in session):

- **M1**: retry's immediate-terminal path rewrote the job key without a
  lease, detaching retention (CP-17).
- **M2**: dhcp/dns ensure handlers could confirm a foreign incompatible
  object when the client echoed its current revisions (CP-8).
- **m3**: cancel CAS loss to a concurrent running transition returned
  success without writing cancelling.
- **m4**: cancellation window could leave a step `cancelled` while its
  object was already written with the job provenance.
- **m5**: reaper-written failed scan metadata carried no TTL lease.
- **m6**: nmap scanner construction failure left the scan stuck `running`.
- **n7**: DNS buildFQDN failure surfaced as a hard failed step; device-mark
  create revision was a placeholder.

All remediated in-place:

- M1 → terminal transitions now persist exclusively through
  `persistTerminalWithRetention` (commitTerminal); regression
  `TestApplyRetryImmediateTerminalKeepsLease`.
- M2 → ensure short-circuits to conflict when currentAction is conflict,
  before any transaction (dhcp + dns); regression
  `TestEnsureForeignObjectPinnedAtCurrentRevision`.
- m3 → cancel retries once with the fresh revision; terminalized races are
  returned as-is.
- m4 → `finalizeCancellation` reconciles pending steps (device + targets
  via dry-run) and confirms objects already owned by the job before marking
  the rest cancelled.
- m5 → reaper attaches the subnet's DiscoveryTTL lease.
- m6 → scanner construction failure now flips the scan to failed.
- n7 → buildFQDN failure is a `zone_missing` conflict; device-mark records
  the actual create revision.

Regression after remediation: all four packages pass sequentially,
`go test -race ./pkg/roles/discovery -count=3` stable, build/vet clean.

### R2 (2026-10-13): pass

Independent reviewer verified all seven R1 findings FIXED with code evidence
and dedicated regression tests; CP-8 and CP-17 re-checked PASS; no new
blocker/major/minor findings (three nits, one pre-existing gofmt nit outside
the diff). The two code-level nits were also addressed before closing
(unified zone_missing classification on the retry re-pin path, idempotent
cancel response for racing cancelling requests). Final verification:
build/vet clean, discovery/dhcp/dns/roles packages pass,
`go test -race ./pkg/roles/discovery -count=3` stable. A flaky pre-existing
IPAM parallel-allocation test (unrelated file) was observed once, passed on
re-run.

