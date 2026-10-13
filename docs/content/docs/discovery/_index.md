---
title: "Discovery Role"
weight: 12
description: Setup Gravity to discover existing devices on a network and safely bulk apply them to DHCP and DNS
---

Gravity can run [nmap](https://nmap.org/) ping scans of your network to
discover devices. Each discovery round produces an **immutable scan result**
with a stable identity. You can inspect the result, preview a still-executable
diff for a set of devices, and create an **apply job** that provisions a DHCP
reservation and forward/reverse DNS records per device through fenced,
idempotent transactions. Failures are tracked per device and per step; retries
only fill in the steps that never confirmed.

### Concepts

##### Subnets

Each subnet is a layer 3 network. For each subnet, Gravity initiates a
[`nmap`](https://nmap.org/)-based ping scan (mutually exclusive across nodes
via an etcd lock), stores immutable scan metadata and per-device snapshots,
and maintains a live "current devices" view.

##### Scans and snapshots

Every scan gets:

- a unique, stable `scanId`;
- the etcd **source revision** captured before scanning;
- `running`/`finished`/`failed` lifecycle metadata (the transition is
  compare-and-swapped exactly once);
- one immutable snapshot per observed device, carrying the hostname/IP/MAC and
  a content hash.

Snapshots and metadata expire with the subnet's `discoveryTTL` (default 24h).
They are never rewritten: replaying a scan keeps the original bytes and
revisions.

##### Devices (live view)

`/api/v1/discovery/devices` is the mutable latest view projected from scans.
The projection is deliberately conservative and never overwrites:

- devices marked as manually maintained;
- devices already claimed by an apply job (`applyId` set);
- devices produced by a newer scan (higher source revision);
- devices deleted or manually edited while the scan was running.

Snapshots are still recorded for every observation, regardless of whether the
projection was applied.

##### Applies and the preview

An apply references one immutable scan and a target set (`dhcp`, `dns`). The
workflow is:

1. `POST /api/v1/discovery/applies/preview` returns, per device, the pinned
   name/address/MAC/subnet plus the content hash, the live device key
   revision, and one diff entry per target step (`create`/`keep`/`conflict`/
   `skip`) with the target key and its current revisions. Preview is strictly
   read-only.
2. `POST /api/v1/discovery/applies` echoes the pinned items back. The server
   re-validates every snapshot hash. An optional `idempotencyKey` makes
   duplicate submissions return the same job.
3. The job executes asynchronously under a per-job distributed lock. Every
   object write is a compare-and-swap guarded by the job revision and the
   target revisions pinned at preview time.

The device mark is the claim gate: if a device cannot be claimed (it changed,
was deleted, is manual or already claimed, or a newer scan superseded it),
none of its DHCP/DNS objects are written. Other devices in the same job keep
processing independently.

##### Shared apply identity

The same provenance (`applyId`, `scanId`, source `discovery-apply`) is
recorded on all four objects of a successful item:

1. the live device (`applyId`/`applyStatus`);
2. the DHCP reservation (a lease with `expiry: -1`);
3. the forward DNS record;
4. the reverse DNS record.

Reservations created by an apply suppress the implicit DHCP→DNS event chain:
the job owns the DNS steps explicitly, so an object can never exist without
its job identity.

##### Step statuses and reasons

Each step is `pending`, `confirmed`, `skipped`, `conflict`, `failed` or
`cancelled`. Stable reason codes explain non-success:

| Reason | Meaning |
|---|---|
| `device_missing` | The live device key no longer exists. |
| `device_changed_manual` | The live device is manually maintained. |
| `device_changed_scan` | A newer scan superseded the pinned snapshot. |
| `device_claimed` | Another apply job already owns the device. |
| `target_changed` | The target object appeared/changed since the pin. |
| `target_owned_other` | An incompatible foreign object occupies the target key. |
| `already_provisioned` | The object already carries this job's identity (reconciled). |
| `job_not_claimable` | The job fencing revision is stale (cancel/retry/newer owner). |
| `scope_missing` / `zone_missing` | The DHCP scope / DNS zone does not resolve. |
| `mac_required` | A DHCP target has no MAC address. |
| `role_unavailable` | The DHCP/DNS role is not enabled on any node. |

A missing target role is an environment-level **skip** (the job can still
complete; only that step is skipped). Missing scope/zone or MAC is a
**conflict** (the job ends `partial` and a retry is possible after fixing
configuration).

##### Cancellation and retries

- `POST /api/v1/discovery/applies/cancel` moves the job to `cancelling`. The
  worker stops at step boundaries, marks remaining steps `cancelled` and
  never deletes confirmed objects.
- `POST /api/v1/discovery/applies/retry` is allowed for `partial`/`failed`/
  `cancelled` jobs. It bumps the fencing generation, re-validates every open
  step against the **current** store (new pins), freezes confirmed steps, and
  only executes what is still missing. Confirmed objects are never rewritten.
- Restarting or adding a node recovers open jobs automatically (immediate
  recovery plus a periodic sweep); the per-job etcd lock and session fencing
  ensure at most one node executes a job.

Terminal jobs (and their idempotency index) are retained for 30 days by
default, configurable via the discovery role config
(`applyRetentionSeconds`). Non-terminal jobs carry no expiry.

##### Legacy single-device apply

`POST /api/v1/discovery/devices/apply` keeps its original behavior for
conflict-free single devices: it dispatches the classic fire-and-forget
events, deletes the device after success, and does not create scan or job
records. The safe batch workflow above is used for all other applies.

### API summary

| Method & path | Purpose |
|---|---|
| `GET /api/v1/discovery/scans` | List immutable scan metadata (optional `subnet` filter). |
| `GET /api/v1/discovery/scans/devices?scanId=` | List immutable device snapshots. |
| `POST /api/v1/discovery/applies/preview` | Compute the still-executable, version-pinned diff. |
| `POST /api/v1/discovery/applies` | Create an apply job (optional `idempotencyKey`). |
| `GET /api/v1/discovery/applies` | List jobs (filter by `scanId`/`status`). |
| `GET /api/v1/discovery/applies/status?applyId=` | Get one job with items, steps and reasons. |
| `POST /api/v1/discovery/applies/cancel?applyId=` | Cancel a running/pending job. |
| `POST /api/v1/discovery/applies/retry?applyId=` | Re-pin and resume a partial/failed/cancelled job. |
