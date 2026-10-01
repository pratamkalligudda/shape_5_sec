# Live Polling API — Design

**Date:** 2026-09-20 (revised 2026-09-21)
**Status:** Design, pending review. No code written.
**Ticket:** Live polling session API (OQ-1 duration, OQ-3 cadence — SHAPE-869)
**Related:** `c:\Users\Pratam\Downloads\2026-09-15-fast-polling-feature.md` (gateway module contract)

---

## 1. Summary

Enable an on-demand, time-boxed "live polling" session for a single device. During a
session the gateway publishes telemetry every 1 second (vs. the normal 10-minute cadence)
for a bounded window. The data is displayed to the operator and is **never written to the
telemetry database**. Normal telemetry collection continues unaffected and its stored
samples remain equally spaced.

The gateway side already exists. This design covers the cloud side: the backend control
API, a new Event Grid-triggered function that reconstructs and broadcasts the live data,
the ephemeral store behind reload and graphing, and a cleanup timer.

---

## 2. What already exists

### 2.1 Gateway module (implemented, not ours)

`FastPollingService.cs` in the edge module exposes IoT Hub direct methods:

| Method | Behaviour |
|---|---|
| `FastPollingEnable` | Starts a session. Optional body `{ sessionDurationMinutes, pollIntervalSeconds }`. Missing → default (10 min / 1 s). Supplied but out of range → 400. Already active → 409. Returns `{ result, durationMinutes, intervalSeconds, sessionId, defaultsApplied }` |
| `FastPollingDisable` | Stops the active session. 409 if none active |
| `Extend-FastPolling` | Extends the session's **total** duration. Body `{ sessionId, extendSessionDurationMinutes }`, **both required**. Returns `{ result, sessionId, extendedByMinutes, totalDurationMinutes }` |

Constraints imposed by the module:

- `sessionDurationMinutes` — 10 to 30, default 10
- `pollIntervalSeconds` — 1 to 60, default 1
- One session per module at a time (`Interlocked.CompareExchange`)
- Auto-shutoff is local: `cts.CancelAfter(TotalDuration)`. **No cloud call stops a session**
- Twin is updated on start, manual disable, auto-expiry and a successful extend. Host shutdown
  and unhandled faults are logged but **not** reported to the twin
- Direct methods are not registered in `Development` — testing requires a real IoT Hub

**Extend semantics — the part easy to get wrong.** Duration is measured **from the original
start**, not from the moment Extend is called, and the module caps **total** duration at
30 minutes. A session starting at T=0 for 10 minutes, extended by 10 at T=5, ends at **T=20**
— not T=25. So the backend's new expiry is `StartedUtc + totalDurationMinutes` taken from the
module's response, never `ExpiresUtc + N`.

Check order is: field validation (400) → session id match (409) → 30-minute cap (400).
A `sessionId` that does not match the running session is a 409.

**Messages are batched and compressed.** Ticks are buffered five at a time and sent as one
gzip message to `output1`:

- App properties: `batchSentUtc`, `session-id`, `telemetry-type: fast-polling`
- `Message.ContentEncoding: "gzip"` — **the body must be gunzipped before it is JSON**
- Body is `{ "Entries": [ … ] }`, up to five entries, each with its own `TimestampUtc`
- Entry 0 of the **session's first batch** carries the full snapshot under `Body`;
  every other entry carries `Changes` — `{}` when nothing moved
- The full snapshot is **never resent** by the gateway mid-session
- A session ending mid-batch flushes a partial batch of 1–4 entries

So a consumer sees `[Body, Δ, Δ, Δ, Δ]` once, then `[Δ, Δ, Δ, Δ, Δ]` for the rest of the session.

**Superseded 2026-09-30:** the `Event` input has shipped. This paragraph previously claimed the
design degraded gracefully without it, which was wrong — `Event` is a sibling of `Body`/`Changes`,
and the parser reads only inside those two, so event state was dropped entirely rather than folded
as an unknown field. See [2026-09-30-live-polling-event-state-design.md](2026-09-30-live-polling-event-state-design.md).

**Measured payload size.** Built from the real 55 `TelemetryMeasures` field names with
representative values: snapshot JSON **1,598 bytes**. A re-materialised batch (§8) — one full
snapshot plus four deltas with their timestamps and envelope — is **~2.6 KB**.
Field *names* account for 1,109 bytes of the snapshot.

### 2.2 Cloud side (Phase 3, merged to `dev` 2026-09-17)

- Telemetry reaches the cloud via an **Event Grid system topic**; `data.properties`
  carries the message app properties (`IotTelemetryProcessorEventGrid.BridgeToEventData`)
- The ingestion write path is a single call — `_writer.WriteAsync` in
  `EventProcessingService` — persisting `TelemetryRaw`, `TelemetryMeasures`,
  `DeviceLatestTelemetry` and the blob archive together
- Live field names are exactly `TelemetryMeasures` columns / `ParameterRegistry.RegisterName`
- `TelemetryDailyRollup` / `TelemetryMonthlyRollup` aggregate from raw via `RollupWatermark`,
  so contamination corrupts `SampleCount`, `TransitionCount` and Min/Max — not merely adds rows
- Real-time push exists: Azure SignalR hub `iot_telemetry`, group
  `telemetry:device:{deviceGuid}`, target `telemetryUpdate`, joined via the negotiator's
  `join-device`
- **No Azure Table Storage anywhere in the monorepo.** Blob is used in four repos
- `IoTHubDeviceService.InvokeDirectMethodAsync` returns `bool`, discarding the module's
  status code and response body

---

## 3. Scope

**In scope** — backend control API; series endpoints; a new Event Grid-triggered function
that folds deltas and broadcasts; a cleanup timer; the ephemeral store; the
`LivePollingSession` table; Event Grid subscription filtering; RBAC.

**Out of scope** — the gateway module itself (IoT team); frontend implementation
(contract only); changes to the normal pipeline beyond the Event Grid filter.

---

## 4. Architecture

```
                    ┌──────────── BACKEND (control plane) ─────────────┐
 Browser ──REST───► │ POST   /devices/{id}/live-session        start    │
                    │ GET    /devices/{id}/live-session        status   │──► IoT Hub
                    │ POST   /devices/{id}/live-session/extend          │    direct methods
                    │ DELETE /devices/{id}/live-session        stop     │    FastPollingEnable
                    │ GET    /devices/{id}/live-session/series          │    Extend-FastPolling
                    │                                                   │    FastPollingDisable
                    │ SQL: LivePollingSession (1 row per session)       │
                    └───────────────────────────────────────────────────┘
                                          │
 Gateway module ──5 ticks/batch, gzip, telemetry-type: fast-polling──► IoT Hub ──► EG system topic
                                                                     │
       sub 1: data.properties.telemetry-type != 'fast-polling' ──► IotTelemetryProcessorEventGrid
                                                                    (unchanged, writes SQL)
       sub 2: data.properties.telemetry-type == 'fast-polling' ──► LiveTelemetryProcessorEventGrid
                                                                    │  NEW, in data-ingestion
                                                                    │  1. base64 → gunzip → Entries[]
                                                                    │  2. read !current
                                                                    │  3. fold each entry in order,
                                                                    │     skipping any not newer
                                                                    │  4. derive device status per entry
                                                                    │  5. ONE transaction: !current
                                                                    │     + every change point
                                                                    │  6. re-materialise batch:
                                                                    │     [snapshot, Δ, Δ, Δ, Δ]
                                                                    └─► SignalR iot_telemetry
                                                                        group  telemetry:device:{guid}
                                                                        target liveTelemetryUpdate
                                                                              │
 Browser ◄─────────── existing negotiator join-device ────────────────────────┘

 LivePollingCleanupTimer (NEW, data-ingestion) ── reads SQL for finished sessions
                                               └─ deletes their Table partition
```

**Writes to `TelemetryRaw`, `TelemetryMeasures`, `TelemetryEvents`, `DeviceLatestTelemetry`
from the live path: none.** Guaranteed by Event Grid subscription filters — the ingestion
function never receives a fast-polling message.

### 4.1 Why the fold happens server-side

The module sends a full baseline exactly once per session and deltas thereafter. A client
that reloads, opens a second tab, or joins at minute 8 has no baseline, and a field that
never changes is never sent — it would stay permanently blank. Something server-side must
hold the running snapshot.

### 4.2 Why a new function in `data-ingestion`, not a new app

Reuses `DeviceResolver` (gatewayId → deviceGuid), the SignalR output binding and the Event
Grid trigger plumbing. Cost: the app scales on total fleet telemetry volume, so per-instance
memory cannot hold fold state — hence the external store.

---

## 5. Storage design

### 5.1 Decision: Azure Table Storage, keyed by parameter

On the function app's existing `AzureWebJobsStorage` account.

The deciding requirement is **retrieval of one parameter's full history without reading
everything else** (§7.3). That only works if the parameter is part of the key. Blob has no
query at all; a time-keyed table would require a scan.

Secondary but decisive on cost — this workload is write-heavy and storage-light:

| Operation | Table (Standard LRS) | Blob (Hot LRS) |
|---|---|---|
| Write | **$0.00036 / 10K** | $0.055 / 10K — **153×** |
| Read | $0.00036 / 10K | $0.0044 / 10K |
| Data stored | $0.045 /GB/mo | $0.0208 /GB/mo |

*(West US pay-as-you-go, verified via the Azure retail prices API, 2026-09-21.)*

Rejected alternatives:

- **SQL table** — 1-2 writes/s/device on the shared 10 DTU database, which is the database
  whose sample spacing the acceptance criteria measure
- **Function memory** — `data-ingestion` scales out; ticks would fold against a baseline the
  instance never saw, producing silently wrong values
- **Blob** — no query, and 153× the operation cost on a write-heavy path
- **Cosmos DB** — native TTL and server-side projection, but a new resource, SDK and ops
  surface for a diagnostic feature
- **Frontend `sessionStorage` alone** — cannot serve a second viewer or a new tab

**Note:** Table Storage is new to this monorepo. That is a real cost (new SDK, new test
doubles, no precedent) and was weighed against the above.

### 5.2 Layout

One table, one partition per session.

```
PartitionKey = sessionId
  RowKey "!current"                -> folded snapshot + session metadata   [upsert, ETag]
  RowKey "{Param}|{timestamp:o}"   -> one change point                     [insert]
```

`!` (0x21) sorts before every letter, so the head row is always first and can never collide
with a parameter prefix range.

**Parameter rows** carry `V` (natively typed — Boolean / Int64 / Double / String) and `T`
(a type discriminator so readers need not guess). `DeviceStatus` is modelled as just another
series, giving the series endpoint one code path instead of two.

**`!current`** holds `DeviceGuid`, `DeviceId`, `Snapshot` (JSON ~1.6 KB), `DeviceStatus`,
`LastSnapshotTimestampUtc`, `PollIntervalSeconds`, `TickCount`, `BytesTotal`, `FirstTickUtc`,
`LastTickUtc`, `ExpiresUtc`.

**Per batch — not per tick — one read and one entity group transaction.**

```
read   (PK, "!current")
batch  [ upsert (PK, "!current")  If-Match: etag        state after the last entry
         insert (PK, "OperatingPressurePSI|{ts of entry 1}")
         insert (PK, "StrokeRateLeft|{ts of entry 1}")
         insert (PK, "OperatingPressurePSI|{ts of entry 3}")
         insert (PK, "DeviceStatus|{ts of entry 4}")    only where status moved
         …                                              every change in all five entries ]
```

**2 operations per five ticks**, regardless of how many fields moved. All five entries fold in
order, then commit together: the head row ends at the state after entry 4, and every change
point from every entry is written in the same transaction. Because everything shares one
partition, the snapshot and its change points commit atomically — there is no window in which
a delta is recorded but never folded.

A heartbeat entry writes no parameter rows; only `TickCount` and `LastTickUtc` advance.

**Transaction size.** The entity group transaction limit is 100 entities. The worst case is the
session's first batch: 55 baseline parameters + up to 4 deltas' changes + a status row + the head
≈ **65 entities**. Within the limit, but close enough that a test asserts it rather than assuming.

**Guards.** The ETag makes concurrent folds safe: a loser gets 412 and retries against fresh
state. Entries are folded in `TimestampUtc` order and any entry not newer than
`LastSnapshotTimestampUtc` is skipped — so a redelivered batch that overlaps the stored state
applies only its genuinely new entries, and a fully duplicate batch applies nothing. A batch
whose `session-id` does not match the partition starts a new session rather than folding onto
the old one.

### 5.3 Verified platform limits

From *Scalability and performance targets for Table storage* (Microsoft Learn):

| | |
|---|---|
| Max request rate per storage account | 20,000 transactions/s (assumes 1 KiB entities) |
| Target throughput, single partition | 2,000 entities/s (1 KiB entities) |
| Max entity size | 1 MiB; max 255 properties |
| Entity group transaction | ≤100 entities, <4 MiB, single PartitionKey |
| PartitionKey / RowKey | ≤1,024 characters each |

These are **targets, not hard quotas**. On exceeding them Azure Storage returns **503 Server
Busy** or **500 Operation Timeout**. The storage client must therefore use **exponential
backoff retry** — a batch losing to a throttle should back off, not fail the invocation.

The session's first batch writes ~65 entities (55 baseline parameters, up to 4 deltas' changes,
a status row and the head), within the 100-entity EGT limit.

### 5.4 Volume

| | |
|---|---|
| First batch | ~65 rows |
| Later batches | ~20 rows (only what changed, across five entries) |
| 10-minute session | ~2,500 rows, ~0.5 MB, written in 120 transactions |
| 10 concurrent sessions | ~5 MB resident — the entire footprint at any instant |

Row keys dominate: `OperatingPressurePSI|2026-09-20T10:27:02.2180000Z` is 49 characters
against an 8-byte value.

### 5.5 Cleanup — no retention window

Session data exists only for the life of the session. **A reload after a session ends shows
nothing**; there is no post-session review. This was chosen deliberately, not for cost —
a 60-minute window would have cost roughly $0.05/year.

`LivePollingCleanupTimer`, a new timer function in `data-ingestion` on a configurable
schedule:

```
SELECT SessionId FROM LivePollingSession
 WHERE PurgedUtc IS NULL
   AND (StoppedUtc IS NOT NULL OR ExpiresUtc < SYSUTCDATETIME())

for each: read !current, copy TickCount/BytesTotal into the SQL row,
          delete the partition (batch 100 per transaction),
          stamp PurgedUtc
```

**It finds its work in SQL, never by scanning Table Storage.** Table has no secondary
indexes, so querying by `ExpiresUtc` against the table itself would be a full scan on every
run. The SQL row already holds the expiry, so the timer gets an exact list of partition keys.

This also covers the two exit paths nothing else reports — auto-expiry and module fault.

Pattern to copy: `BlobSweeperTimer` / `BlobReconcileTimer` in `process-telemetry`.

---

## 6. Session lifecycle

### 6.1 Who stops a session

**The gateway owns auto-shutoff. The backend owns manual stop.** The backend must never send
`FastPollingDisable` at expiry — the module has already stopped itself, and a Disable then
races with any session that has since started.

| Exit path | Who acts | Backend behaviour |
|---|---|---|
| Auto-expiry | Module (`cts.CancelAfter`) | Nothing. The row is treated as closed once `ExpiresUtc` passes, evaluated on read; the cleanup timer purges storage |
| Manual stop | Backend → `FastPollingDisable` | Set `StoppedUtc`, `StopReason = ManualStop` |
| Host shutdown | Module, silently | Row lapses at `ExpiresUtc`; timer cleans up |
| Unhandled fault | Module, silently | Same |

Auto-shutoff keeps working if the backend is down. The worst case is stale bookkeeping,
never a gateway left polling indefinitely.

### 6.2 Concurrency gating

One session per device, enforced by `LivePollingSession`. A global `MaxConcurrentSessions`
cap bounds fleet-wide wireless cost and SignalR load — at the expected scale this is the
cost governor, not a technical limit.

The module's own 409 is the backstop; the backend gate fails fast and returns a specific,
actionable error.

Expiry is evaluated lazily on read, so a session that ended without telling anyone never
blocks a new start.

---

## 7. Backend API

Versioned routes, `ApiResponse<T>` envelope, `[RequirePrivilege("LivePolling")]`.

| Endpoint | Purpose |
|---|---|
| `POST /api/v1/devices/{deviceId}/live-session` | start |
| `GET /api/v1/devices/{deviceId}/live-session` | status + current snapshot |
| `POST /api/v1/devices/{deviceId}/live-session/extend` | extend |
| `DELETE /api/v1/devices/{deviceId}/live-session` | stop |
| `GET /api/v1/devices/{deviceId}/live-session/series` | metric series for the graph |

### 7.1 Start

Body optional; both fields fall back to configuration.

```json
{ "pollIntervalSeconds": 1, "sessionDurationMinutes": 15 }
```
```json
{ "success": true,
  "data": { "sessionId": "cc27db6b-...", "deviceId": "PUMP-00417",
            "pollIntervalSeconds": 1, "sessionDurationMinutes": 15,
            "startedUtc": "2026-09-20T10:27:00Z",
            "expiresUtc": "2026-09-20T10:42:00Z",
            "extensionsRemaining": 2 } }
```

The backend clamps configured values into the module's ranges before calling, so an
out-of-range configuration surfaces as a clear error from us rather than an opaque module
rejection.

### 7.2 Status

Serves reload. Session metadata from SQL, `snapshot` from the `!current` row. No gateway
call, no telemetry-database read.

```json
{ "success": true,
  "data": { "active": true, "sessionId": "cc27db6b-...",
            "pollIntervalSeconds": 1, "expiresUtc": "...", "secondsRemaining": 412,
            "extensionsRemaining": 1, "deviceStatus": "Warning",
            "snapshot": { "MotorRunning": true, "OperatingPressurePSI": 54372 },
            "snapshotTimestampUtc": "...",
            "tickCount": 412, "bytesTotal": 97644 } }
```

### 7.3 Series

```
GET .../live-session/series                               -> the configured default metrics
GET .../live-session/series?metric=OperatingPressurePSI   -> one metric
GET .../live-session/series?since=2026-09-20T10:31:00Z    -> incremental catch-up
```

Served by `RowKey` prefix range within the session partition:

```
PartitionKey eq '{sid}'
  and RowKey ge 'OperatingPressurePSI|'
  and RowKey lt 'OperatingPressurePSI}'      -- '}' is '|' + 1
```

The `|` delimiter is what keeps `StrokeRate` from also matching `StrokeRateLeft` and
`StrokeRateRight`. The default set runs four such ranges in parallel. No fold-forward, no
whole-session read.

Default metrics come from `LivePolling:DefaultSeriesMetrics` (four, to be decided), so the
choice is configuration rather than a release. Names are validated against
`ParameterRegistry.RegisterName`; an unknown name returns 400 listing valid names rather
than an empty graph.

Response is columnar, with `t` and `status` shared across metrics rather than repeated:

```json
{ "sessionId": "cc27db6b-...", "pollIntervalSeconds": 1,
  "startedUtc": "...", "lastTickUtc": "...", "tickCount": 432,
  "t":      ["10:27:01Z", "10:27:02Z", "10:27:03Z"],
  "status": ["Normal", "Normal", "Warning"],
  "series": [ { "metric": "OperatingPressurePSI", "v": [54351, 54360, 54360] } ] }
```

Change points are step-expanded onto the tick grid — the value carries forward when a tick
did not change it — so the series is evenly spaced with no gaps. Four metrics over 1,800
ticks is ~116 KB; ~75 KB if timestamps become millisecond offsets from `startedUtc`.

**Available only while the session is live** (see §5.5).

### 7.4 Device status

Device-level, not per-metric. Derived per tick from the folded snapshot and stored as its
own series:

```
Alarm   if OperatingAlarmActive > 0 or MaintenanceAlarmActive > 0
Warning else if OperatingWarningActive > 0 or MaintenanceWarningActive > 0
Normal  otherwise
```

These fields are already in the payload, so this needs no SQL, no alert-definition cache and
no enrichment. The live path keeps zero database dependencies.

### 7.5 Error contract

Every failure carries a machine-readable `code`. No generic 500s for expected conditions.

| Code | HTTP | When |
|---|---|---|
| `LIVE_SESSION_ALREADY_ACTIVE` | 409 | device already has a session; body carries `sessionId`, `expiresUtc` |
| `LIVE_SESSION_CAPACITY_REACHED` | 409 | global `MaxConcurrentSessions` reached |
| `LIVE_SESSION_NOT_ACTIVE` | 409 | extend/stop with no active session |
| `LIVE_SESSION_EXTENSION_LIMIT_REACHED` | 400 | the module's 30-minute total-duration cap would be breached; echoes its `reason` |
| `LIVE_SESSION_INVALID_PARAMETERS` | 400 | outside 10-30 / 1-60; echoes the module's `errors` |
| `DEVICE_NOT_FOUND` / `GATEWAY_NOT_PAIRED` | 404 | |
| `GATEWAY_UNREACHABLE` | 503 | direct method timed out or device offline |

### 7.6 Required backend change

`IIoTHubDeviceService.InvokeDirectMethodAsync` returns `bool`, discarding status code and
response body — so 409 (already active) cannot be told from 400 (bad range), and `sessionId`
cannot be read. Add a method returning status + payload, leaving the existing signature
intact for current callers.

---

## 8. SignalR contract

- Hub `iot_telemetry`, group `telemetry:device:{deviceGuid}` — both already in use
- **New target `liveTelemetryUpdate`**, distinct from `telemetryUpdate`

A distinct target on the existing group means no negotiator change and no chance of existing
consumers mistaking live data for regular telemetry.

**One message per batch, and every batch leads with a full snapshot.** The gateway sends the
baseline once per session; the backend re-materialises it at the head of every batch from the
state it already holds:

```json
{ "sessionId": "cc27db6b-…", "deviceGuid": "…", "batchSentUtc": "…",
  "entries": [
    { "timestampUtc": "10:27:01.220Z", "deviceStatus": "Normal",
      "snapshot": { "…all 55 fields…" } },
    { "timestampUtc": "10:27:02.218Z", "deviceStatus": "Normal",
      "changes": { "OperatingPressurePSI": 54360 } },
    { "timestampUtc": "10:27:03.123Z", "deviceStatus": "Normal",  "changes": {} },
    { "timestampUtc": "10:27:04.201Z", "deviceStatus": "Warning",
      "changes": { "StrokeRateLeft": 97, "OperatingWarningActive": 1 } },
    { "timestampUtc": "10:27:05.190Z", "deviceStatus": "Warning", "changes": {} } ] }
```

The frontend does one thing per batch: take entry 0 as truth, apply ntries 1-4 in order, render
five frames.

**Why re-materialise rather than forward the deltas as they arrive.** SignalR does not guarantee
delivery. Forwarding deltas only means one lost batch leaves a field wrong on screen indefinitely,
with nothing to signal it; leading every batch with a snapshot bounds that to five seconds. It also
removes the special case for the session's first batch and the separate bootstrap path a reloading
or late-joining client would otherwise need — the stream alone is sufficient.

**The message is gzip-compressed.** The batch shown above is what the client sees *after* decoding.
On the wire it is an opaque envelope:

```json
{ "encoding": "gzip+base64", "payload": "H4sIAAAAAAAA..." }
```

The client decodes with the browser's native `DecompressionStream("gzip")` — no library, but it
requires Chrome 80+, Safari 16.4+ or Firefox 113+. Accepted: the console is internal and modern.

| | bytes | SignalR chunks |
|---|---|---|
| Batch JSON, uncompressed | 2,260 | 2 |
| gzip | 760 | — |
| gzip + base64, with envelope | ~1,050 | **1** |

SignalR bills outbound messages in 2 KB chunks, so this halves the message count on every batch.
At the expected volume the saving is small in absolute terms, and the cost is real: a live message
is no longer readable in browser devtools or Azure SignalR live trace without decoding it, on a
feature whose purpose is diagnostics. Taken deliberately.

Two tests hold the line — the decoded batch under 4,096 bytes, and the compressed envelope under
2,048. The second is the one that asserts the compression is still buying anything.

Compact serialisation, minimal envelope, no pretty-printing on this path.

Frontend keeps `sessionStorage` as an instant-redraw cache. The server is authoritative for
anyone not present from the first tick.

---

## 9. Session record — `LivePollingSession`

New SQL table. Checked 2026-09-21: no existing table fits — `AuditLog`, `AgentAuditLog`,
`EulaConsentAudit`, `DeploymentHistory`, `SchemaChangeLog` and `TelemetryRetentionLog` are
all append-only event logs with no session lifecycle shape.

| Column | Type | Purpose |
|---|---|---|
| `SessionId` | uniqueidentifier PK | module's session GUID |
| `DeviceGuid` | uniqueidentifier | |
| `DeviceId`, `GatewayId` | nvarchar | |
| `StartedUtc` | datetime2 | |
| `StartedByUserId` | nvarchar(300) | from `ICurrentUserService.PrincipalId` |
| `PollIntervalSeconds` | int | as accepted by the module |
| `SessionDurationMinutes` | int | |
| `ExpiresUtc` | datetime2 | drives gating **and** cleanup |
| `ExtensionsUsed` | int | bounded by `MaxExtensions` |
| `LastExtendedUtc` | datetime2 null | |
| `StoppedUtc` | datetime2 null | manual stop only |
| `StopReason` | varchar(20) null | `ManualStop` / `AutoExpiry` / `Unknown` |
| `TickCount` | int null | harvested by the cleanup timer before purge |
| `BytesTotal` | bigint null | harvested by the cleanup timer before purge |
| `PurgedUtc` | datetime2 null | cleanup watermark |

Indexes: `(DeviceGuid, ExpiresUtc)` for gating; filtered index on `PurgedUtc IS NULL` for
the cleanup query.

**Three jobs in one table:** concurrency gating, the cleanup worklist, and a permanent
record of who ran what with which settings — including the tick and byte counters, which
survive the deletion of the ephemeral partition and supply the wireless-cost evidence.

Writes: ~3 per session (insert at start, update on extend, update at stop/purge). Nothing
per tick.

**No `AuditLog` rows.** Consequence: a **rejected** start creates no session row, so
rejections are recorded in Application Insights only (device, user, reason) — not queryable
from SQL.

Per-tick diagnostics — guard rejections, SignalR failures, fold latency, storage throttling
— go to Application Insights tagged with `sessionId`. Nothing per-tick reaches SQL.

---

## 10. RBAC

Existing privileges: `PlatformAccess`, `DeviceProvisioning`, `DeviceEditing`,
`DeviceDeletion`, `Diagnostics`, `ManageConsent`, `ManageRoles`, `ImportAccounts`,
`ConfigureDeviceModels`. There is no Service / Engineering / Admin *role* concept in code.

Add a `LivePolling` privilege plus a seed migration mapping it to the `Service`, `Engineering`
and `Admin` roles — all three confirmed present in `dbo.PlatformRoles` on 2026-09-21.

---

## 11. Configuration

All runtime-changeable, satisfying the requirement that cadence and duration be adjustable
once OQ-1 and OQ-3 are answered.

```
LivePolling:PollIntervalSeconds        # OQ-3. Clamped to module range 1-60
LivePolling:SessionDurationMinutes     # OQ-1. Clamped to module range 10-30
LivePolling:ExtensionMinutes           # minutes requested per extend call
LivePolling:MaxConcurrentSessions      # cost governor
LivePolling:DefaultSeriesMetrics       # four metric names, TBD
LivePolling:ModuleId
LivePolling:DirectMethodTimeoutSeconds
LivePolling:CleanupSchedule            # NCRONTAB for the cleanup timer
```

**There is deliberately no `MaxExtensions`.** The module enforces a 30-minute cap on total
duration and returns a 400 naming the numbers when an extension would breach it. Duplicating
that as a backend counter would give two rules that can disagree — and the module's is the one
that actually stops the gateway. The backend surfaces its rejection instead.

---

## 12. Event Grid routing and rollout order

One system topic, two subscriptions:

```
sub 1 (existing): data.properties.telemetry-type  StringNotIn  ['fast-polling']
                    -> IotTelemetryProcessorEventGrid
sub 2 (new):      data.properties.telemetry-type  StringIn     ['fast-polling']
                    -> LiveTelemetryProcessorEventGrid
```

Event Grid delivers to **every** subscription whose filter matches. Without the filter on
subscription 1, fast-polling messages reach both the new function and the ingestion pipeline.

**Rollout order is load-bearing:**

1. Deploy the filter on subscription 1
2. Verify with a real message that regular telemetry (no `telemetry-type` property at all)
   still matches subscription 1
3. Create subscription 2 and deploy the new function
4. Only then enable fast polling on any gateway

Reversing 1 and 4 contaminates `TelemetryRaw`, and through `RollupWatermark` that propagates
into rollup aggregates and daily usage — corrupted `SampleCount`, `TransitionCount` and
Min/Max, not merely extra rows.

---

## 13. Cost

West US pay-as-you-go retail, verified via the Azure retail prices API on 2026-09-21.
Table Standard LRS: **$0.00036 per 10K operations**, **$0.045 per GB/month**.

Per session (10 min @ 1 s = 600 ticks = **120 batches**, 2 ops per batch): **~241 operations,
~0.5 MB**. Batching cut the operation count fivefold.

| Usage | Ops/year | Op cost/year | Storage |
|---|---|---|---|
| 10 sessions/day (expected) | 0.9 M | **$0.03** | ~$0.003 |
| 480/day (10 devices busy 8 h) | 42 M | **$1.52** | ~$0.003 |
| 1,440/day (10 devices saturated 24/7) | 127 M | **$4.56** | ~$0.003 |

Storage never accumulates — peak resident is 10 concurrent × 0.5 MB ≈ 5 MB.

Incremental IoT Hub cost at expected usage is **$0** — 6,000 messages/day against a
400,000/day S1 unit already paid for. SignalR at one chunk per tick is likewise absorbed.

**Cellular data is the only material cost**: ~120 KB of payload per session before protocol
overhead. That is the figure the ticket requires to be measured on a real gateway.

Every figure scales roughly linearly with **average parameters changing per tick**, assumed
here to be 4 of 55. See §16.

---

## 14. How the acceptance criteria are met

| Criterion | Mechanism |
|---|---|
| Start, poll, extend, stop implemented and documented | §7. "Poll" is served by `GET /live-session` (current) and `/series` (history); the 1-second polling is gateway-side and live data reaches the browser by SignalR push |
| Concurrent conflict returns a specific error, not a 500 | `LIVE_SESSION_ALREADY_ACTIVE` (§7.5) |
| Auto-shutoff at the configured duration | Module-local `cts.CancelAfter`; duration is configuration (§11) |
| Auto-shutoff testable without waiting | Backend expiry logic driven by an injected `TimeProvider`; end-to-end timing against a real gateway is §16 |
| Extension resets the timer, bounded | `Extend-FastPolling`; bounded by the module's 30-minute total-duration cap (§2.1, §11) |
| Live data absent from the telemetry database | Event Grid filtering (§12) — the ingestion function never receives the message. Verified by querying `TelemetryRaw`, `TelemetryMeasures`, `DeviceLatestTelemetry` after a session |
| Regular sampling unaffected, samples equally spaced | Separate subscription; the live path issues zero SQL writes on the hot path |
| Wireless cost measured and documented | `TickCount` / `BytesTotal` harvested into `LivePollingSession` before purge (§9) |
| Tested against a real gateway in dev | §16 — the module does not register its direct methods in `Development` |

---

## 15. Testing

**Unit** — delta folding (baseline, delta, heartbeat, out-of-order, duplicate, session-id
mismatch); device status derivation; parameter clamping; expiry via `TimeProvider`; error-code
mapping for each module response; **compressed SignalR envelope under 2,048 bytes** and decoded batch under 4,096; gzip + base64 encoding and decoding,
including a corrupt body; folding a five-entry batch in order; a partial batch of 1-4 entries;
a redelivered batch that overlaps stored state.

**Integration** — session gating under concurrent starts; extend rejection at the module's cap;
EGT atomicity and **entity count for a first batch staying under 100**; `RowKey` prefix-range correctness, explicitly
covering `StrokeRate` vs `StrokeRateLeft`; status and series endpoints against a seeded
partition; cleanup timer deletes only finished sessions and harvests counters first;
retry/backoff on simulated 503.

**Non-persistence** — run a session, then assert no new rows in `TelemetryRaw`,
`TelemetryMeasures`, `TelemetryEvents` and no change to `DeviceLatestTelemetry`.

**Sample spacing** — capture regular-telemetry timestamps across a live session and assert
intervals are unchanged.

---

## 16. Open verification items

1. **Event Grid filter on an absent property** — regular telemetry carries no
   `telemetry-type`. Confirm `StringNotIn ['fast-polling']` still matches it; if absent keys
   do not match, subscription 1 needs an explicit "key does not exist" clause
2. **Advanced-filter key syntax** — confirm `data.properties.telemetry-type` resolves with
   the hyphen in the key
3. **Average parameters changing per tick** — assumed 4 of 55. Every cost, row-count and
   transaction-size figure scales off it; measure on the dev gateway
4. **Module id** of the telemetry module hosting `FastPollingService` (existing calls use
   `WatchdogModule`, a different module)
5. **How Event Grid presents a gzip body** — whether `data.body` arrives base64-encoded, and
   whether `ContentEncoding` survives into `data.properties` or `data.systemProperties`.
   The function must gunzip before parsing; confirm the exact shape against a real message
6. **Entity group transaction billing granularity** — one transaction or one per entity.
   Unconfirmed; does not change any decision, both figures are under a dollar a year
7. **Wireless data cost** of a representative session, measured on a real dev gateway.
   Gzip means the on-wire figure is now compressed — measure the compressed size, which is
   what the cellular plan is billed for
8. **End-to-end auto-shutoff** timing against a real gateway

**Resolved during design:** role names (`Admin`, `Engineering`, `Service` all exist in
`dbo.PlatformRoles`, confirmed 2026-09-21) and the extend contract (`Extend-FastPolling`,
documented in §2.1 as of the 2026-09-25 gateway revision).

---

## 17. Risks and constraints

| Risk | Mitigation |
|---|---|
| Event Grid filter edited or lost in the portal ⇒ rollup contamination | Startup assertion in the new function that it only receives `telemetry-type: fast-polling`; log loudly otherwise |
| Event Grid unordered, at-least-once | ETag on the `!current` upsert plus a per-entry `TimestampUtc` guard; a redelivered batch applies only its genuinely new entries |
| Storage throttling (503/500) | Exponential backoff retry required on the storage client (§5.3) |
| Compressed batch crossing 2 KB | Test asserts <2,048 bytes on the compressed envelope, and <4,096 on the decoded batch |
| Corrupt or undecodable gzip body | Log and drop the batch rather than throw — a poison message that fails the invocation is redelivered forever |
| Five-second batch latency | Accepted. The lever is the gateway's `BatchSize`, not ours |
| Backend restart mid-session | `LivePollingSession` is in SQL, so control survives; the fold partition is untouched |
| `data-ingestion` scale-out | Fold state is external by design; no instance affinity assumed |
| Cleanup timer never runs | Partitions leak. Alert on `LivePollingSession` rows with `PurgedUtc IS NULL` older than a threshold |
| No post-session review | Accepted. Data exists only while the session is live (§5.5) |
| Rejections not in SQL | Accepted. Application Insights only (§9) |
| Table Storage is new to the monorepo | New SDK and test doubles; weighed in §5.1 |

---

## 18. Decisions taken during design

| Decision | Rationale |
|---|---|
| Separate Event Grid subscription rather than a code filter | Contamination becomes impossible by construction rather than by code discipline |
| Fold server-side, not in the client | The baseline is sent once per session; a late joiner or new tab would otherwise never be complete |
| Every batch leads with a re-materialised full snapshot | SignalR does not guarantee delivery: forwarding deltas alone means a lost batch corrupts the view indefinitely, while a snapshot per batch bounds it to five seconds. Also gives the frontend one code path and removes the bootstrap call on reload |
| Table Storage, keyed by parameter | Only shape that answers "one parameter's history" without a scan; 153× cheaper per operation than blob on a write-heavy path (§5.1) |
| One entity group transaction per batch | 2 ops per five ticks, and atomicity removes the "recorded but never folded" failure mode |
| No backend extension counter | The module caps total duration at 30 minutes and rejects with a 400 naming the numbers. A second counter in the backend could disagree with the one that actually stops the gateway |
| No retention window | Chosen for product reasons, not cost; a 60-minute window would have cost ~$0.05/year |
| Cleanup timer reads SQL, not Table | Table has no secondary indexes; querying by `ExpiresUtc` against the table would scan on every run |
| `LivePollingSession` only, no `AuditLog` rows | Single system of record; rejections go to Application Insights |
| Device-level status, not per-metric | Derivable from the payload; per-metric would add an alert-definition SQL dependency to a path that has none |
| One session per device, global cap in config | The ticket requires per-device; the global cap bounds fleet-wide wireless cost |
