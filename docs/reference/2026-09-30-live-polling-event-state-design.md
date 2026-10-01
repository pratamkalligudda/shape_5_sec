# Live Polling — Event State Design

**Date:** 2026-09-30
**Status:** approved, not yet implemented
**Amends:** [2026-09-20-live-polling-api-design.md](2026-09-20-live-polling-api-design.md)

## 1. Why

The gateway now ships the `Event` input described as "planned, not implemented" in the original
design. Our parser reads only `Body` and `Changes`, and `Event` sits beside them at entry level, so
today every event transition is **silently dropped** — it reaches neither storage, nor the series
endpoint, nor the browser.

The live chart needs two things the current contract cannot supply:

- **Event markers on the time axis** — the triangle/flag glyphs in the Live Mode mock: Motor On,
  Motor Off, Warning, Warning Cleared, Alarm, Alarm Cleared.
- **A human label for operational events** — a tooltip reading "Operational Alarm Triggered:
  Overstroke Alarm", not a bare register value of `6`.

This document covers only event state. Everything else in the original design stands.

## 2. What the gateway sends

`Event` is a **sibling of `Body` and `Changes`**, never nested inside them.

| Entry kind | `Event` present | Contents |
|---|---|---|
| Baseline (`Body`) | always, possibly empty | full event-state snapshot |
| Delta (`Changes`) | only on ticks where event state moved | just what moved |
| Heartbeat | absent | — |

Keys are parameter names identical to those in the telemetry payload; values are the raw register
values. Confirmed with the IoT team 2026-09-30 — the `"triggered"` / `"cleared"` string form that
appeared in an earlier doc revision is not what ships:

```json
{ "TimestampUtc": "2026-09-28T08:00:35Z",
  "Changes": { "OperatingPressure": 28000 },
  "Event":   { "OperatingAlarmActive": 5 } }
```

The gateway performs the edge detection. We do not diff, and we do not keep a hardcoded list of
alert parameters — **presence of a key inside `Event` is what makes it an event.**

## 3. What the database gives us

Two tables, both effectively static.

`dbo.DeviceAlertDefinitions` — 18 rows, keyed for our purposes by `ParameterName`:

| ParameterName | AlertCode | EventType | Domain | Severity |
|---|---|---|---|---|
| `MotorRunning` | 40001 | status | operational | minor |
| `OperatingAlarmActive` | 40006 | alarm | operational | critical |
| `OperatingWarningActive` | 40007 | warning | operational | major |
| `MaintenanceAlarmActive` | 40008 | alarm | maintenance | critical |
| `MaintenanceWarningActive` | 40009 | warning | maintenance | major |
| `OilChangeAlarm`, `OilFilterAlarm`, … | 40301–40413 | alarm/warning | maintenance/operational | critical/major |

`dbo.DeviceAlertValueDefinitions` — 57 rows, `AlertDefinitionId` + `ValueCode` → `ValueLabel`. Rows
exist for **exactly two** definitions, 40006 and 40007:

```
OperatingAlarmActive     0 -> "No Fault Active"          (26 rows total)
OperatingAlarmActive     5 -> "Emergency Stop Active"
OperatingAlarmActive     6 -> "Overstroke Alarm"
OperatingWarningActive   0 -> "No Warning Active"        (31 rows total)
OperatingWarningActive  21 -> "Left Overstroke Warning"
```

Three consequences drive the design:

1. **`ValueCode 0` is the cleared state.** "No Fault Active" / "No Warning Active". There is no
   separate triggered/cleared field on the wire and none is needed: `0` → cleared, non-zero →
   triggered.
2. **Only the two operational-domain parameters carry labels.** The other 16 are booleans where
   `1` is triggered and `0` is cleared; they get no label.
3. **`EventType`, `Domain` and `Severity` are available for all 18** from the one definition row,
   at no extra cost once that table is cached.

## 4. Level versus edge

The two outputs are different in kind and stay separate.

| | Source | Cardinality | Where it goes |
|---|---|---|---|
| **`DeviceStatus`** — level | folded snapshot (`Body`/`Changes`) | one value per grid point | `status[]`, unchanged |
| **Events** — edge | `Event` block | a handful per session | `events[]`, new |

`DeviceStatusDeriver` is **not changed**. It keeps reading `OperatingAlarmActive`,
`MaintenanceAlarmActive`, `OperatingWarningActive` and `MaintenanceWarningActive` out of the folded
snapshot. `MotorRunning` is `EventType = status` and must never affect it.

One breaking change to the already-documented contract: device status becomes `int` (`0` Normal,
`1` Warning, `2` Alarm) instead of the string `"Normal"/"Warning"/"Alarm"`, on **every** channel —
`status[]` on the series endpoint, `deviceStatus` on the status endpoint, and
`entries[].deviceStatus` on SignalR. A 600-point array of strings is wasteful, and a status that is
an integer on one channel and a string on another is a silent mismatch waiting to happen: a client
comparing `deviceStatus === 2` would simply never match. The strings remain internal to the folded
snapshot. `deviceStatus` on the status endpoint stays nullable, because "has not reported yet" is a
different statement from Normal.

## 5. Where resolution happens

**In the ingestion function, once, at fold time.** Resolved events are stored resolved; the
series endpoint serves what was stored. The backend gains no alert knowledge and live and reload
cannot drift apart.

### 5.1 A new catalog, not the existing `AlertCacheService`

Correcting an earlier call in the design conversation: we reuse the *query*, not the class.
`ShapeIot.Ingestion.Infrastructure.AlertCacheService` is wrong for this path on two counts — it is
keyed by `int` AlertCode where we hold a parameter name, and every method demands a caller-supplied
open `SqlConnection`, which the live path does not have. Adapting it would change an interface the
regular telemetry pipeline depends on, for no gain.

New `LivePolling/AlertDefinitionCatalog.cs`, modelled on the backend's
`ParameterRegistryMetricCatalog`: opens its own connection, loads **once**, holds for the life of
the process, does not cache a failed load.

```csharp
public sealed record AlertDefinition(
    string ParameterName, int AlertCode, string EventType, string Domain,
    string Severity, string Description, IReadOnlyDictionary<int, string>? ValueLabels);

public interface IAlertDefinitionCatalog
{
    Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default);
}
```

One query, both tables, 18 + 57 rows:

```sql
SELECT d.AlertDefinitionId, d.AlertCode, d.ParameterName, d.EventType, d.Domain,
       d.Severity, d.Description, v.ValueCode, v.ValueLabel
FROM dbo.DeviceAlertDefinitions d
LEFT JOIN dbo.DeviceAlertValueDefinitions v ON v.AlertDefinitionId = d.AlertDefinitionId
WHERE d.ParameterName IS NOT NULL;
```

No TTL, for the same reason the metric catalog has none: these rows change with a firmware or model
release, which ships with a deployment, and a deployment restarts the process.

**This adds the live path's first database dependency.** The remark on `DeviceStatusDeriver`
claiming the live path keeps zero database dependencies becomes wrong and must be corrected in the
same change — status derivation itself still needs no SQL, but the pipeline around it now does.

### 5.2 Resolution rules

For each `parameter: value` pair in an entry's `Event` block:

```
state    = value == 0 ? "cleared" : "triggered"
def      = catalog[parameter]                      // may be absent
eventType, domain, severity = def ?? ("unknown", "unknown", "unknown")
label    = def?.ValueLabels?[value]                // null for the 16 boolean params
```

An `Event` key absent from `DeviceAlertDefinitions` is **emitted anyway** with `unknown` metadata
and a warning logged once per session per parameter. Dropping it would hide a gateway that started
reporting a parameter before the definition row was seeded — the exact failure this feature exists
to make visible.

### 5.3 Baseline events are pre-existing, not transitions

A baseline's `Event` block records state that was already true when the session opened. Treating
those as triggers would draw a marker at `t0` for a condition that triggered an hour earlier.

Dropping them outright is also wrong. A session that opens with an alarm already active would then
carry no record of *which* alarm: `status[]` reads `2` from the first grid point, but "Emergency Stop
Active" appears nowhere, and a reload has nothing to show.

So baseline events get a third state:

```
baseline entry, value != 0  -> state "active"   (stored, no marker drawn)
baseline entry, value == 0  -> not emitted at all
delta entry,    value != 0  -> state "triggered"
delta entry,    value == 0  -> state "cleared"
```

Suppressing the zeros matters: a baseline carries all 18 parameters, and without this rule every
session would open with a dozen meaningless "cleared" rows.

This keeps the pre-existing condition on the record and out of the marker layer. The frontend draws
glyphs for `triggered` and `cleared` and renders `active` as opening state — legend, tooltip, or a
banner, its choice.

No event state is persisted on the head row. The gateway performs edge detection, the ordering guard
rejects redeliveries before resolution runs, and `HasBaseline` already tracks whether the baseline
was folded — so there is nothing for a stored event map to decide.

## 6. Storage

Events are sparse — single digits per session — so they do not join the per-parameter fold.

One row per occurrence in the same session partition:

```
PartitionKey = {sessionId}
RowKey       = evt|{timestampUtc:O}|{parameter}
```

Timestamp leads the parameter so the whole set range-scans chronologically under the `evt|` prefix,
which is how the reload endpoint reads it; the parameter tail keeps two simultaneous transitions on
the same tick from colliding. Columns: `Parameter`, `Value`, `State`, `EventType`, `Domain`,
`Severity`, `Label`.

`LiveSessionKeys` gains `EventRowKey(parameter, timestampUtc)` and `EventRange(since)`. The class is
duplicated verbatim in the backend and both copies carry the same tests, so both move together.

`evt` is reserved as a metric name by this scheme: a parameter of that name would write
`evt|{timestamp}` and land inside the event range. No such row exists in `ParameterRegistry`, but
`LiveSessionKeys` should reject it outright rather than leave the collision latent.

These rows are written inside the existing entity-group transaction alongside the head row and the
change points, so a batch's snapshot, series and events can never disagree. They are deleted by the
existing cleanup timer with the rest of the partition — the 100-entity transaction ceiling is
unaffected at this volume.

## 7. Wire contract

### 7.1 SignalR

`events` is omitted entirely on the ~99% of ticks with no transition.

```json
{ "t": "2026-09-28T08:00:35Z",
  "status": 2,
  "changes": { "OperatingPressure": 28000 },
  "events": [
    { "parameter": "OperatingAlarmActive", "value": 5, "state": "triggered",
      "eventType": "alarm", "domain": "operational", "severity": "critical",
      "label": "Emergency Stop Active" },
    { "parameter": "MaintenanceAlarmActive", "value": 1, "state": "triggered",
      "eventType": "alarm", "domain": "maintenance", "severity": "critical",
      "label": null }
  ] }
```

`parameter` and `value` ship alongside `label` because they are the stable identity — `label` is
display text an administrator can edit in the database, and is null for 16 of the 18 parameters.
`eventType`, `domain` and `severity` ship for every event, boolean ones included: they cost nothing
once the catalog is loaded, and withholding them would force the frontend to hardcode a
parameter-name-to-marker mapping, which is the hardcoding this design exists to avoid.

The broadcast stays gzip+base64 as a whole; nothing about the compression changes.

### 7.2 Reload — `GET …/live-session/series`

`status[]` and `events[]` are returned on **both** the default call and the specific-metrics call.
The chart draws markers and the status column regardless of which metrics are selected in the
legend; omitting them from the filtered call would make markers vanish when the user changes the
selection. A 10-minute session carries roughly five events and one 600-int array — the cost is
negligible against the metric arrays.

```json
{ "sessionId": "…", "pollIntervalSeconds": 1, "startedUtc": "…", "tickCount": 600,
  "t": ["…"],
  "status": [0,0,1,1,2],
  "series": [ { "metric": "OperatingPressure", "v": [29800, 31200, null, 46700, 52900] } ],
  "events": [
    { "timestampUtc": "2026-09-28T08:00:35Z", "parameter": "OperatingAlarmActive",
      "value": 5, "state": "triggered", "eventType": "alarm", "domain": "operational",
      "severity": "critical", "label": "Emergency Stop Active" }
  ] }
```

`events[]` is filtered by the endpoint's existing `since` parameter on the same basis as the grid.

## 8. Components changed

### `shape-iot-function-data-ingestion`

| File | Change |
|---|---|
| `LivePolling/AlertDefinitionCatalog.cs` | **new** — load-once catalog keyed by `ParameterName` |
| `LivePolling/IAlertDefinitionCatalog.cs` | **new** — interface + `AlertDefinition` record |
| `LivePolling/LiveEventResolver.cs` | **new** — `Event` block → resolved `LiveEvent` list (§5.2) |
| `LivePolling/LiveTelemetryBatch.cs` | parse `Event` into `LiveEntry.Events` |
| `LivePolling/LiveSessionEntities.cs` | `LiveEventWrite` record — `LiveCurrent` is unchanged (§5.3) |
| `LivePolling/LiveSessionKeys.cs` | `EventRowKey` / `EventRange` |
| `LivePolling/ILiveSessionStore.cs` | `WriteBatchAsync` takes the batch's events |
| `LivePolling/TableLiveSessionStore.cs` | write event rows in the same transaction |
| `LivePolling/LiveTelemetryProcessor.cs` | resolve per entry; baseline seeds without emitting; `BroadcastEntry.Events` |
| `LivePolling/DeviceStatusDeriver.cs` | XML remark corrected (§5.1) — no behaviour change |
| `Program.cs` | register the catalog |

### `shape-iot-backend`

| File | Change |
|---|---|
| `Infrastructure/LivePolling/LiveSessionKeys.cs` | mirror the two new key methods verbatim |
| `Application/LivePolling/ILiveSessionReader.cs` | `GetEventsAsync(sessionId, since, ct)`; `LiveEventPoint` record |
| `Infrastructure/LivePolling/TableLiveSessionReader.cs` | range-scan the `evt\|` prefix |
| `Application/DTOs/LivePolling/LivePollingDtos.cs` | `LivePollingEventDto`; `Events` on the series DTO; `Status` becomes `IReadOnlyList<int>` |
| `API/Controllers/LivePollingController.cs` | return events on both series variants |

### Not changed

`DeviceStatusDeriver` logic, `SnapshotFolder`, `SeriesStepExpander`, `GzipPayload`, the direct-method
path, `LivePollingSession` SQL auditing, the cleanup timer, RBAC, and the Event Grid filters.

## 9. Failure handling

| Case | Behaviour |
|---|---|
| Catalog load fails | Not cached; the batch's events resolve as `unknown` and are still stored and broadcast. Telemetry is never blocked on an alert-definition read. |
| Parameter not in `DeviceAlertDefinitions` | Emitted with `unknown` metadata, warned once per session per parameter. |
| `ValueCode` not in `DeviceAlertValueDefinitions` | `label: null`. Frontend falls back to `eventType` + `domain`. |
| `Event` present but not an object | Ignored; the entry's `Body`/`Changes` still fold. |
| Duplicate batch redelivered | Existing ordering guard rejects the whole entry before resolution runs, so no duplicate event rows. |
| Baseline arrives out of order | Its non-zero events are written as `active` during the existing back-fill path, at the baseline's own timestamp, so they sort ahead of the transitions already stored. |

## 10. Testing

Unit, in `shape-iot-function-data-ingestion.Tests/LivePolling/`, matching the existing plain-xunit
and hand-rolled-fake style:

- `AlertDefinitionCatalogTests` — load-once; failure not cached; value labels attach to 40006/40007 only
- `LiveEventResolverTests` — `0` → cleared; non-zero → triggered; label for operational only; unknown parameter passes through as `unknown`; boolean parameters carry type/domain/severity with a null label
- `LiveTelemetryBatchTests` — `Event` beside `Body`; `Event` beside `Changes`; `Event` absent; `Event` present but empty; `Event` a non-object
- `LiveTelemetryProcessorTests` — baseline non-zero events write `active`; baseline zeros write nothing; delta writes `triggered`/`cleared`; out-of-order baseline writes `active` during back-fill; redelivery produces no duplicate rows
- `LiveSessionKeysTests` — the `evt|` range excludes parameter rows and the head row; two events on one tick do not collide

Backend: `LivePollingControllerTests` covering events returned on the default and filtered series
calls, `since` filtering, and the `status[]` int mapping.

Integration (`LivePollingSessionRepositoryTests` style, Docker required): event rows survive the
entity-group transaction and range-scan back in chronological order.

## 11. Considered and deferred

**Enriching `active` events from `dbo.DeviceActiveConditions`.** That table carries `ActivatedAtUtc`,
so an `active` event could report when the condition actually started rather than only that it was
on when the session opened. Deferred 2026-09-30: the live chart does not show a "since" time, so the
field would be unused. If that changes, the shape is one query in the series endpoint — matched on
`ParameterName`, filling a nullable `activatedAtUtc` — and it decorates the baseline's rows rather
than replacing them, because the baseline cannot be stale and the table can.

## 12. Known gaps after implementation

Raised by the whole-branch review on 2026-09-30, judged not worth fixing now. Listed here rather
than in a scratch ledger so they survive.

**Not covered by any executed test.** The event row's Table Storage round-trip. Docker is not
installed on the development machine, so `Brilliant.IoT.IntegrationTests` fails wholesale on the
Testcontainers fixture. Three cross-repo contract points are verified by inspection only: `Label`
written as null surviving as an absent property, `Value` written as `Edm.Int64` read back by
`GetInt64`, and `TimestampUtc` written as `DateTimeOffset` read back by `GetDateTime`. The writer is
in one repo and the reader in the other with no compile-time link. This is the seam most worth an
Azurite-backed test before the feature ships.

**Transaction headroom is 13 entities.** The worst realistic first batch measures 87 against the
100-entity limit: 64 change points, 22 events, one head row.
`WorstCaseFirstBatch_StaysUnderTheHundredEntityTransactionLimit` guards it. Exceeding 100 returns
400, which `TableLiveSessionStore` does not catch — the delivery fails and Event Grid redelivers
until the batch dead-letters. Adding fields to `TelemetryMeasures` eats this headroom directly.

**`label` is null on `cleared`** even though `DeviceAlertValueDefinitions` holds
`0 → "No Fault Active"`. §5.2's formula is unconditional; the implementation follows the plan
instead. Display-only, and the frontend is told to fall back to `eventType` + `domain`.

**SignalR sends `"events": null`** on quiet ticks rather than omitting the key as §7.1 says.
Deliberate — six bytes against a 2 KB billing chunk — and documented, but the frontend reference's
JSON examples still omit the key, as they already do for `snapshot`/`changes`.

**`AlertDefinitionCatalog` keys by `ParameterName` alone**, while the unique index is
`(ParameterName, AlertCode)`. Two definitions sharing a parameter name would silently merge their
value labels and lose the second's metadata. No such row exists today.

**Unknown parameters are warned once per occurrence**, not once per session per parameter as §5.2
says. Per-session state has no eviction point in a singleton processor, and the gateway edge-detects,
so in practice this is one line per transition.

**A failed catalog load retries on every batch** behind one `SemaphoreSlim`. A hanging query would
serialise batches. Narrow in practice: `ResolveDeviceAsync` opens a SQL connection first, so a real
outage fails earlier. A short negative cache would make §9's promise literally true.

**Two entries in one batch sharing a `TimestampUtc`** would produce duplicate RowKeys inside one
transaction. Pre-existing for change points; events now reach the same path.

**A second baseline mid-session** — a gateway restart — would write `active` rows at a timestamp
other than the first tick, contradicting the frontend doc's absolute. §2 says one baseline per
session.

## 13. Open items

- `docs/live-polling-frontend-integration.md` needs the events contract, the `status[]` type change
  and the enumerations for `state`, `eventType`, `domain` and `severity`.
