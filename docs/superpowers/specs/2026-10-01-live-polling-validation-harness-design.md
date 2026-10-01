# Live Polling Validation Harness — Design

**Date:** 2026-10-01
**Status:** Draft — awaiting review
**Branch:** `feat/live-polling-harness`

## 1. Purpose

Validate the live polling **backend design** end to end, locally, before its contract is handed to the
frontend team. The repo builds faithful local stand-ins for every component the design names — gateway
module, ingestion function, backend API — plus a validation frontend that consumes the documented
REST + SignalR contract the way the frontend team will.

The authoritative contracts are the reference documents, moved into this repo unchanged:

| Document | Role |
|---|---|
| `docs/reference/2026-09-15-fast-polling-feature-v3.md` | Gateway module contract (direct methods, gzip batches, `Event` input) |
| `docs/reference/2026-09-20-live-polling-api-design.md` | Cloud design: fold, Table layout, SignalR, cleanup, gating |
| `docs/reference/2026-09-30-live-polling-event-state-design.md` | Event state amendment: catalog, `active`/`triggered`/`cleared`, `evt|` rows, int status |
| `docs/reference/live-polling-frontend-integration.md` | REST + SignalR contract handed to the frontend team |
| `docs/reference/2026-09-21-live-polling-api.md`, `docs/reference/2026-09-30-live-polling-event-state.md` | Real-repo implementation plans (context only; not executed here) |

Where this document and a reference document disagree, the reference document wins unless this document
lists the difference under §11 (Deliberate deviations).

**Deliverables**

1. Running stand-ins whose externally visible behaviour matches the reference contracts.
2. Tests that exercise every behaviour the designs promise, including the cross-repo seam.
3. `docs/findings.md` — contract gaps and design questions discovered while validating, with evidence.
   This is the artefact handed back to the backend team.

**Non-goals:** production UI, authentication/RBAC, multiple devices, regular 10-minute telemetry and its
historical data, real Azure resources, the Event Grid subscription filter split.

## 2. Decisions

| Topic | Decision |
|---|---|
| Purpose | Backend validation harness; frontend is a validation consumer |
| Structure | One project per real deployable (§3) |
| Old prototype | Deleted on this branch as the first implementation step (stays in git history) |
| Frontend stack | React + Vite + TypeScript + Recharts |
| Session store | SQLite file shared by backend and cleanup timer |
| Live store | Azure Table Storage on Azurite (the design under test) |
| Alert catalog | Static `alert-catalog.json` from the 18 provided rows, `ParameterName`s aligned to telemetry names |
| Devices | Exactly one, from configuration |
| Auth | None. Endpoints are open; `StartedByUserId` is recorded as `"local"`; negotiator takes `x-user-id` |
| History | Live only, with a session-start boundary marker; reload from `/series` |

## 3. Architecture

```
┌──────────────────────────┐ POST EventGridEvent (Microsoft.Devices.DeviceTelemetry)
│ Shape.Gateway.Simulator  │ data.body = base64(gzip({Entries})) ───────────────────────┐
│  :5100                   │                                                            ▼
│  • pump device + events  │◄── POST /twins/{dev}/modules/{mod}/methods ──┐ ┌────────────────────────────┐
│  • FastPolling* methods  │    (IoT Hub direct-method facade)            │ │ Shape.LiveIngestion :7071  │
│  • /sim/* scenario API   │                                              │ │  LiveTelemetryProcessor    │
└──────────────────────────┘                                              │ │  LivePollingCleanupTimer   │
                                                                          │ │  negotiate / join / leave  │
┌──────────────────────────┐   REST /api/v1/devices/{sn}/live-session     │ └───┬──────────┬──────────┬──┘
│ web (validation UI)      │──────────────────────────────►┌──────────────┴──┐  │ EGT      │ SignalR  │ SQLite
│  :5173                   │                               │ Shape.LiveBackend│  ▼          ▼  (read)  ▼
│                          │◄── liveTelemetryUpdate ───┐   │  :5080           │ Azurite   asrs-emulator  data/
└──────────────────────────┘    (gzip+base64)          │   │  SQLite session  │ Table     :8888          live-polling.db
                                                       │   │  Table reader    │ 10100-2                   ▲
                                                       └───┤  (via emulator)  ├───────────────────────────┘
                                                           └──────────────────┘
```

**Ports:** simulator `5100`, backend `5080`, function host `7071`, SignalR emulator `8888`,
Azurite `10100`/`10101`/`10102` (10000/10002 are held by SCADA services on the dev machine),
Vite `5173`.

### 3.1 Repository layout

```
Shape_5_sec/
  ShapeLive.sln
  global.json                         # SDK 8.0.425, rollForward latestFeature
  config/
    device.json                       # the one device: serial, DeviceGuid, gateway id, module id, name
    alert-catalog.json                # 18 definitions + value labels (§6)
    pump-seed.json                    # 73-field telemetry seed
  data/                               # SQLite file (git-ignored)
  src/
    Shape.Gateway.Simulator/          # gateway module + IoT Hub facade + Event Grid delivery
    Shape.LiveIngestion/              # Functions isolated: processor, cleanup timer, negotiator
    Shape.LiveBackend/                # ASP.NET Core 8 API
    Shape.SmokeClient/                # headless REST + SignalR e2e driver
  tests/
    Shape.Gateway.Simulator.Tests/
    Shape.LiveIngestion.Tests/
    Shape.LiveBackend.Tests/
    Shape.Seam.Tests/                 # cross-project Azurite + SQLite integration tests
  web/
  scripts/run-local.ps1, scripts/smoke.ps1
  docs/findings.md
```

`LiveSessionKeys` is **duplicated** in `Shape.LiveIngestion` and `Shape.LiveBackend`, exactly as the real
repos duplicate it. Both copies carry the same tests, and `Shape.Seam.Tests` writes with the ingestion
writer and reads with the backend reader. There is no shared contracts library between ingestion and
backend; that absence is the seam being validated.

## 4. Gateway simulator (`Shape.Gateway.Simulator`)

### 4.1 Device

One pump from `config/device.json`:

```json
{ "serialNumber": "PUMP-00417", "deviceGuid": "9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284",
  "name": "PREC-NEO30-MARYL-07", "gatewayId": "gw-001", "moduleId": "DefaultModeModule" }
```

Telemetry is seeded from `config/pump-seed.json` (the 73-field pump sample). Field generators:

| Field(s) | Rule |
|---|---|
| `OperatingPressurePSI` | `54000 + 3000·sin(2πt/30) + uniform(±150)`; `OperatingPressureBar = round(PSI/14.5038)` |
| `StrokeRateLeft/Right` | random walk ±2, clamped 80–100 |
| `StrokeTimeLeft/Right` | random walk ±10, clamped 5200–5400 |
| `StrokeCounterTW1` | accumulates `(L+R)/2/60` per tick |
| `HydraulicOilTemperatureF/C` | changes every 10 ticks, walk ±1 clamped 100–115; C derived |
| `ProportionalOutputValue` | linear map of PSI 51000–57000 → 70–90 |
| `OperatingHours*` | +1 per 3600 ticks |
| The 18 event parameters (§6) | held at their current event-state value (§4.3); never random |

`--seed <int>` makes generators deterministic.

### 4.2 Direct methods — IoT Hub facade

```
POST /twins/{deviceId}/modules/{moduleId}/methods
     { "methodName": "...", "payload": { ... }, "responseTimeoutInSeconds": 30 }
→ 200 { "status": <module status>, "payload": { ... } }
```

Unknown `deviceId`/`moduleId` → HTTP `404`. `--unreachable` makes every call hang past
`responseTimeoutInSeconds` and return HTTP `504` (IoT Hub's device-timeout code).

Methods behave exactly as `2026-09-15-fast-polling-feature-v3.md`:

- **`FastPollingEnable`** — optional `{ sessionDurationMinutes (10–30, default 10), pollIntervalSeconds (1–60, default 1) }`,
  case-insensitive names; missing/empty/unparseable body → defaults; supplied out of range or non-integer → `400`
  listing every error; validation before the "already active" `409`. `200` body
  `{ result, durationMinutes, intervalSeconds, sessionId, defaultsApplied }`.
- **`FastPollingDisable`** — `200 { result, sessionId }` or `409`.
- **`Extend-FastPolling`** — required `{ sessionId (GUID), extendSessionDurationMinutes (positive int) }`.
  Check order: field validation `400` → session-id match `409` → 30-minute total cap `400` with the module's
  reason text. Total duration measured from the original start; one expiry timer, rescheduled.
  `200 { result, sessionId, extendedByMinutes, totalDurationMinutes }`.

One session at a time. Session ends on disable, expiry or host shutdown; the partial batch (1–4 entries) is
flushed. Twin reports are logged only.

### 4.3 Event state

The simulator keeps a current value for each of the 18 event parameters (raw register values: booleans as
`0`/`1`, `OperatingAlarmActive`/`OperatingWarningActive` as codes). Those values also drive the matching
telemetry fields, so `Body`/`Changes` and `Event` stay consistent.

- Baseline entry: `Event` carries all 18 current values (zeros included — the function suppresses them).
- Delta entry: `Event` present only on a tick where at least one value changed, carrying only the changed keys.
- `MotorRunning` is `1` while a session runs unless overridden.

### 4.4 Batches and delivery

Every `pollIntervalSeconds` one entry `{ TimestampUtc, Body | Changes, Event? }` is buffered; at 5 entries the
batch `{ "Entries": [...] }` is gzipped (`CompressionLevel.Optimal`) and delivered as an Event Grid event:

```json
[{ "id": "<guid>", "eventType": "Microsoft.Devices.DeviceTelemetry", "subject": "devices/gw-001",
   "eventTime": "<batchSentUtc>", "dataVersion": "",
   "data": {
     "properties": { "batchSentUtc": "...", "session-id": "<guid>", "telemetry-type": "fast-polling" },
     "systemProperties": { "iothub-connection-device-id": "gw-001", "iothub-content-encoding": "gzip",
                           "iothub-content-type": "application/json", "iothub-enqueuedtime": "..." },
     "body": "<base64 of gzip bytes>" } }]
```

POST to `http://localhost:7071/runtime/webhooks/EventGrid?functionName=LiveTelemetryProcessorEventGrid`
with header `aeg-event-type: Notification`. Retries 1 s / 2 s / 4 s on failure (timeouts included); a
publish exception never stops the session's publishing loop.

This envelope is an **assumption** answering open item 5 of the 09-20 design. Only `LiveEventGridDecoder`
(§5.1) depends on it.

### 4.5 Scenario API and fault flags

| Control | Effect |
|---|---|
| `POST /sim/events` `{ "OperatingAlarmActive": 5, "MotorRunning": 0 }` | Sets event values, effective next tick |
| `POST /sim/events/clear` | Resets all event values to 0 (MotorRunning to 1) |
| `GET /sim/state` | Current session and event values |
| `--alarm-at-start` | Next session opens with `OperatingAlarmActive = 5` |
| `--drop-every N` | Skip every Nth batch (N ≥ 2) |
| `--reorder` | Send adjacent batches after the first swapped |
| `--duplicate` | Send every batch twice |

## 5. Ingestion function (`Shape.LiveIngestion`)

.NET 8 isolated worker. SignalR worker extension pinned to **1.15.0** (2.x needs a newer Core Tools host than
the dev machine's 4.9).

### 5.1 `LiveTelemetryProcessorEventGrid`

Per event:

1. **Guard** — `data.properties["telemetry-type"] != "fast-polling"` → log error, return (no throw).
2. **Decode** (`LiveEventGridDecoder`) — base64 → gunzip → `LiveTelemetryBatch { SessionId, BatchSentUtc,
   GatewayId, CompressedBytes, Entries[] }`; each entry `{ TimestampUtc, Body?, Changes?, Event? }`.
   `Event` that is not an object is ignored. Corrupt base64/gzip/JSON → log, return.
3. **Resolve device** (`DeviceResolver`) — gateway id → `{ DeviceGuid, SerialNumber }` from `config/device.json`.
   Unknown gateway → log, return.
4. **Load** head row `(PK = sessionId, RK = "!current")` if present.
5. **Fold** (`SnapshotFolder`, pure) — entries sorted by `TimestampUtc`:
   - entry not newer than `LastSnapshotTimestampUtc` → skipped, **unless** it carries `Body` and
     `HasBaseline` is false (back-fill: write its change points at its own timestamp, fill only snapshot
     fields not already known, resolve its events as baseline events; do not move `LastSnapshotTimestampUtc` back);
   - `Body` → snapshot := Body; every field becomes a change point; `HasBaseline = true`;
   - `Changes` → merge; each key becomes a change point; `{}` writes none;
   - `DeviceStatus` derived per entry (§5.2); a `DeviceStatus|{ts}` row only where it differs from the previous value;
   - `TickCount += 1`, `LastTickUtc`, `FirstTickUtc` (first applied), `LastSnapshotTimestampUtc` advance.
   `BytesTotal += CompressedBytes` once per batch that applied at least one entry.
6. **Resolve events** (`LiveEventResolver`) per applied entry — §6.2.
7. **Commit** (`TableLiveSessionStore.WriteBatchAsync`):
   - **≤ 100 entities** (every batch after the first, in practice): one entity-group transaction — head
     upsert (`UpdateMode.Replace`, `If-Match` ETag; add when new) + change-point and event rows.
   - **> 100 entities** (the session's first batch with this 73-field payload measures ~108, see F2):
     change-point and event rows are **upserted** (`UpdateMode.Replace`) in chunks of ≤ 100, then the head
     row is written with `If-Match` in its own final transaction. Those rows are keyed by parameter and
     timestamp with deterministic values, so writing them again on a retry or redelivery is idempotent; the
     head row stays the single commit point, so a delta is never "folded but unrecorded". The cost versus the
     design: a reader can briefly see change points newer than the head row — the backend's series grid is
     bounded by the head's `LastTickUtc`, so they are invisible until the head commits.
   - On `412`/`409` on the head → reload, re-fold, retry (max 3). Client retry policy: exponential backoff
     for `503`/`500`.
8. **Broadcast** — only if at least one entry applied:
   - Entry 0 of the outgoing batch carries the full `snapshot` as folded at that entry; entries 1–n carry
     `changes` (`{}` for heartbeats). Back-filled entries are not broadcast.
   - Every entry has all keys: `timestampUtc`, `deviceStatus` (int), `snapshot`, `changes`, `events`
     (explicit `null` when absent).
   - Batch `{ sessionId, deviceGuid, batchSentUtc, entries }` → compact JSON → gzip → base64 →
     `{ "encoding": "gzip+base64", "payload": "..." }`.
   - `SignalRMessageAction` target `liveTelemetryUpdate`, hub `iot_telemetry`,
     group `telemetry:device:{deviceGuid}`.

### 5.2 Device status

```
2 Alarm    if OperatingAlarmActive > 0 or MaintenanceAlarmActive is true/non-zero
1 Warning  else if OperatingWarningActive > 0 or MaintenanceWarningActive is true/non-zero
0 Normal   otherwise
```

Read from the folded snapshot; `MotorRunning` never affects it. Stored internally as the integer.

### 5.3 Negotiator

Same function app (deviation, §11).

| Function | Route | Behaviour |
|---|---|---|
| `negotiate` | `POST /api/negotiate` | `SignalRConnectionInfoInput(HubName="iot_telemetry", UserId="{headers.x-user-id}")` |
| `join-device` | `POST /api/join-device?deviceId=` | `SignalRGroupAction(Add)` for `x-user-id` to `telemetry:device:{deviceId}` |
| `leave-device` | `POST /api/leave-device?deviceId=` | `SignalRGroupAction(Remove)` |

Missing `x-user-id` or `deviceId` → `400`. CORS: `http://localhost:5173` with credentials.

### 5.4 `LivePollingCleanupTimer`

NCRONTAB from `LivePolling:CleanupSchedule` (local default `0 */1 * * * *`).

```
SELECT SessionId FROM LivePollingSession
 WHERE PurgedUtc IS NULL AND (StoppedUtc IS NOT NULL OR ExpiresUtc < @now)
for each: read !current → copy TickCount/BytesTotal into the row (if the head exists)
          delete every row of the partition, ≤100 per transaction
          set PurgedUtc = @now
```

Never scans Table Storage. Failures per session are logged and retried next run.

### 5.5 `LiveSessionKeys`

```
HeadRowKey                    = "!current"
ChangeRowKey(param, ts)       = $"{param}|{ts:O}"          (param ≠ "evt", no '|')
ParamRange(param, since?)     = ["{param}|{since:O or ''}", "{param}}")     -- '}' = '|' + 1
EventRowKey(param, ts)        = $"evt|{ts:O}|{param}"
EventRange(since?)            = ["evt|{since:O or ''}", "evt}")
```

`ts` is always UTC, `O` format (7 fractional digits), so lexical order is chronological. A metric named
`evt` or containing `|` throws `ArgumentException`.

### 5.6 Row shapes

| Row | Columns |
|---|---|
| `!current` | `DeviceGuid`, `DeviceId`, `Snapshot` (JSON), `DeviceStatus` (int, null before first tick), `LastSnapshotTimestampUtc`, `HasBaseline`, `TickCount`, `BytesTotal`, `FirstTickUtc`, `LastTickUtc` |
| change point | `V` (native: Boolean / Int64 / Double / String), `T` (`b`/`i`/`d`/`s`), `TimestampUtc` |
| `DeviceStatus|{ts}` | `V` Int64, `T` = `i`, `TimestampUtc` |
| `evt|{ts}|{param}` | `Parameter`, `Value` (Int64), `State`, `EventType`, `Domain`, `Severity`, `Label` (absent when null), `TimestampUtc` |

The head row omits the design's `PollIntervalSeconds` and `ExpiresUtc`: the function has no source for
either, and the backend reads both from SQLite (§11).

## 6. Alert catalog and event resolution

### 6.1 `config/alert-catalog.json`

The 18 provided definitions, with `ParameterName` aligned to the telemetry field names:

| ParameterName | AlertCode | EventType | Domain | Severity |
|---|---|---|---|---|
| `OperatingAlarmActive` | 40006 | alarm | operational | critical |
| `OperatingWarningActive` | 40007 | warning | operational | major |
| `MaintenanceAlarmActive` | 40008 | alarm | maintenance | critical |
| `MaintenanceWarningActive` | 40009 | warning | maintenance | major |
| `OilChangeWarning` | 40301 | warning | maintenance | major |
| `OilChangeAlarm` | 40302 | alarm | maintenance | critical |
| `OilFilterWarning` | 40303 | warning | maintenance | major |
| `OilFilterAlarm` | 40304 | alarm | maintenance | critical |
| `WaterFilterAssyWarning` | 40305 | warning | maintenance | major |
| `WaterFilterAssyAlarm` (was `WaterFilterFault`) | 40306 | alarm | maintenance | critical |
| `TemperatureCheckValveAlarmA` (was `TemperatureCheckValveAssy_A_Alarm`) | 40401 | alarm | operational | critical |
| `TemperatureCheckValveAlarmB` (was `TemperatureCheckValveAssy_B_Alarm`) | 40402 | alarm | operational | critical |
| `TemperatureCheckValveWarningA` (was `TemperatureCheckValveAssy_A_Warning`) | 40403 | warning | operational | major |
| `TemperatureCheckValveWarningB` (was `TemperatureCheckValveAssy_B_Warning`) | 40404 | warning | operational | major |
| `BoosterPressureFault` | 40405 | alarm | operational | critical |
| `HydraulicFilterAssyWarning` | 40412 | warning | maintenance | major |
| `HydraulicFilterAssyAlarm` | 40413 | alarm | maintenance | critical |
| `MotorRunning` | 40001 | status | operational | minor |

Each entry also carries `Description` and `Action` text from the provided rows. Value labels (reference set,
not complete):

| Parameter | ValueCode → ValueLabel |
|---|---|
| `OperatingAlarmActive` | `0` No Fault Active · `5` Emergency Stop Active · `6` Overstroke Alarm |
| `OperatingWarningActive` | `0` No Warning Active · `21` Left Overstroke Warning |

`AlertDefinitionCatalog` loads the file once, keyed by `ParameterName` (ordinal, case-sensitive), behind
`IAlertDefinitionCatalog` with the same shape as the 09-30 design. A load failure is not cached; events then
resolve as `unknown`.

### 6.2 Resolution rules

For each `parameter: value` in an applied entry's `Event`:

```
value    = coerce to integer: JSON integer; double with zero fraction; numeric string; true→1 / false→0
           (anything else → skip that key, log warning)
baseline entry (Body):   value != 0 → state "active";  value == 0 → not emitted
delta entry (Changes):   value != 0 → state "triggered"; value == 0 → state "cleared"
def      = catalog[parameter]  (missing → eventType/domain/severity = "unknown", warn)
label    = state == "cleared" ? null : def?.ValueLabels?[value]   (null when absent)
```

`label` on `cleared` is `null` — this mirrors the real implementation (09-30 §12, "label is null on
cleared"), so the frontend team sees the behaviour it will actually get.

## 7. Backend API (`Shape.LiveBackend`)

ASP.NET Core 8 controllers. Contract exactly as `live-polling-frontend-integration.md`: base path
`/api/v1/devices/{serialNumber}/live-session`, camelCase, ISO-8601 UTC, envelope
`{ succeeded, message, errors, data }` with `errors` omitted when null and `errors[0]` the code.
CORS for `http://localhost:5173`. No auth.

### 7.1 Configuration (`LivePolling:*`)

| Key | Local value |
|---|---|
| `PollIntervalSeconds` | 1 (clamped 1–60) |
| `SessionDurationMinutes` | 10 (clamped 10–30) |
| `ExtensionMinutes` | 10 |
| `MaxConcurrentSessions` | 5 |
| `DefaultSeriesMetrics` | `OperatingPressurePSI`, `HydraulicOilTemperatureF`, `StrokeRateLeft`, `StrokeRateRight` |
| `ModuleId` | from `config/device.json` |
| `DirectMethodTimeoutSeconds` | 10 |
| `IoTHubUrl` | `http://localhost:5100` |
| `SessionDbPath` | `data/live-polling.db` |
| `TableConnectionString` | Azurite on 10100–10102 |

### 7.2 Endpoints

**Device lookup** (every endpoint): serial ≠ configured device → `404 DEVICE_NOT_FOUND`; configured device with
no `gatewayId` → `404 GATEWAY_NOT_PAIRED`.

**An open session** is a row with `StoppedUtc IS NULL AND ExpiresUtc > now` (lazy expiry).

| Endpoint | Behaviour |
|---|---|
| `POST` | Body optional; supplied field out of range → `400 LIVE_SESSION_INVALID_PARAMETERS` (`errors[1..]` per field). Device has an open session → `409 LIVE_SESSION_ALREADY_ACTIVE` (message names `expiresUtc`). Open sessions fleet-wide ≥ cap → `409 LIVE_SESSION_CAPACITY_REACHED`. Call `FastPollingEnable` with the request values or clamped config. Module `200` → insert row, return start DTO. Module `409` → `ALREADY_ACTIVE`. Module `400` → `INVALID_PARAMETERS` echoing module errors. Timeout / 404 / 504 / connection failure → `503 GATEWAY_UNREACHABLE` |
| `GET` | `{ active: false }` with all else null when no open session. Otherwise SQLite metadata + head row: `snapshot`, `deviceStatus`, `snapshotTimestampUtc`, `tickCount`, `bytesTotal` (null before first tick); `secondsRemaining` floored at 0 |
| `POST /extend` | No open session → `409 LIVE_SESSION_NOT_ACTIVE`. Call `Extend-FastPolling { sessionId, extendSessionDurationMinutes: ExtensionMinutes }`. Module `200` → `ExpiresUtc = StartedUtc + totalDurationMinutes`, update row, return start DTO. Module `400` → `LIVE_SESSION_EXTENSION_LIMIT_REACHED`, `errors[1]` = module reason. Module `409` → `NOT_ACTIVE`. Unreachable → `503` |
| `DELETE` | No open session → `409 NOT_ACTIVE`. `FastPollingDisable`: module `200` or `409` → stamp `StoppedUtc`, `StopReason = ManualStop`, `200 { message: "Live session stopped." }`. Unreachable → `503`, row left open |
| `GET /series` | §7.3 |

Start/extend DTO: `sessionId`, `deviceId`, `pollIntervalSeconds`, `sessionDurationMinutes`, `startedUtc`,
`expiresUtc`, `totalDurationMinutes`, `canExtend` (`totalDurationMinutes < 30`).

### 7.3 Series

- `metric` repeatable, 0–10, de-duplicated case-insensitively, request order kept; none → defaults.
- Valid names: the seed field names (the parameter-registry stand-in) plus synthetic `DeviceStatus`;
  matched case-insensitively, returned in registry casing. Unknown/blank/>10 →
  `400 LIVE_SESSION_INVALID_PARAMETERS`: `errors[1..]` one per offending value, last element
  `"Valid metrics: ..."` sorted.
- No open session → `409 LIVE_SESSION_NOT_ACTIVE`.
- Reads, in parallel: one `ParamRange` per metric, `ParamRange("DeviceStatus")`, `EventRange()`.
- **Grid:** `interval` = session's `PollIntervalSeconds`; `tₖ = FirstTickUtc + k·interval` for
  `k = 0..floor((LastTickUtc − FirstTickUtc)/interval)`; with `since`, only `tₖ ≥ since`.
- **Step expansion** (`SeriesStepExpander`): value at `tₖ` = last change point with
  `TimestampUtc < tₖ + interval/2`; `null` before the first one. `status[]` uses the same rule over
  `DeviceStatus` rows, `0` before the first.
- **Events:** all rows from `EventRange()`, oldest first; with `since`, keep `active` events always and
  `triggered`/`cleared` only when `TimestampUtc ≥ since`.
- No ticks yet → empty `t`, `status`, `v` arrays.

### 7.4 Storage seams

- `ILivePollingSessionRepository` over `Microsoft.Data.Sqlite`, raw SQL, schema §7.5. WAL mode
  (backend and cleanup timer share the file).
- `ILiveSessionReader` over `Azure.Data.Tables` with the backend's own `LiveSessionKeys` copy.
- `IDirectMethodClient.InvokeAsync(deviceId, moduleId, methodName, payload, timeout, ct)` →
  `DirectMethodResult(int Status, JsonElement? Payload)`; throws `GatewayUnreachableException`.
- `TimeProvider` injected for every "now".

### 7.5 SQLite schema

```sql
CREATE TABLE IF NOT EXISTS LivePollingSession (
  SessionId TEXT PRIMARY KEY, DeviceGuid TEXT NOT NULL, DeviceId TEXT NOT NULL, GatewayId TEXT NOT NULL,
  StartedUtc TEXT NOT NULL, StartedByUserId TEXT NOT NULL, PollIntervalSeconds INTEGER NOT NULL,
  SessionDurationMinutes INTEGER NOT NULL, ExpiresUtc TEXT NOT NULL, ExtensionsUsed INTEGER NOT NULL DEFAULT 0,
  LastExtendedUtc TEXT, StoppedUtc TEXT, StopReason TEXT, TickCount INTEGER, BytesTotal INTEGER, PurgedUtc TEXT);
CREATE INDEX IF NOT EXISTS IX_Session_Device_Expires ON LivePollingSession(DeviceGuid, ExpiresUtc);
CREATE INDEX IF NOT EXISTS IX_Session_Unpurged ON LivePollingSession(PurgedUtc) WHERE PurgedUtc IS NULL;
```

Timestamps stored as ISO-8601 `O` strings (lexical = chronological). Created on startup by both the backend
and the function.

## 8. Validation frontend (`web/`)

React + Vite + TypeScript + Recharts. Config: `VITE_BACKEND_URL`, `VITE_NEGOTIATOR_URL`,
`VITE_SIMULATOR_URL`, `VITE_DEVICE_SERIAL`, `VITE_DEVICE_GUID` (see F1).

### 8.1 Data flow

1. **Load:** `GET /live-session`. Active → `GET /series` (defaults) seeds chart, status strip and events.
2. **Live:** `negotiate` (`x-user-id: local`) → connect → `join-device(deviceGuid)` (re-joined after every
   reconnect). Each `liveTelemetryUpdate` envelope is decoded with `DecompressionStream("gzip")`.
3. **Apply:** per batch, `entries[0].snapshot` is truth; entries 1–n apply `changes` in order → frames.
   Frames with `timestampUtc ≤` last shown are dropped (handles overlap with the `/series` seed and
   redelivery). A gap > 1.5 × interval between consecutive frames inserts a line break.
4. **Pacing:** frames play one per interval; backlog > 10 frames drains to the newest 5; a
   `visibilitychange` to visible runs an immediate catch-up tick.
5. **Connection:** `onclose` restarts the connection after automatic reconnect gives up.
6. After every Start/Extend/Stop the UI re-reads `GET /live-session`.

### 8.2 Layout (modelled on the Live Mode mock, not pixel-matched)

- **Header:** serial · name; idle → **Start** with interval/duration inputs; active → **Live** elapsed timer,
  remaining time (from `expiresUtc`), **+ Extend 10 min** (disabled when `!canExtend`), **Stop Live Mode**.
- **Error banner:** `errors[0]` code + `message` for every failed call.
- **Device Parameter Graph:** metric chips; three Y axes (PSI left, °F left, strokes/min right); session-start
  boundary line; status strip coloured by status; event markers by `eventType` + `state`
  (Motor On/Off from `MotorRunning` triggered/cleared, Warning / Warning Cleared, Alarm / Alarm Cleared,
  neutral glyph for `unknown`); tooltip with values and event text — label when present, else
  `"{domain} {eventType} {state}"`. `active` events render in an "Opened with: …" banner, never as markers.
- **Cards:** Motor Operating Hours (`OperatingHoursMotor`), Intensifier Operating Hours (`OperatingHoursTW1`).
- **Live Metrics:** searchable list of every snapshot field with units; selecting one loads
  `GET /series?metric=X` into a Time / Value / Status table that continues live.
- **Dev panels:** simulator controls (§4.5) and a **contract inspector**: last decoded batch, envelope and
  decoded byte sizes, and runtime checks flagging violations (missing key, both/neither of
  `snapshot`/`changes`, non-integer `deviceStatus`, unknown enum value, entries not ascending).

## 9. Error handling

| Where | Failure | Behaviour |
|---|---|---|
| Simulator | Event Grid POST fails / times out | Retry 1/2/4 s, then drop and log; session continues |
| Function | Wrong `telemetry-type`, corrupt body, unknown gateway | Log, return success (no redelivery loop) |
| Function | Table `412`/`409` on commit | Reload, re-fold, retry ×3, then throw (→ redelivery) |
| Function | Batch needs > 100 entities | Chunked idempotent upserts, head last (§5.1 step 7) |
| Function | Table `503`/`500` | SDK exponential backoff |
| Function | Catalog load failure | Events resolve `unknown`; telemetry still folds and broadcasts |
| Function | SignalR send fails | Invocation fails after state is saved; redelivery is stale → skipped (accepted, as in the design) |
| Backend | Gateway timeout / 404 / 504 / refused | `503 GATEWAY_UNREACHABLE` |
| Backend | Table read fails on `GET` | `500` with `message` (unexpected condition) |
| Timer | Per-session failure | Logged; retried next run |
| Frontend | Any non-2xx | Banner with `errors[0]` + `message` |

## 10. Testing

**Unit — `Shape.Gateway.Simulator.Tests`:** Enable defaults/`defaultsApplied`/400 listing all errors/400
before 409; Disable 200/409; Extend field validation → id match → cap order, total from original start
(T=0 +10, extend 10 at T=5 → ends T=20), cap reason text; event baseline carries all 18, delta only changed,
absent when unchanged; gzip `Entries` round-trip; partial flush; fault flags; publisher timeout retried.

**Unit — `Shape.LiveIngestion.Tests`:** decoder (valid, corrupt base64, corrupt gzip, corrupt JSON, `Event`
non-object); fold (baseline, delta, heartbeat, skip not-newer, full duplicate applies nothing, overlapping
redelivery applies only new entries, out-of-order baseline back-fill); status derivation (bool and int
fields, MotorRunning ignored); event resolution (active/triggered/cleared, baseline zeros suppressed,
`"5"`/`5.0`/`true` coerced, unknown parameter, label null on cleared, label only for the two operational
params); `LiveSessionKeys` (`StrokeRate` vs `StrokeRateLeft`, `evt|` range excludes parameter and head
rows, `evt` and `|` rejected, two events on one tick distinct); a first batch with the 73-field seed is
measured (> 100) and takes the chunked path; a later 5-delta batch stays ≤ 100 and takes the single
transaction; a chunked commit retried after a head `412` produces no duplicate rows;
broadcast decoded batch < 4,096 bytes and envelope < 2,048; every entry has all five keys.

**Unit — `Shape.LiveBackend.Tests`:** clamping; gating (open session, capacity, lazy expiry via fake
`TimeProvider`); every module status → error code mapping; extend expiry math; stop with module 409;
series validation (unknown, blank, >10, duplicates, casing); step expansion (jitter < interval/2, null before
first report, `since` trimming, `active` kept with `since`); envelope shape (`errors` omitted when null).

**Integration — `Shape.Seam.Tests`** (Azurite + temp SQLite): ingestion writer → backend reader round-trip
for change points (Boolean/Int64/Double/String), status rows and event rows (`Value` Int64, null `Label`
absent, timestamps); EGT atomicity; 412 → re-fold; cleanup timer purges only finished sessions, harvests
counters first, deletes partitions >100 rows.

**Frontend — Vitest:** envelope decode, batch applier/frames, pacing and backlog, series + live merge,
gap breaks, event glyph mapping and label fallback, contract-inspector checks.

**End-to-end — `scripts/smoke.ps1`** (`Shape.SmokeClient` through REST + negotiator + SignalR):
1. Start → 3 batches → `GET` shows snapshot and `tickCount ≥ 15` → `/series` grid length matches ticks.
2. `POST /sim/events {OperatingAlarmActive: 5}` → `triggered` with label "Emergency Stop Active" on SignalR
   and in `/series`; set `0` → `cleared`; `deviceStatus` 2 then 0.
3. Extend → `expiresUtc = startedUtc + 20 min`; extend again → 30 min with `canExtend = false`; extend once
   more → `400 LIVE_SESSION_EXTENSION_LIMIT_REACHED`.
4. Stop → `/series` returns `409`; within 2 timer runs the partition is gone and the row has `PurgedUtc`,
   `TickCount`, `BytesTotal`.
5. `--alarm-at-start` → `status[0] = 2` and one `active` event.
6. `--duplicate` and `--reorder` → event rows and change points without duplicates; grid unaffected.
7. Second start while active → `409 LIVE_SESSION_ALREADY_ACTIVE`; `--unreachable` → `503 GATEWAY_UNREACHABLE`.

## 11. Deliberate deviations from the reference design

| Reference | Here | Why |
|---|---|---|
| Azure SQL `LivePollingSession` | SQLite file | No SQL Server locally; logic sits above the repository interface |
| `DeviceAlertDefinitions` tables + SQL catalog | `alert-catalog.json` | No DB; same `IAlertDefinitionCatalog` shape |
| `ParameterRegistry` | seed field names | No DB |
| Negotiator is its own function app | Same app as the processor | One fewer process |
| `RequirePrivilege("LivePolling")` | No auth | Out of scope |
| Event Grid filter split, two subscriptions | Simulator posts only fast-polling events | No regular pipeline here |
| `DeviceResolver` via SQL | `config/device.json` | One device |
| `!current.ExpiresUtc`, `!current.PollIntervalSeconds` | Not stored on the head row | The function has no source for them; backend uses SQLite |
| One entity-group transaction per batch, always | Chunked idempotent upserts, head last, when a batch exceeds 100 entities | The real 73-field payload overflows the limit on the first batch (F2) |
| SignalR worker extension latest | 1.15.0 | Local Core Tools 4.9 host |
| Azurite default ports | 10100–10102 | Ports held by SCADA services |

## 12. Findings log (`docs/findings.md`)

Format per finding: id, title, evidence (test or observation), impact on the frontend contract, proposal.
Seeded with:

- **F1 — DeviceGuid not exposed.** `join-device` requires the DeviceGuid, but no live-session response
  returns it (`deviceId` is the serial). The frontend cannot subscribe from the documented API alone.
  Proposal: add `deviceGuid` to the start/extend and status responses.

- **F2 — First batch exceeds the 100-entity transaction limit with the real payload.** The design measured
  55 fields (worst case 87 entities, 13 of headroom). The gateway sample has 73 fields: 73 baseline change
  points + ~32 delta changes + status + head + events ≈ 108. As designed, `TableLiveSessionStore` would get a
  `400` on every session's first batch and Event Grid would redeliver it until dead-lettering — no session
  would ever show data. Proposal: the chunked idempotent commit in §5.1 step 7, or reconcile the field count
  against `TelemetryMeasures`.

The 09-30 design's §12 known gaps are re-checked against the running harness and each marked
*confirmed*, *not reproduced* or *not applicable*.

## 13. Open verification items carried from the reference design

Answered as far as a local harness can, and recorded in findings:

| 09-20 §16 item | Here |
|---|---|
| 5. How Event Grid presents a gzip body | Assumed (§4.4); isolated in `LiveEventGridDecoder`; still needs a real message |
| Transaction headroom (09-30 §12) | Measured by test with this seed — exceeded (F2) |
| Cross-repo Table round-trip (09-30 §12) | Covered by `Shape.Seam.Tests` |
| 1, 2 Event Grid filter semantics | Not testable locally (no Event Grid) |
| 3, 7, 8 Real-gateway measurements | Not testable locally |
