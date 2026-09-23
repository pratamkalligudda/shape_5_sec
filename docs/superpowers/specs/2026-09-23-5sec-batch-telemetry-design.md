# 5-Second Batched Fast-Polling Telemetry — Local Simulation Design

**Date:** 2026-09-23
**Status:** Draft — awaiting review

## 1. Goal

Simulate, fully locally, the evolution of the existing Fast Polling feature (see `2026-09-15-fast-polling-feature.md`) from **one message per second** to **one 5-second batch** per message:

- An edge gateway polls a Modbus pump every 1 s, buffers 5 samples in memory, and sends the 5-second packet as a whole.
- The first sample of a session is a full snapshot (`Body`); every later sample is a delta (`Changes` — changed fields with their **new values**; `{}` = heartbeat).
- The packet travels through an Event Grid–shaped delivery into an Azure Function.
- The Function stores the snapshot per session, folds each delta into it to reconstruct full state for every sample, and pushes the 5 folded samples to SignalR.
- A React frontend receives the batch and replays it **one sample per second** on live charts.

**Success criteria**

- Whole stack runs on one Windows laptop with no Azure resources.
- Function code (Event Grid trigger, SignalR output binding, Table storage) is production-shaped; moving to Azure changes only configuration.
- Charts move smoothly at 1 point/s with a steady ~5–6 s lag.
- Gaps (lost/reordered batches, Function restarts) are handled and visible, never silently corrupting state.

**Non-goals:** real Modbus hardware, real IoT Hub / Event Grid / Azure SignalR, multi-device support, authentication, resync return channel.

## 2. Decisions

| Topic | Decision |
|---|---|
| Environment | Fully local |
| Backend stack | C# .NET 8 isolated worker Functions; C# simulator |
| Event Grid | No emulator. Simulator POSTs `EventGridEvent[]` directly to the local `EventGridTrigger` webhook |
| Data shape | Waterjet pump payload (~80 fields), seeded from the sample in Appendix A |
| Delta semantics | Chained vs previous tick, new values (not numeric deltas), empty `Changes` = heartbeat — same as existing Fast Polling |
| Broken chain | Fold onto last known snapshot, flag `gap: true`; state persisted in Azurite Table Storage |
| SignalR | Azure SignalR Emulator (`asrs-emulator`), serverless mode, Functions `[SignalROutput]` + `negotiate` |
| Frontend | React + Vite + Recharts |
| Devices / sessions | One pump; session controlled via simulator HTTP API mirroring `FastPollingEnable`/`FastPollingDisable` |
| Chart window | Default 5 min, selectable 1 / 5 / 15 min |

## 3. Architecture

```
┌────────────────────┐   POST EventGridEvent[]     ┌─────────────────────────┐
│ GatewaySimulator   │ ──────────────────────────▶ │ Functions (.NET 8 iso)  │
│ (.NET 8 console +  │  /runtime/webhooks/EventGrid│  IngestTelemetry        │
│  minimal API :5100)│  aeg-event-type:Notification│   (EventGridTrigger)    │
└────────────────────┘                             │  Negotiate (HTTP)       │
        ▲ enable/disable                           └──────┬─────────┬────────┘
        │ (HTTP, CORS)                    Azurite Table   │         │ [SignalROutput]
┌───────┴────────────┐                    (snapshots) ◀──┘         ▼
│ web (React+Vite    │ ◀──────── WebSocket ─────────── asrs-emulator (:8888)
│  :5173, Recharts)  │           hub "telemetry", target "batch"
└────────────────────┘
```

**Ports:** simulator `5100`, Functions host `7071`, SignalR emulator `8888`, Azurite `10000–10002`, Vite `5173`.

### 3.1 Repository layout

```
Shape_5_sec/
  Shape5Sec.sln
  src/
    Shape.Contracts/          # DTOs shared by simulator, Functions, test client
    Shape.GatewaySimulator/   # console + minimal API
    Shape.Functions/          # isolated worker Function App
    Shape.SmokeClient/        # headless SignalR client for e2e smoke test
  tests/
    Shape.Tests/              # xUnit unit + integration tests
  web/                        # React + Vite + Recharts
  scripts/
    run-local.ps1             # start all processes
    smoke.ps1                 # e2e smoke test
  seed/
    pump-sample.json          # Appendix A payload
  docs/superpowers/specs/
```

### 3.2 Units

**`Shape.Contracts`**
- `TelemetryBatch { Properties, Samples[] }`
- `BatchProperties { SessionId ("session-id"), TelemetryType ("telemetry-type" = "fast-polling"), BatchSeq, BatchStartUtc }`
- `Sample { SnapshotTimestampUtc, Body?, Changes? }` — exactly one of `Body`/`Changes` is non-null.
- `FoldedBatch { SessionId, BatchSeq, Gap, Partial, Samples: FoldedSample[] }`
- `FoldedSample { Ts, Values, Changed: string[] }`
- Field maps are `Dictionary<string, JsonElement>` — no typed pump class, so schema changes never break folding.

**`Shape.GatewaySimulator`**
- `PumpDevice` — Modbus stand-in. Loads `seed/pump-sample.json`, applies per-field generators (§5.1). `Read(tick)` returns a full field map.
- `Poller` — `PeriodicTimer(1 s)`; runs only while a session is active.
- `DeltaEncoder` — holds `lastSent`; first read of a session → `Body`, else `Changes` of fields whose value differs (`JsonElement` deep equality).
- `BatchBuffer` — collects 5 samples, stamps `BatchSeq` (starts at 0 per session), emits `TelemetryBatch`. `Flush()` emits a partial batch.
- `EventGridPublisher` — wraps a batch in an `EventGridEvent` and POSTs it with retry (§6).
- `SessionController` — enable/disable semantics (§5.2); one session at a time via `Interlocked.CompareExchange`.
- `FaultInjector` — `--drop-every N`, `--reorder`.

**`Shape.Functions`**
- `IngestTelemetry` — `[EventGridTrigger]`; deserializes `TelemetryBatch`, loads state, calls `SnapshotFolder`, saves state, returns `[SignalROutput(HubName = "telemetry")]` message with target `batch`.
- `SnapshotFolder` — **pure**, no I/O: `Fold(SessionState? state, TelemetryBatch batch) → FoldResult { NewState, FoldedBatch?, Outcome }`.
- `SnapshotStore` — Azure.Data.Tables over Azurite. Table `sessionsnapshots`, PK = session id, RK = `state`, columns `SnapshotJson`, `LastBatchSeq`; ETag optimistic concurrency.
- `Negotiate` — `[HttpTrigger] POST /api/negotiate` + `[SignalRConnectionInfoInput(HubName = "telemetry")]`.

**`web`**
- `useTelemetryHub` — negotiate, connect, auto-reconnect; exposes connection state.
- `ReplayQueue` — pure TS; dedupe, 1 s playback, starve/backlog handling, ring buffers.
- `PressureChart`, `StrokeChart`, `TempChart`, `StatusPanel`, `SessionControls`, `RawDrawer`, `Header`.

## 4. Data Flow and Folding

### 4.1 Wire packet (Event Grid `data`)

Event envelope: `eventType = "Shape.Telemetry.FastPollingBatch"`, `subject = "pumps/pump-001"`, `dataVersion = "1.0"`, header `aeg-event-type: Notification`, POST to
`http://localhost:7071/runtime/webhooks/EventGrid?functionName=IngestTelemetry`.

```json
{
  "Properties": {
    "session-id": "cc27db6b-508b-488e-a816-faae684491d8",
    "telemetry-type": "fast-polling",
    "batchSeq": 0,
    "batchStartUtc": "2026-09-23T12:00:00Z"
  },
  "Samples": [
    { "snapshotTimestampUtc": "2026-09-23T12:00:00Z", "Body":    { "...": "all fields" } },
    { "snapshotTimestampUtc": "2026-09-23T12:00:01Z", "Changes": { "OperatingPressurePSI": 54360 } },
    { "snapshotTimestampUtc": "2026-09-23T12:00:02Z", "Changes": {} },
    { "snapshotTimestampUtc": "2026-09-23T12:00:03Z", "Changes": { "StrokeRateLeft": 97 } },
    { "snapshotTimestampUtc": "2026-09-23T12:00:04Z", "Changes": { "OperatingPressurePSI": 54410 } }
  ]
}
```

Only `batchSeq 0` of a session contains a `Body` (in `Samples[0]`). A batch normally has 5 samples; the final batch of a session may have 1–4.

### 4.2 Per-batch processing

1. Load `SessionState` for `session-id` (may be absent).
2. `SnapshotFolder.Fold`:
   - Classify batch per §4.3.
   - Walk samples in order: `Body` → snapshot := Body; `Changes` → merge keys into snapshot (overwrite).
   - After each sample, emit a `FoldedSample` holding an **independent full copy** of the snapshot and `Changed` = keys in that sample's `Changes` (all keys for `Body`).
3. Save `NewState` with ETag (insert if new, `UpdateEntity` with ETag otherwise; on 412 reload and re-fold, max 3 attempts).
4. Return the SignalR message; binding broadcasts to all clients.

### 4.3 Classification rules

Let `last` = stored `LastBatchSeq`.

| Case | Outcome | Action |
|---|---|---|
| State exists, `batchSeq == last + 1` | Normal | Fold; `gap = false` |
| State exists, `batchSeq > last + 1` | Gap | Fold onto stored snapshot; `gap = true` |
| State exists, `batchSeq <= last` | Stale/duplicate | Drop, log warning, return 200, no push |
| No state, batch has `Body` | New session | Fresh state; `gap = false` |
| No state, no `Body` | Orphan | Start from empty snapshot, fold; `gap = true`, `partial = true` |

`SessionState` stores `Partial`. Once a session is partial, every later batch for that session is also pushed with `partial = true`; the flag clears only when a new session (with `Body`) starts.

### 4.4 Push payload (SignalR target `batch`)

```json
{
  "sessionId": "cc27db6b-...",
  "batchSeq": 7,
  "gap": false,
  "partial": false,
  "samples": [
    { "ts": "2026-09-23T12:00:35Z", "values": { "...": "all fields" }, "changed": ["OperatingPressurePSI"] }
  ]
}
```

**Known limitation:** state is saved before the SignalR binding runs. If the push fails, the retried batch is classified stale and dropped, so that batch never reaches the UI. Acceptable for the simulation.

## 5. Simulator Behaviour

### 5.1 Field generators

`t` = seconds since session start. Fields not listed keep their seed value.

| Field(s) | Rule |
|---|---|
| `OperatingPressurePSI` | `54000 + 3000·sin(2πt/30) + uniform(±150)`, rounded int |
| `OperatingPressureBar` | `round(PSI / 14.5038)` |
| `StrokeRateLeft`, `StrokeRateRight` | random walk, step ±2, clamped 80–100 |
| `StrokeTimeLeft`, `StrokeTimeRight` | random walk, step ±10, clamped 5200–5400 |
| `StrokeCounterTW1` | accumulates `(StrokeRateLeft + StrokeRateRight)/2 / 60` per tick, stored as int |
| `HydraulicOilTemperatureF` | changes only every 10 ticks: random walk ±1, clamped 100–115 |
| `HydraulicOilTemperatureC` | `round((F − 32) · 5/9)` |
| `ProportionalOutputValue` | linear map of PSI 51000–57000 → 70–90, rounded |
| `OperatingHours*` (`TW1`, `CheckValveAssy_A/B`, `Motor`) | +1 every 3600 ticks |
| Warning/alarm bools (`*Warning*`, `*Alarm*`, `BoosterPressureFault`) | per tick 0.2 % chance to become true; stays true 20 ticks, then false |
| `MotorRunning` | true while a session is active |

`--seed <int>` makes the random sequence deterministic.

### 5.2 Session control API (`:5100`, CORS allows `http://localhost:5173`)

`POST /fast-polling/enable` — body optional: `{ "sessionDurationMinutes": 10..30 }`.

| Status | Body |
|---|---|
| 200 | `{ "result": "accepted", "durationMinutes": 10, "intervalSeconds": 1, "sessionId": "<guid>", "defaultsApplied": ["sessionDurationMinutes"] }` |
| 400 | `{ "result": "rejected", "reason": "Invalid request.", "errors": ["..."] }` |
| 409 | `{ "result": "rejected", "reason": "A fast polling session is already active." }` |

Missing/empty/invalid-JSON body → default (10 min). A supplied out-of-range value → 400. Validation precedes the 409 check. Poll interval is fixed at 1 s; a supplied `pollIntervalSeconds` other than 1 → 400 (batching assumes 1 s ticks).

`POST /fast-polling/disable` — 200 `{ "result": "accepted", "sessionId": "<guid>" }` or 409 `{ "result": "rejected", "reason": "No fast polling session is active." }`.

`GET /fast-polling/status` — `{ "active": bool, "sessionId"?, "startedUtc"?, "durationMinutes"? }` (used by the UI on load).

Session end (disable or expiry): stop the poller, flush the partial batch, clear `lastSent` and `BatchSeq`. No session → no telemetry.

## 6. Error Handling

| Where | Failure | Behaviour |
|---|---|---|
| Publisher | POST fails / non-2xx | Retry 3× with 1 s / 2 s / 4 s backoff; then drop batch, log error. `BatchSeq` still advances → Function sees a real gap |
| Publisher | `--drop-every N` | Every Nth batch skipped (logged) |
| Publisher | `--reorder` | Adjacent batch pairs sent swapped → later one arrives first, earlier one classified stale |
| Function | `data` fails to deserialize / invalid shape | Log error, return normally (no retry loop) |
| Function | Table 412 | Reload, re-fold, retry up to 3× |
| Function | Table unreachable | Throw → 500 → simulator retries |
| Frontend | SignalR disconnect | Auto-reconnect with backoff; dedupe absorbs replays |
| Frontend | Simulator API unreachable | `SessionControls` shows error text; charts unaffected |

## 7. Frontend

### 7.1 `ReplayQueue` (pure TS)

- `push(batch)` — ignore if `(sessionId, batchSeq)` already seen; enqueue samples. If `batch.gap`, enqueue a gap marker before its samples. If `sessionId` differs from the last seen, enqueue a session marker.
- `tick()` (called every 1000 ms):
  - **Before first batch:** no-op.
  - **Queue empty (starved):** append nothing; state `buffering`.
  - **Queue length > 10 (backlog):** drain all but the newest 5 into the series immediately, then play one.
  - Otherwise shift one item: sample → append to series; gap marker → append `null` point; session marker → record a session-start timestamp.
- Series stored as ring buffers sized to `windowSeconds`. X value = sample `ts` (not arrival time).
- Exposes `queueDepth`, `state` (`waiting | playing | buffering`), `latest` (last played `FoldedSample`).

### 7.2 Layout

- **Header** — connection state dot, session id, lag (`now − latest.ts`), queue depth, window selector (1 / 5 / 15 min, default 5).
- **PressureChart** — `OperatingPressurePSI` line; `HighPressureSetpointPSI` / `LowPressureSetpointPSI` as dashed reference lines; PSI/Bar toggle switches to `*Bar` fields.
- **StrokeChart** — `StrokeRateLeft`, `StrokeRateRight`.
- **TempChart** — `HydraulicOilTemperatureF` (left axis), `ProportionalOutputValue` (right axis).
- All charts: `connectNulls={false}` so gaps render as breaks; dashed vertical `ReferenceLine` labelled "session start" at session markers; animations off.
- **StatusPanel** — LED chips: `MotorRunning` (green/grey), every `*Alarm*` (red) and `*Warning*` (amber) bool plus `BoosterPressureFault`; counters `StrokeCounterTW1`, `OperatingHoursMotor`, `OperatingHoursTW1`. Values in `latest.changed` flash for ~600 ms.
- **SessionControls** — duration input (10–30), Enable / Disable buttons calling `:5100`; shows last response status and body; polls `/fast-polling/status` on load.
- **RawDrawer** — collapsible JSON of `latest.values`, keys in `latest.changed` highlighted.

## 8. Testing

**Unit — xUnit (`tests/Shape.Tests`)**
- `DeltaEncoder`: first read → `Body`; identical read → `Changes: {}`; only changed keys with new values; bool flip detected; string field compared; new session resets `lastSent`.
- `BatchBuffer`: emits at 5 samples; `BatchSeq` increments; `Flush()` emits partial; seq resets per session.
- `SnapshotFolder`: every row of §4.3; `Changed` lists correct; output samples are independent copies (mutating one does not affect others or state); `Partial` persists.
- `SessionController`: 200 with defaults and `defaultsApplied`; 400 out-of-range (all errors listed); 400 on bad `pollIntervalSeconds`; 409 when active; validation before 409; disable 200/409.
- `PumpDevice`: same `--seed` → identical sequence; all generated values within declared ranges.

**Integration — xUnit + Azurite**
- `SnapshotStore` insert/load/update round-trip; ETag conflict triggers reload-and-retry.
- `IngestTelemetry` invoked with a real `EventGridEvent` JSON → expected `SignalRMessageAction` payload.

**Frontend — Vitest (fake timers)**
- `ReplayQueue`: 1 sample per tick; dedupe; waiting and buffering states; backlog drains to 5; gap → `null` point; session marker recorded; ring buffer trims to window.

**End-to-end — `scripts/smoke.ps1`**
1. Start Azurite, emulator, Functions, simulator.
2. `POST /fast-polling/enable`.
3. `Shape.SmokeClient` connects via negotiate, collects 3 batches.
4. Assert `batchSeq` = 0, 1, 2; each sample's `values` has all seed keys; `gap = false`.
5. Restart simulator with `--drop-every 2`; assert a batch with `gap = true` arrives.

Frontend visuals are verified manually.

## 9. Local Tooling Prerequisites

- .NET 8 SDK
- Azure Functions Core Tools v4
- Azurite (`npm i -g azurite`)
- Azure SignalR Emulator (`dotnet tool install -g Microsoft.Azure.SignalR.Emulator`), started with `asrs-emulator start`; its connection string goes into `AzureSignalRConnectionString` in `local.settings.json` (serverless mode)
- Node 20+

`scripts/run-local.ps1` starts all five processes; each can also be started individually.

## Appendix A — Seed payload (`seed/pump-sample.json`)

```json
{"MotorRunning":true,"OperatingPressurePSI":50623,"OperatingPressureBar":3490,"RemainingMaintenanceTime":1,"OperatingAlarmActive":0,"OperatingWarningActive":0,"MaintenanceAlarmActive":false,"MaintenanceWarningActive":false,"PLCSoftwareVersion":0,"HighPressureSetpointPSI":59000,"HighPressureSetpointBar":4068,"LowPressureSetpointPSI":50834,"LowPressureSetpointBar":3505,"ActivateDumpValveOnHighLow":false,"DumpValveOpenTime":0,"ShutoffTime":0,"PLCYear":0,"PLCMonth":0,"PLCDay":0,"PLCWeekday":0,"PLCHour":0,"PLCMinute":0,"PLCSecond":0,"PLCNanosecond":0,"PLCReturnValue":0,"PLCWritySystemTime":false,"PLCReturnValueT":0,"FrontEndYear":0,"FrontEndMonth":0,"FrontEndDay":0,"FrontEndWeekday":0,"FrontEndHour":0,"FrontEndMinute":0,"FrontEndSecond":0,"FrontEndNanosecond":0,"PumpType":7,"PumpHP":125,"PumpBrandLabel":0,"PumpCoolingVersion":0,"PumpMotorFrequency":0,"PumpSerialNumber":78954293,"ENSoftStartFunctionality":false,"ENCheckvalveTemperature":false,"ENSealLifeTimeTracking":false,"ENRemoteNetworkControl":false,"ENProxSwitchIsNormallyClosed":false,"RemoteModbusIPChanged":false,"IPAddressRemoteStation":"0.0.103.9","OilChangeWarning":false,"OilChangeAlarm":false,"OilFilterWarning":false,"OilFilterAlarm":false,"OperatingHoursTW1":5706,"OperatingHoursCheckValveAssy_A":5706,"OperatingHoursCheckValveAssy_B":5706,"StrokeCounterTW1":5297,"OperatingHoursMotor":5706,"StrokeTimeLeft":5313,"StrokeTimeRight":5325,"StrokeRateLeft":89,"StrokeRateRight":84,"TemperatureCheckValveAlarmA":false,"TemperatureCheckValveAlarmB":false,"TemperatureCheckValveWarningA":false,"TemperatureCheckValveWarningB":false,"BoosterPressureFault":false,"WaterFilterAssyWarning":false,"WaterFilterAssyAlarm":false,"HydraulicOilTemperatureF":103,"HydraulicOilTemperatureC":39,"HydraulicFilterAssyWarning":false,"HydraulicFilterAssyAlarm":false,"ProportionalOutputValue":81}
```

Note: `OperatingAlarmActive` / `OperatingWarningActive` are ints (0/1) in the seed, not bools. The generator toggles them 0↔1 and the StatusPanel treats non-zero as active.
