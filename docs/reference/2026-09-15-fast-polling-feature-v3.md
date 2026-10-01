# Fast Polling Feature

`DefaultModeModule` normally publishes one telemetry snapshot every `System.PollIntervalSeconds` (twin-configurable, 1–20 min, default 10 min). **Fast Polling** is an on-demand, temporary mode, controlled entirely via two IoT Hub direct methods, that publishes telemetry at a much higher cadence (as low as 1s) for a bounded window, without disturbing the normal cadence — both run side by side.

Rather than sending one message per tick, ticks are buffered into batches of 5 and each batch is sent as a single gzip-compressed message — see [Batching & Compression](#batching--compression).

Implemented in `Workers/FastPollingService.cs`, backed by `Services/LatestTelemetryCache.cs` (fed by `Workers/PollingWorker.cs` on every `LiveTelemetry` message), `Services/TelemetryBatcher.cs` (batching), `Services/GzipCompressor.cs` (compression), and `Services/LatestEventStateCache.cs` (fed by `EventModeModule` on every `events` message — see [Event State Input](#event-state-input)).

## Direct Methods

All three methods are registered on the `ModuleClient` via `IMessagePublisher.SetMethodHandlerAsync` when the module starts. In `Development` environment they are **not** registered at all (the module skips all real IoT Hub interaction in dev), so they can only be exercised against a real IoT Hub connection.

### `FastPollingEnable`

Starts a fast-polling session. There is no persisted/twin-based configuration anymore — everything comes from this one call. **Neither parameter is required**: a missing parameter (or a missing/unparseable payload entirely) simply falls back to that parameter's default. A parameter that *is* supplied but outside the allowed range is rejected — missing and invalid are handled differently on purpose.

**Input payload** (JSON body of the direct method call — this entire body is optional):
```json
{
  "sessionDurationMinutes": 15,
  "pollIntervalSeconds": 5
}
```
| Field | Type | Required | Allowed range | Default if omitted |
|---|---|---|---|---|
| `sessionDurationMinutes` | int | no | 10 – 30 | 10 |
| `pollIntervalSeconds` | int | no | 1 – 60 | 1 |

Field names are matched case-insensitively. An empty body (`{}`), no body at all, or a body that isn't valid JSON are all treated identically to "both fields omitted" — every field just falls back to its default; none of these count as a rejection.

**Outputs / status codes:**

| Status | Meaning | Body |
|---|---|---|
| `200` | Accepted — session started | `{ "result": "accepted", "durationMinutes": 10, "intervalSeconds": 1, "sessionId": "<guid>", "defaultsApplied": ["sessionDurationMinutes", "pollIntervalSeconds"] }` |
| `400` | Rejected — a supplied value is outside the allowed range, or the payload could not be interpreted at all | `{ "result": "rejected", "reason": "Invalid request.", "errors": ["...", "..."] }` — `errors` lists **every** problem found, not just the first |
| `409` | Rejected — a fast polling session is already active | `{ "result": "rejected", "reason": "A fast polling session is already active." }` |

Validation (400) is always checked before the "already active" check (409) — an invalid payload is rejected as invalid even if a session happens to already be running.

`durationMinutes`/`intervalSeconds` in a `200` response are always the values actually used for the session (whatever was supplied, or the default) — never left ambiguous. `defaultsApplied` lists which of the two fields (if any) fell back to their default because they weren't supplied; it's always present, as an empty array (`[]`) when both were explicitly supplied. `sessionId` is a freshly generated GUID identifying this one session.

**Example — omitted payload, both defaults used:**
```json
{
  "result": "accepted",
  "durationMinutes": 10,
  "intervalSeconds": 1,
  "sessionId": "cc27db6b-508b-488e-a816-faae684491d8",
  "defaultsApplied": ["sessionDurationMinutes", "pollIntervalSeconds"]
}
```

**Example — only one field supplied (`{"pollIntervalSeconds": 5}`):**
```json
{
  "result": "accepted",
  "durationMinutes": 10,
  "intervalSeconds": 5,
  "sessionId": "9b1c2e3a-1111-4a2b-8c3d-abcdef123456",
  "defaultsApplied": ["sessionDurationMinutes"]
}
```

**Example — an explicit out-of-range value (`{"sessionDurationMinutes": 45, "pollIntervalSeconds": 5}`):**
```json
{
  "result": "rejected",
  "reason": "Invalid request.",
  "errors": ["'sessionDurationMinutes'=45 is outside the allowed range (10-30)."]
}
```

### `FastPollingDisable`

Stops the currently active session early, if any. No input payload.

| Status | Meaning | Body |
|---|---|---|
| `200` | Accepted — active session was signaled to stop | `{ "result": "accepted", "sessionId": "<guid>" }` (the id of the session just disabled) |
| `409` | Rejected — no session is currently active | `{ "result": "rejected", "reason": "No fast polling session is active." }` |

### `Extend-FastPolling`

Extends the currently active session's total planned duration — measured from when it originally started, not from when Extend is called. Both parameters are **required**; there is no default fallback here (unlike `FastPollingEnable`).

**Input payload:**
```json
{
  "sessionId": "cc27db6b-508b-488e-a816-faae684491d8",
  "extendSessionDurationMinutes": 10
}
```
| Field | Type | Required | Constraint |
|---|---|---|---|
| `sessionId` | string (GUID) | yes | must match the currently active session's id |
| `extendSessionDurationMinutes` | int | yes | must be a positive number of minutes |

**Outputs / status codes:**

| Status | Meaning | Body |
|---|---|---|
| `200` | Accepted — session extended | `{ "result": "accepted", "sessionId": "<guid>", "extendedByMinutes": 10, "totalDurationMinutes": 20 }` |
| `400` | Rejected — a required field is missing/invalid, or the extension would push the session's total duration past the 30-minute cap | `{ "result": "rejected", "reason": "...", "errors": [...] }` (field errors) or `{ "result": "rejected", "reason": "Extending by ... would bring the session's total duration to ... min, exceeding the 30 min cap (currently ... min)." }` (cap exceeded) |
| `409` | Rejected — no session is active, or `sessionId` doesn't match the currently active session | `{ "result": "rejected", "reason": "No fast polling session is active." }` or `"...does not match the currently active session."` |

Checks run in this order: field validation (`400`) → session-id match (`409`) → 30-minute total-duration cap (`400`).

**Worked example** — a session starts at `T=0` with `sessionDurationMinutes=10` (so it would normally auto-expire at `T=10`). At `T=5`, `Extend-FastPolling` is called with `extendSessionDurationMinutes=10`:
- Current total duration is 10 min; `10 + 10 = 20` min, which is within the 30-minute cap, so it's accepted.
- The session's total duration becomes 20 min — measured from its original start (`T=0`), **not** from the extend call (`T=5`). The new expiry is `T=0 + 20min = T=20`.
- The response is `{ "result": "accepted", "sessionId": "...", "extendedByMinutes": 10, "totalDurationMinutes": 20 }`.
- The session now ends at **T=20**: 15 minutes after the extend call, and 10 minutes later than its original T=10 expiry — extending "by 10 minutes" always adds 10 minutes to the total planned duration, regardless of when during the session you call it.

Internally, `FastPollingSession.TryExtend` atomically checks the cap and, on success, recomputes the remaining time from the *original* start and reschedules the same `CancellationTokenSource` via `cts.CancelAfter(remaining)` — there's only ever one expiry timer per session, just rescheduled.

A successful extend also patches the twin — see [Session Exit & Twin Reporting](#session-exit--twin-reporting).

## How Fast Polling Works

1. `PollingWorker` writes every incoming `LiveTelemetry` message into `LatestTelemetryCache` (a simple thread-safe single-slot cache: `Volatile.Write`/`Read`, no locking) — regardless of whether fast polling is active.
2. `FastPollingEnable` validates its payload, then:
   - For each of `sessionDurationMinutes`/`pollIntervalSeconds`: if it was supplied but is outside the allowed range, that's a validation error; if it was supplied and in range, that value is used; if it was omitted (or the whole payload was missing/unparseable), its default (10 min / 1 sec respectively) is used instead and noted in `defaultsApplied`.
   - Rejects (`400`) only if at least one *supplied* value was out of range (or the payload was structurally invalid in a way that couldn't even be treated as "no params") — never for a value simply being absent.
   - Otherwise atomically tries to claim `_activeSession` via `Interlocked.CompareExchange`; if one is already set, rejects (`409`).
   - Otherwise creates a `FastPollingSession` — a new `Guid SessionId`, a `CancellationTokenSource` linked to the module's host-shutdown token, and the resolved (supplied-or-default) duration/interval — reports the session as started on the twin (see [Session Exit & Twin Reporting](#session-exit--twin-reporting)), and only then starts the background loop (`RunFastPollingLoopAsync`) via fire-and-forget `Task.Run`.
3. The loop uses a `PeriodicTimer` set to the session's configured interval. On every tick:
   - If `LatestTelemetryCache.Current` is still `null` (no telemetry has arrived yet), the tick is skipped (debug log only) — nothing is sent, no state advances, and nothing is added to the batch.
   - Otherwise the snapshot is compared against `lastSent` (the previous tick's snapshot, `null` on the first tick of the session) and the resulting entry (`Body` on the first tick, `Changes` afterwards — see [Sample Payloads](#sample-telemetry-payloads) below) is appended to a `TelemetryBatcher<object>`, not sent immediately.
   - `lastSent` is updated to the new snapshot after every tick is added to the batch (whether or not the eventual batch send actually reaches IoT Hub — see [Known limitation](#known-limitation-delivery-is-not-guaranteed) below).
   - Once the batcher accumulates 5 entries, they're sent as one gzip-compressed message to IoT Hub output `output1` and the batch buffer is cleared — see [Batching & Compression](#batching--compression).
4. The session's duration is enforced via `cts.CancelAfter(session.TotalDuration)` on the same token the loop awaits on — so the loop exits automatically once the configured window elapses, or immediately if `FastPollingDisable` cancels it early, or if the module itself is shutting down.
5. The session's `PollInterval` is fixed for the life of the session (captured once, at creation) — to use a different interval you must let the session end (or `FastPollingDisable` it) and call `FastPollingEnable` again. `TotalDuration`, however, can grow mid-session: `Extend-FastPolling` atomically extends it (up to the 30-minute cap) and reschedules the same expiry timer — see [`Extend-FastPolling`](#extend-fastpolling) above.

Only one session can be active module-wide at any time.

## Batching & Compression

Ticks are no longer sent individually. Each tick's payload (`Body` on the session's first tick, `Changes` afterwards, exactly as before) is appended to an in-memory `TelemetryBatcher<object>` (`Services/TelemetryBatcher.cs`) with a fixed batch size of 5 (`FastPollingService.BatchSize`). The batcher is a small, generic, non-thread-safe buffer — it hands back a completed batch of items once it fills, and can be `Flush()`-ed to return whatever is currently buffered.

- **Full batch**: once the 5th entry is added, all 5 are sent together as one message (`SendBatchAsync`), and the buffer resets for the next 5 ticks.
- **Partial batch on session exit**: whenever the polling loop exits — manual disable, auto-expiry, host shutdown, or an unexpected fault — the loop's `finally` block calls `batcher.Flush()` and sends whatever 1–4 entries are left, so a session ending mid-batch never silently drops telemetry. This flush send deliberately uses `CancellationToken.None` rather than the session's own (already-cancelled) token, so it isn't aborted by the very cancellation that ended the loop; it's still bounded by `EdgeHubPublisher`'s existing 30s send timeout.
- **Compression**: every batch send passes `compress: true` to `IMessagePublisher.SendMessageAsync`. `EdgeHubPublisher` gzips the serialized JSON bytes (`Services/GzipCompressor.cs`, `GZipStream` at `CompressionLevel.Optimal`) and sets `Message.ContentEncoding = "gzip"` on the outgoing IoT Hub message before sending. This is opt-in per call (`compress` defaults to `false`) — the normal slow-poll path (`Workers/PollingWorker.cs`) is unaffected and still sends uncompressed.

There is currently no in-repo downstream consumer of these messages (they route straight to IoT Hub's `$upstream`); any consumer built later needs to check `ContentEncoding` and gzip-decompress before parsing JSON.

## Sample Telemetry Payloads

Each batch message carries three properties: `batchSentUtc` (when the batch was sent), `session-id` (the session's GUID), and `telemetry-type: fast-polling`, plus `Message.ContentEncoding: "gzip"` (see [Batching & Compression](#batching--compression)). The body is `{ "Entries": [...] }`, an array of up to 5 per-tick entries, each carrying its own `TimestampUtc`. Whether an entry is a baseline, a delta, or a heartbeat is conveyed purely by the shape of the entry (`Body` vs. `Changes` key) — no extra property is needed to tell them apart. The baseline entry always also carries an `Event` field (current event state, see [Event State Input](#event-state-input)) as a sibling of `Body`; a delta entry gets that same `Event` field as a sibling of `Changes` (not nested inside it) only on a tick where event state actually changed. The examples below show the JSON **before** gzip compression is applied.

**A batch spanning a session's first 5 ticks** — entry 0 is the full baseline snapshot plus current event state for every active/known param (unconditionally, so a consumer always has a complete starting point); entries 1–4 are deltas, including an empty one where nothing changed (a heartbeat) and one where two events change state (one clears, another triggers) in the same tick:
```json
{
  "Properties": {
    "batchSentUtc": "2026-09-15T00:03:23.6159289Z",
    "session-id": "cc27db6b-508b-488e-a816-faae684491d8",
    "telemetry-type": "fast-polling"
  },
  "ContentEncoding": "gzip",
  "Entries": [
    {
      "TimestampUtc": "2026-09-15T00:03:19.6159289Z",
      "Body": {
        "MotorRunning": true,
        "OperatingPressurePSI": 54351,
        "OperatingPressureBar": 3747,
        "...": "...(all ~60 TelemetryPayload fields)"
      },
      "Event": {
        "OperatingAlarmActive": 1,
        "OperatingWarningActive": 3,
        "MaintenanceAlarmActive": 0
      }
    },
    {
      "TimestampUtc": "2026-09-15T00:03:20.6159289Z",
      "Changes": { "OperatingPressurePSI": 54360, "StrokeRateLeft": 97 }
    },
    {
      "TimestampUtc": "2026-09-15T00:03:21.6159289Z",
      "Changes": { },
      "Event": { "OperatingWarningActive": 1, "MaintenanceAlarmActive": 2 }
    },
    {
      "TimestampUtc": "2026-09-15T00:03:22.6159289Z",
      "Changes": { "StrokeRateLeft": 98 }
    },
    {
      "TimestampUtc": "2026-09-15T00:03:23.6159289Z",
      "Changes": {}
    }
  ]
}
```

**A later batch (session already past its baseline)** — every entry is a `Changes` delta or heartbeat, same as above minus the `Body` entry.

**A trailing partial batch flushed at session end** — same shape, just fewer than 5 entries (1–4), sent when the session ends mid-batch (see [Batching & Compression](#batching--compression)).

## Event State Input

`EventModeModule` (a sibling IoT Edge module, external to this repo) detects discrete events — e.g. an alarm triggering or clearing. `DefaultModeModule` now learns about them too, so fast-polling telemetry surfaces these events alongside PLC telemetry — a consumer watching the fast-polling stream doesn't also need to separately watch `EventModeModule`'s own output to know an event occurred.

**`events` input.** `FastPollingService` registers an input handler (alongside its three direct-method registrations, in the same `ExecuteAsync`) subscribed to an input named `events`, fed by `deployment.template.json`'s `EventModeToDefaultMode` route from `EventModeModule`. Payload shape — a JSON object of one or more param entries:
```json
{ "<param-name>": "triggered" }
```
or, multiple at once:
```json
{ "paramA": "triggered", "paramB": "cleared" }
```
Handled (`FastPollingService.OnEventReceived`) with the same poison-message convention already used for `PollingWorker`'s `LiveTelemetry` input (`Workers/PollingWorker.cs:139-190`): a malformed or null payload is logged (with a payload preview and `MessageId`/`SequenceNumber`, same as `DropPoisonMessage`) and acknowledged (`MessageResponse.Completed`) rather than redelivered, since retrying can't fix a payload that was never valid; any other unexpected error returns `MessageResponse.Abandoned` so EdgeHub redelivers. State strings (`"triggered"`/`"cleared"`) are stored verbatim with no allow-list validation — `EventModeModule` is the trusted source of valid values, and an individual entry with a null/empty value is skipped (logged) without dropping the rest of the message.

**`EdgeHubPublisher` fix (prerequisite).** `EdgeHubPublisher` used to store its one registered input handler (`LiveTelemetry`, for `PollingWorker`) in scalar fields, replayed on reconnect. Registering a second input (`events`) needed that converted to a list (`_savedInputHandlers`), mirroring the pattern already used for method handlers (`_savedMethodHandlers`) — otherwise the second registration would have silently clobbered the first's reconnect bookkeeping.

**`LatestEventStateCache`** (`Services/LatestEventStateCache.cs`). Mirrors `Services/LatestTelemetryCache.cs`'s minimal style, but holds a `Dictionary<string,string>` (param name → last-known state) instead of a single snapshot, since individual keys are updated independently — guarded by a lock around mutation, with a defensive-copy `Snapshot()` for readers. Registered as a singleton with the module's whole lifetime, **independent of fast-polling session boundaries** — it is not reset when a session starts or ends, exactly like `LatestTelemetryCache`.

**How event state surfaces in batch entries:**
- **Baseline entry** (a session's first tick) gets a top-level `Event` field, sibling to `Body`, holding the full current event-state snapshot — always present, even if empty — so a fresh session is self-describing about already-active events, not just ones that change later:
  ```json
  {
    "TimestampUtc": "2026-09-15T00:03:19.6159289Z",
    "Body": { "...": "...(full TelemetryPayload snapshot)" },
    "Event": {
      "OperatingAlarm": "triggered",
      "OperatingWarning": "triggered",
      "MaintenanceAlarm": "cleared"
    }
  }
  ```
- **Delta entries** get that same top-level `Event` field, sibling to `Changes` (**not** nested inside it — `Changes` only ever holds `TelemetryPayload` property names), holding only the params whose state changed since the last tick (`FastPollingService.DiffEventState`) — same "only report what changed" convention `ComputeChanges` already applies to telemetry properties. Absent entirely on a tick where no event changed. A single tick can carry more than one changed param at once, e.g. one clearing while another triggers:
  ```json
  {
    "TimestampUtc": "2026-09-15T00:03:21.6159289Z",
    "Changes": { "StrokeRateLeft": 98 },
    "Event": { "OperatingWarning": "cleared", "MaintenanceAlarm": "triggered" }
  }
  ```
- Internally this uses a per-session `lastSentEventState` (analogous to `lastSent`), updated after every tick — including the baseline tick — so the first delta tick doesn't spuriously report every already-active event as "just changed." Event state is never "removed" — a param only ever gets an explicit new value via `LatestEventStateCache.Apply` — so `DiffEventState` never needs to detect a key disappearing.

**Known limitation:** the `EventModeToDefaultMode` route's `EventModeModule`-side output name (`events`) is a placeholder — `EventModeModule`'s source isn't in this repo, so this needs to be confirmed/renamed to match whatever that module actually calls the output carrying these payloads.

## Session Exit & Twin Reporting

A session starts one way and can end four different ways; each is logged distinctly (no exit path is ever mislabeled as another), and the twin's reported properties are updated for the start and for exactly two of the four exit paths — manual disable and auto-expiry — each specifying how it ended. Host shutdown and an unexpected fault are logged but deliberately do **not** touch the twin.

**Start** — reported synchronously in `OnFastPollingEnable`, right before the polling loop is kicked off (and before the `200` response is returned):
```json
{
  "FastPolling": {
    "Active": true,
    "SessionId": "cc27db6b-508b-488e-a816-faae684491d8",
    "StartedUtc": "2026-09-15T00:03:19.0000000Z",
    "DurationMinutes": 15,
    "IntervalSeconds": 5
  }
}
```

| Cause | How it's detected | Logged as | Twin reported property updated? |
|---|---|---|---|
| **Manual disable** (`FastPollingDisable` called) | `session.ManuallyDisabled` flag set before cancelling | `"Fast polling session {id} disabled by direct method call."` | **Yes** — `StopReason: "ManualDisable"`, reported synchronously in `OnFastPollingDisable`, before its `200` response |
| **Auto-expiry** (configured duration elapsed) | Cancellation observed, and it wasn't manual disable or host shutdown → `session.TimedOut = true` | `"Fast polling session auto-expired after {duration} min."` | **Yes** — `StopReason: "AutoExpiry"`, reported from the polling loop's `finally` block |
| **Host shutdown** (module stopping) | Cancellation observed via the linked host token, not manual, not a timeout | `"Fast polling session stopped because the host is shutting down."` | No |
| **Unexpected fault** (an unhandled exception escapes the loop) | Generic `catch (Exception)` → `session.Faulted = true` | `"Fast polling loop terminated unexpectedly."` (with the exception) | No |

**Manual disable** report:
```json
{
  "FastPolling": {
    "Active": false,
    "SessionId": "cc27db6b-508b-488e-a816-faae684491d8",
    "StopReason": "ManualDisable",
    "LastStoppedUtc": "2026-09-15T00:08:42.0000000Z"
  }
}
```

**Auto-expiry** report:
```json
{
  "FastPolling": {
    "Active": false,
    "SessionId": "cc27db6b-508b-488e-a816-faae684491d8",
    "StopReason": "AutoExpiry",
    "LastStoppedUtc": "2026-09-15T00:18:19.0000000Z"
  }
}
```

`SessionId` is included on every report (start and both stop kinds) so the twin's history can be correlated with a specific session and with that session's `session-id` telemetry property. If a session ends via host shutdown or a fault, the twin is left as-is from its last update — for those two cases, rely on the module logs, not the twin, to see that it stopped.

**Mid-session extend** (`Extend-FastPolling` succeeds) also patches the twin, but minimally — only the total duration, nothing else:
```json
{
  "FastPolling": {
    "DurationMinutes": 20
  }
}
```
This merges into the existing `FastPolling` object rather than replacing it — `Active`, `SessionId`, `StartedUtc`, etc. from the start report are untouched.

`_activeSession` is cleared (via `Interlocked.CompareExchange`) in every exit path's `finally` block, so a new `FastPollingEnable` call is always accepted once a prior session has actually finished exiting — regardless of which of the four ways it ended.

## Edge Cases & Assumptions Handled

- **Concurrent Enable calls**: only one wins — enforced atomically via `Interlocked.CompareExchange` on `_activeSession`, not a lock, so there's no window where two sessions could both start.
- **No telemetry yet**: if `LatestTelemetryCache.Current` is `null` (module just started, no `LiveTelemetry` received), ticks are silently skipped until data arrives — nothing is sent, `lastSent` doesn't advance, and the next real snapshot still becomes that session's baseline.
- **Malformed/empty request payload**: no longer a validation failure — `JsonException` from deserializing `request.DataAsJson` is caught and the payload is treated exactly like "no params supplied," falling back to both defaults, rather than propagating or rejecting.
- **Missing vs. invalid, handled differently on purpose**: omitting `sessionDurationMinutes`/`pollIntervalSeconds` is not an error (falls back to the default, reported via `defaultsApplied`); *supplying* a value outside 10–30/1–60 is still a `400` rejection. The two are intentionally not conflated — a caller who never mentions a field gets a sane default silently, while a caller who explicitly asks for something out of range is told so.
- **Race between auto-expiry and a `Disable` call**: `Disable` calling `Cancel()` on a `CancellationTokenSource` whose loop has already exited is a harmless no-op. The `Cts` is deliberately never disposed by the loop itself (only ever cancelled), specifically so a racing `Disable` can't hit an `ObjectDisposedException`.
- **Fire-and-forget loop**: the polling loop runs via `_ = Task.Run(...)`, not awaited by the method handler — so `FastPollingEnable` returns immediately without waiting for the whole session to run.
- **Session-length limits enforced twice conceptually, but validated in one place**: `sessionDurationMinutes`/`pollIntervalSeconds` are no longer twin-configurable at all; validation happens exactly once, in `ValidateEnableRequest`, at the moment `FastPollingEnable` is called.
- **Session ends mid-batch**: fewer than 5 ticks may have accumulated when the session exits (any of the four ways). The `finally` block flushes that partial batch (1–4 entries) before reporting the exit, so a short session — or one disabled right after it starts — still delivers whatever telemetry it collected instead of losing it silently. See [Batching & Compression](#batching--compression).
- **Event state is independent of fast-polling sessions**: `LatestEventStateCache` keeps receiving and applying `events` messages regardless of whether a session is active — it's not started/stopped/reset with a session. A session that starts later still sees whatever event state accumulated while no session was running, via its baseline entry's `Event` field.
- **Malformed `events` payload**: handled exactly like a malformed `LiveTelemetry` payload — logged and acknowledged (`MessageResponse.Completed`) rather than redelivered, so one bad `events` message can't jam the input or block subsequent valid ones. See [Event State Input](#event-state-input).
