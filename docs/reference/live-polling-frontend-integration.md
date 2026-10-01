# Live Polling API Reference

Backend API and SignalR message contract for on-demand live telemetry sessions.

**Status:** implemented on `feature/live-mode`, not yet deployed. See [Deployment prerequisites](#deployment-prerequisites).

### Changed on 2026-09-30

If you started against an earlier revision of this document, three things moved:

- **Device status is an integer everywhere**: `0` Normal, `1` Warning, `2` Alarm. That covers
  `status[]` on the series endpoint, `deviceStatus` on `GET /live-session`, and
  `entries[].deviceStatus` on SignalR. All three were strings in earlier revisions. See
  [`deviceStatus`](#devicestatus).
- **`events` is new**, on both the series response and each SignalR entry. It carries alarm,
  warning and motor transitions with a human label for the two operational-domain parameters.
- **Every key in a SignalR entry is always present**, including `snapshot`, `changes` and `events`.
  Absent data is an explicit `null`, never a missing key. Earlier examples in this document showed
  the keys omitted; that was wrong.

---

## Conventions

| | |
|---|---|
| Base path | `/api/v{version}/devices/{serialNumber}/live-session` |
| Version | `1.0` — use `v1` |
| Authentication | Bearer token, as for all backend endpoints |
| Authorisation | Privilege `LivePolling` (roles `Admin`, `Engineering`, `Service`). Missing → `403` |
| Content type | `application/json` |
| Property casing | camelCase |
| Dates | ISO 8601 UTC, e.g. `2026-09-28T10:27:00.1230000Z` |

### Response envelope

Every response, success or failure, is wrapped:

| Field | Type | Nullable | Description |
|---|---|---|---|
| `succeeded` | boolean | no | `true` on 2xx |
| `message` | string | yes | Human-readable summary. Present on most failures |
| `errors` | array&lt;string&gt; | yes | Omitted when null. **`errors[0]` is the error code**; later elements are detail |
| `data` | object | yes | Payload; `null` on failure |

```json
{
  "succeeded": false,
  "message": "A live session is already active on this device until 2026-09-28T10:42:00Z.",
  "errors": ["LIVE_SESSION_ALREADY_ACTIVE"]
}
```

### Common path parameter

| Name | In | Type | Required | Description |
|---|---|---|---|---|
| `serialNumber` | path | string | yes | Device serial number. **Not** the DeviceGuid |

### Common error responses

Applies to every endpoint below.

| HTTP | `errors[0]` | Condition |
|---|---|---|
| 403 | — | Caller lacks the `LivePolling` privilege |
| 404 | `DEVICE_NOT_FOUND` | No device with that serial number |
| 404 | `GATEWAY_NOT_PAIRED` | Device exists but has no active gateway pairing |
| 503 | `GATEWAY_UNREACHABLE` | Direct method to the gateway timed out, or the device is offline |

---

## POST /live-session

Starts a session.

### Request

`POST /api/v1/devices/{serialNumber}/live-session`

**Body** — optional. Send `{}`, omit the body, or omit either field; each omitted field falls back to
a server-side default. A field that *is* supplied and out of range returns `400`.

| Field | Type | Required | Range | Default |
|---|---|---|---|---|
| `pollIntervalSeconds` | integer | no | 1–60 | server configured |
| `sessionDurationMinutes` | integer | no | 10–30 | server configured |

```json
{ "pollIntervalSeconds": 1, "sessionDurationMinutes": 15 }
```

### 200 response — `data`

| Field | Type | Nullable | Description |
|---|---|---|---|
| `sessionId` | string (uuid) | no | Session identifier, also the SignalR `sessionId` |
| `deviceId` | string | no | Echo of `serialNumber` |
| `pollIntervalSeconds` | integer | no | Interval the gateway accepted — may differ from the request |
| `sessionDurationMinutes` | integer | no | Duration the gateway accepted |
| `startedUtc` | string (date-time) | no | |
| `expiresUtc` | string (date-time) | no | Authoritative session end |
| `totalDurationMinutes` | integer | no | Total planned duration, measured from `startedUtc` |
| `canExtend` | boolean | no | `false` once `totalDurationMinutes` reaches the 30-minute cap |

```json
{
  "succeeded": true,
  "data": {
    "sessionId": "cc27db6b-508b-488e-a816-faae684491d8",
    "deviceId": "PUMP-00417",
    "pollIntervalSeconds": 1,
    "sessionDurationMinutes": 15,
    "startedUtc": "2026-09-28T10:27:00Z",
    "expiresUtc": "2026-09-28T10:42:00Z",
    "totalDurationMinutes": 15,
    "canExtend": true
  }
}
```

### Errors

| HTTP | `errors[0]` | Condition |
|---|---|---|
| 400 | `LIVE_SESSION_INVALID_PARAMETERS` | Supplied value out of range. `errors[1..]` name each offending field and its range |
| 409 | `LIVE_SESSION_ALREADY_ACTIVE` | This device already has a session |
| 409 | `LIVE_SESSION_CAPACITY_REACHED` | Fleet-wide concurrent session cap reached |

---

## GET /live-session

Returns session state and the most recent values.

### Request

`GET /api/v1/devices/{serialNumber}/live-session`

No query parameters, no body.

### 200 response — `data`

Returned with `200` whether or not a session is running. When `active` is `false`, every other
field is `null`.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `active` | boolean | no | |
| `sessionId` | string (uuid) | yes | |
| `pollIntervalSeconds` | integer | yes | |
| `expiresUtc` | string (date-time) | yes | |
| `secondsRemaining` | integer | yes | Floor 0 |
| `totalDurationMinutes` | integer | yes | |
| `canExtend` | boolean | yes | |
| `deviceStatus` | integer | yes | `0` Normal, `1` Warning, `2` Alarm. **`null` before the first tick** — which is not the same as `0` |
| `snapshot` | object | yes | Field name → value, all 55 fields. `null` before the first tick |
| `snapshotTimestampUtc` | string (date-time) | yes | Timestamp of the tick `snapshot` reflects |
| `tickCount` | integer | yes | Ticks accepted so far this session |
| `bytesTotal` | integer (int64) | yes | Compressed bytes received from the gateway so far |

```json
{
  "succeeded": true,
  "data": {
    "active": true,
    "sessionId": "cc27db6b-508b-488e-a816-faae684491d8",
    "pollIntervalSeconds": 1,
    "expiresUtc": "2026-09-28T10:42:00Z",
    "secondsRemaining": 412,
    "totalDurationMinutes": 15,
    "canExtend": true,
    "deviceStatus": 1,
    "snapshot": { "MotorRunning": true, "OperatingPressurePSI": 54372, "StrokeRateLeft": 97 },
    "snapshotTimestampUtc": "2026-09-28T10:35:08.1230000Z",
    "tickCount": 412,
    "bytesTotal": 97644
  }
}
```

Inactive:

```json
{ "succeeded": true, "data": { "active": false } }
```

### Errors

Common errors only.

---

## POST /live-session/extend

Extends the running session's total duration.

### Request

`POST /api/v1/devices/{serialNumber}/live-session/extend`

No query parameters. **No body** — the number of minutes is server-configured.

### 200 response — `data`

Same schema as `POST /live-session`. `expiresUtc` and `totalDurationMinutes` carry the new values.

Total duration is measured from `startedUtc`, not from the time of the call: a session started at
10:00 for 10 minutes and extended at 10:05 ends at **10:20**.

```json
{
  "succeeded": true,
  "data": {
    "sessionId": "cc27db6b-508b-488e-a816-faae684491d8",
    "deviceId": "PUMP-00417",
    "pollIntervalSeconds": 1,
    "sessionDurationMinutes": 25,
    "startedUtc": "2026-09-28T10:27:00Z",
    "expiresUtc": "2026-09-28T10:52:00Z",
    "totalDurationMinutes": 25,
    "canExtend": true
  }
}
```

### Errors

| HTTP | `errors[0]` | Condition |
|---|---|---|
| 400 | `LIVE_SESSION_EXTENSION_LIMIT_REACHED` | Extension would exceed the gateway's 30-minute total cap. `errors[1]` carries the gateway's wording |
| 409 | `LIVE_SESSION_NOT_ACTIVE` | No session running, or the gateway's session no longer matches |

---

## DELETE /live-session

Stops the running session early.

### Request

`DELETE /api/v1/devices/{serialNumber}/live-session`

No query parameters, no body.

### 200 response

`data` is `null`.

```json
{ "succeeded": true, "message": "Live session stopped." }
```

A session that reaches `expiresUtc` stops on the gateway by itself; this call is only for stopping
early. A `200` is returned even if the gateway reports it had already stopped.

### Errors

| HTTP | `errors[0]` | Condition |
|---|---|---|
| 409 | `LIVE_SESSION_NOT_ACTIVE` | No session running |

On `503 GATEWAY_UNREACHABLE` the session record is left open.

---

## GET /live-session/series

Time series for charting.

### Request

`GET /api/v1/devices/{serialNumber}/live-session/series`

| Name | In | Type | Required | Default | Description |
|---|---|---|---|---|---|
| `metric` | query | string, repeatable | no | server-configured set of 4 | Metric name. Repeat the parameter for several. Maximum **10** per request. Each must match a `dbo.ParameterRegistry` name (case-insensitive), or the synthetic name `DeviceStatus` |
| `since` | query | string (date-time) | no | session start | Lower bound; shortens the returned grid |

Repeat `metric` to request several — comma-separated values are **not** supported and are read as a
single, invalid metric name.

```
GET /api/v1/devices/PUMP-00417/live-session/series
GET /api/v1/devices/PUMP-00417/live-session/series?metric=OperatingPressurePSI
GET /api/v1/devices/PUMP-00417/live-session/series?metric=OperatingPressurePSI&metric=StrokeRateLeft
GET /api/v1/devices/PUMP-00417/live-session/series?metric=OperatingPressurePSI&since=2026-09-28T10:31:00Z
```

`series` comes back in the order the metrics were requested. Duplicates are collapsed.

### 200 response — `data`

| Field | Type | Nullable | Description |
|---|---|---|---|
| `sessionId` | string (uuid) | no | |
| `pollIntervalSeconds` | integer | no | Spacing of `t` |
| `startedUtc` | string (date-time) | no | |
| `lastTickUtc` | string (date-time) | yes | |
| `tickCount` | integer | no | |
| `t` | array&lt;string (date-time)&gt; | no | Shared timestamp grid |
| `status` | array&lt;integer&gt; | no | Device status per grid point: `0` Normal, `1` Warning, `2` Alarm. Same length as `t` |
| `series` | array&lt;object&gt; | no | One entry per requested metric |
| `series[].metric` | string | no | Metric name |
| `series[].v` | array | no | Values per grid point. Same length as `t`. Element `null` = not yet reported |
| `events` | array&lt;object&gt; | no | Event occurrences during the session, oldest first. Empty when none occurred |

`t`, `status` and every `series[].v` are index-aligned and the same length. Values are already
carried forward across ticks where the metric did not change, so the grid has no holes.

`status` and `events` are returned on **both** the default call and the `?metric=` call. The chart
draws the status column and the event markers regardless of which metrics are selected, so filtering
the metric list must not make them disappear.

`?since=` shortens the grid and filters transitions, but **`active` events are always returned**,
whatever `since` is. They describe the condition the session opened in, which is also what the
carried-forward `status[]` is showing — so a late `since` never leaves you with a red status column
and no event naming the alarm.

**`events[]` is not grid-aligned.** These are discrete occurrences with their own timestamps, they
are never carried forward, and two events may share one timestamp when two parameters transitioned
on the same tick.

| Field | Type | Nullable | Description |
|---|---|---|---|
| `events[].timestampUtc` | string (date-time) | no | Tick the transition was reported on |
| `events[].parameter` | string | no | Telemetry parameter name, e.g. `OperatingAlarmActive` |
| `events[].value` | integer | no | Raw register value, e.g. `5` |
| `events[].state` | string | no | `triggered` \| `cleared` \| `active` |
| `events[].eventType` | string | no | `alarm` \| `warning` \| `maintenance` \| `status` \| `unknown` |
| `events[].domain` | string | no | `operational` \| `maintenance` \| `unknown` |
| `events[].severity` | string | no | `critical` \| `major` \| `minor` \| `unknown` |
| `events[].label` | string | yes | Human text for this value. **Non-null only for `OperatingAlarmActive` and `OperatingWarningActive`** |

Use `parameter` + `value` as the stable identity. `label` is display text an administrator can edit
in the database, and is an explicit `null` — not an omitted key — for 16 of the 18 alert parameters.
Render from `eventType` and `domain` when it is null.

The two events above share a timestamp, which is legal — two parameters transitioned on the same
tick. The second shows the unlabelled shape.

A session that opened while a condition was already firing looks like this instead — note that
`status` starts at `2` and the event is `active`, carrying the label but describing no transition:

```json
{
  "t": ["2026-09-28T10:27:01Z", "2026-09-28T10:27:02Z", "2026-09-28T10:27:03Z"],
  "status": [2, 2, 2],
  "events": [
    { "timestampUtc": "2026-09-28T10:27:01Z",
      "parameter": "OperatingAlarmActive", "value": 5, "state": "active",
      "eventType": "alarm", "domain": "operational", "severity": "critical",
      "label": "Emergency Stop Active" }
  ]
}
```

```json
{
  "succeeded": true,
  "data": {
    "sessionId": "cc27db6b-508b-488e-a816-faae684491d8",
    "pollIntervalSeconds": 1,
    "startedUtc": "2026-09-28T10:27:00Z",
    "lastTickUtc": "2026-09-28T10:34:13.1230000Z",
    "tickCount": 432,
    "t": ["2026-09-28T10:27:01Z", "2026-09-28T10:27:02Z", "2026-09-28T10:27:03Z"],
    "status": [0, 0, 1],
    "series": [
      { "metric": "OperatingPressurePSI", "v": [54351, 54360, 54360] }
    ],
    "events": [
      { "timestampUtc": "2026-09-28T10:27:03.1230000Z",
        "parameter": "OperatingWarningActive", "value": 21, "state": "triggered",
        "eventType": "warning", "domain": "operational", "severity": "major",
        "label": "Left Overstroke Warning" },
      { "timestampUtc": "2026-09-28T10:27:03.1230000Z",
        "parameter": "MaintenanceAlarmActive", "value": 1, "state": "triggered",
        "eventType": "alarm", "domain": "maintenance", "severity": "critical",
        "label": null }
    ]
  }
}
```

### Errors

| HTTP | `errors[0]` | Condition |
|---|---|---|
| 400 | `LIVE_SESSION_INVALID_PARAMETERS` | An unknown or blank `metric`, or more than 10. `errors[1..]` name each offending value, then list every valid metric |
| 409 | `LIVE_SESSION_NOT_ACTIVE` | No session running. Series are deleted when a session ends and are not retrievable afterwards |

```json
{
  "succeeded": false,
  "message": "Unknown metric requested.",
  "errors": [
    "LIVE_SESSION_INVALID_PARAMETERS",
    "'OperatingPresurePSI' is not a known metric.",
    "Valid metrics: DeviceStatus, HydraulicOilTemperatureC, MotorRunning, OperatingPressurePSI, …"
  ]
}
```

A metric that is valid but has not reported during this session returns `null` for every grid point
rather than an error — that is a real answer, not a rejection.

---

## SignalR message contract

### Connection

Provided by the socket negotiator function app, not the backend API.

| Method | Path | Params | Header |
|---|---|---|---|
| POST | `/api/negotiate` | — | `x-user-id` |
| POST | `/api/join-device` | `deviceId` (query, required) | `x-user-id` |
| POST | `/api/leave-device` | `deviceId` (query, required) | `x-user-id` |

`deviceId` must be the **DeviceGuid**, not the serial number: it is used verbatim as the group name
`telemetry:device:{deviceId}`, and the publisher uses the DeviceGuid. A mismatch produces a
successful subscription to a group that never receives messages.

| | |
|---|---|
| Hub | `iot_telemetry` |
| Group | `telemetry:device:{deviceGuid}` |
| Target | `liveTelemetryUpdate` |
| Frequency | One message per 5 ticks — about every 5 seconds at a 1-second interval |

`liveTelemetryUpdate` is distinct from `telemetryUpdate`, which continues to carry regular
10-minute telemetry on the same group.

### Message envelope

The single argument to `liveTelemetryUpdate`:

| Field | Type | Description |
|---|---|---|
| `encoding` | string | Always `"gzip+base64"` |
| `payload` | string | Base64 of the gzip-compressed batch JSON |

```json
{ "encoding": "gzip+base64", "payload": "H4sIAAAAAAAA..." }
```

Decode: base64 → gzip inflate → UTF-8 → JSON. In the browser, `DecompressionStream("gzip")`
(Chrome 80+, Safari 16.4+, Firefox 113+).

### Decoded batch

| Field | Type | Nullable | Description |
|---|---|---|---|
| `sessionId` | string (uuid) | no | Matches the REST `sessionId` |
| `deviceGuid` | string (uuid) | no | |
| `batchSentUtc` | string (date-time) | no | When the gateway sent the batch |
| `entries` | array&lt;object&gt; | no | 1–5 entries, ascending by `timestampUtc` |
| `entries[].timestampUtc` | string (date-time) | no | When the gateway sampled this tick |
| `entries[].deviceStatus` | integer | no | `0` Normal, `1` Warning, `2` Alarm at this tick |
| `entries[].snapshot` | object | yes | All 55 fields. Non-null on `entries[0]` only |
| `entries[].changes` | object | yes | Fields changed since the previous tick. Non-null on `entries[1..]` only |
| `entries[].events` | array&lt;object&gt; | yes | Event transitions on this tick. `null` on ticks with no transition — which is almost all of them |

**All three keys are always present.** The server serialises nulls rather than omitting keys, so
`'changes' in entry` and `'events' in entry` are both always true and tell you nothing. Test the
value: `if (entry.events)`, not `if ('events' in entry)`.

Exactly one of `snapshot` and `changes` is non-null on each entry. `entries[0]` always carries a
complete `snapshot`, on every batch. `"changes": {}` — an empty object, not null — is a tick where
nothing changed.

`entries[].events` carries the same object as the series endpoint's `events[]`, minus
`timestampUtc` — the entry's own `timestampUtc` is the event's:

| Field | Type | Nullable | Description |
|---|---|---|---|
| `parameter` | string | no | Telemetry parameter name |
| `value` | integer | no | Raw register value |
| `state` | string | no | `triggered` \| `cleared` \| `active` |
| `eventType` | string | no | `alarm` \| `warning` \| `maintenance` \| `status` \| `unknown` |
| `domain` | string | no | `operational` \| `maintenance` \| `unknown` |
| `severity` | string | no | `critical` \| `major` \| `minor` \| `unknown` |
| `label` | string | yes | Non-null only for the two operational-domain parameters |

A batch may contain fewer than 5 entries when a session ends mid-batch.

```json
{
  "sessionId": "cc27db6b-508b-488e-a816-faae684491d8",
  "deviceGuid": "9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284",
  "batchSentUtc": "2026-09-28T10:27:05.1900000Z",
  "entries": [
    { "timestampUtc": "2026-09-28T10:27:01.2200000Z", "deviceStatus": 0,
      "snapshot": { "MotorRunning": true, "OperatingPressurePSI": 54351, "StrokeRateLeft": 96 },
      "changes": null,
      "events": null },
    { "timestampUtc": "2026-09-28T10:27:02.2180000Z", "deviceStatus": 0,
      "snapshot": null,
      "changes": { "OperatingPressurePSI": 54360 },
      "events": null },
    { "timestampUtc": "2026-09-28T10:27:03.1230000Z", "deviceStatus": 0,
      "snapshot": null,
      "changes": {},
      "events": null },
    { "timestampUtc": "2026-09-28T10:27:04.2010000Z", "deviceStatus": 1,
      "snapshot": null,
      "changes": { "StrokeRateLeft": 97, "OperatingWarningActive": 21 },
      "events": [
        { "parameter": "OperatingWarningActive", "value": 21, "state": "triggered",
          "eventType": "warning", "domain": "operational", "severity": "major",
          "label": "Left Overstroke Warning" }
      ] },
    { "timestampUtc": "2026-09-28T10:27:05.1900000Z", "deviceStatus": 1,
      "snapshot": null,
      "changes": {},
      "events": null }
  ]
}
```

Delivery is not guaranteed. A dropped batch needs no recovery: the next batch carries a full
snapshot.

---

## Enumerations

### `deviceStatus`

An integer on every channel: `status[]` in the series response, `deviceStatus` on
`GET /live-session`, and `entries[].deviceStatus` in the SignalR batch. One encoding, no branching
on where the value came from.

| Value | Meaning | Derived from |
|---|---|---|
| `2` | Alarm | `OperatingAlarmActive > 0` or `MaintenanceAlarmActive` set |
| `1` | Warning | `OperatingWarningActive > 0` or `MaintenanceWarningActive` set |
| `0` | Normal | neither |
| `null` | Not yet reported | `GET /live-session` only, before the first tick. Never appears in `status[]` or on SignalR |

Device status is device-level, not per-metric, and alarm outranks warning. `eventType: "status"`
events — `MotorRunning` — never affect it.

### `state`

| Value | Meaning |
|---|---|
| `triggered` | The condition became active on this tick. Draw a marker |
| `cleared` | The condition resolved on this tick (`value` is `0`). Draw a cleared marker |
| `active` | The condition was **already in effect when the session opened**. Record it — do not draw a transition marker, because the trigger happened before the session started |

`active` appears only on events timestamped at the session's first tick. A session that opens with
an alarm already firing reports `status[0] = 2` and one `active` event naming which alarm it is.

### `eventType`

| Value | Meaning |
|---|---|
| `alarm` | Critical condition |
| `warning` | Non-critical condition |
| `maintenance` | Maintenance interval event |
| `status` | Informational — `MotorRunning`. **Never affects `status[]`** |
| `unknown` | See below |

### `domain`

`operational` \| `maintenance` \| `unknown`.

### `severity`

`critical` \| `major` \| `minor` \| `unknown`.

### `unknown` metadata

`unknown` means the gateway reported an event for a parameter with no row in
`dbo.DeviceAlertDefinitions` — typically a new parameter whose definition has not been seeded yet.
The event is **still delivered** rather than dropped, so that the gap is visible. Render these
defensively: do not assume `eventType`, `domain` or `severity` name something you have a glyph for.

### Error codes

| Code | HTTP | Endpoints |
|---|---|---|
| `LIVE_SESSION_ALREADY_ACTIVE` | 409 | POST |
| `LIVE_SESSION_CAPACITY_REACHED` | 409 | POST |
| `LIVE_SESSION_NOT_ACTIVE` | 409 | extend, DELETE, series |
| `LIVE_SESSION_EXTENSION_LIMIT_REACHED` | 400 | extend |
| `LIVE_SESSION_INVALID_PARAMETERS` | 400 | POST, series |
| `DEVICE_NOT_FOUND` | 404 | all |
| `GATEWAY_NOT_PAIRED` | 404 | all |
| `GATEWAY_UNREACHABLE` | 503 | POST, extend, DELETE |

### Snapshot fields

`snapshot` and `changes` use `TelemetryMeasures` column names — the same names as regular telemetry.
55 fields, including `MotorRunning`, `OperatingPressurePSI`, `OperatingPressureBar`,
`StrokeRateLeft`, `StrokeRateRight`, `HydraulicOilTemperatureC`, `OperatingHoursMotor`,
`OperatingAlarmActive`, `OperatingWarningActive`, `MaintenanceAlarmActive`,
`MaintenanceWarningActive`.

Values are JSON primitives: boolean, number or string.

---

## Deployment prerequisites

Until these are done in dev, REST endpoints respond but no `liveTelemetryUpdate` messages are
published:

1. Event Grid subscription for `telemetry-type == 'fast-polling'` routed to the new function
2. Gateway module id set in backend configuration — currently a placeholder; `POST` may return
   `503 GATEWAY_UNREACHABLE` against an online device
3. `LivePolling__CleanupSchedule` set on the function app

---

## Related

- Design: `docs/superpowers/specs/2026-09-20-live-polling-api-design.md`
- Default series metrics are configured server-side (`LivePolling:DefaultSeriesMetrics`) and are not
  yet decided.
