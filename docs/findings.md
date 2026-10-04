# Live polling — validation findings

Contract gaps and design questions found by the validation harness, for the backend team.
Format: evidence (test or observation), impact on the frontend contract, proposal.
Design: `docs/superpowers/specs/2026-10-01-live-polling-validation-harness-design.md`.

## F1 — DeviceGuid is not exposed by the live-session API

- **Evidence:** `join-device` needs the DeviceGuid (`telemetry:device:{deviceGuid}`); no response of `POST`/`GET`/`extend`
  carries it — `deviceId` is the serial. `Shape.SmokeClient` and `web/` both have to be configured with it
  (`SmokeConfig.DeviceGuid`, `VITE_DEVICE_GUID`).
- **Impact:** the frontend cannot subscribe from the documented API alone; a wrong id is a silent subscription to a
  group that never receives messages.
- **Proposal:** add `deviceGuid` to the start/extend DTO and to `GET /live-session`.

## F2 — The first batch exceeds the 100-entity transaction limit with the real payload

- **Evidence:** `SnapshotFolderTests.Real_seed_first_batch_exceeds_the_transaction_limit_but_later_batches_fit`:
  the 73-field seed gives 73 baseline change points + 32 delta change points + 1 status row + 1 event + 1 head
  = **108** entities. A later 5-delta batch is 41. Measured live (simulator → function host): the first batch
  committed 102 change points + 1 event + head = **104** entities through the chunked path; later batches 33–35. The 09-30 design measured 87 with 55 fields (13 of headroom);
  the integration doc still says "all 55 fields".
- **Impact:** as designed, `TableLiveSessionStore` gets a `400` on every session's first batch; Event Grid redelivers
  it until it dead-letters, so no session would ever show data.
- **Proposal:** the chunked idempotent commit implemented here (rows upserted in chunks of ≤ 100, head last with
  `If-Match`; `TableLiveSessionStoreTests.Large_first_batch_takes_the_chunked_path_and_commits`,
  `Chunked_commit_retried_after_head_conflict_produces_no_duplicate_rows`), or reconcile the field count against
  `TelemetryMeasures`. Either way, update the integration doc's "55 fields".

## F3 — Inactive `GET /live-session` example contradicts its field table

- **Evidence:** the field table says "When `active` is `false`, every other field is `null`"; the example shows
  `{ "data": { "active": false } }` with the keys omitted. The harness returns explicit nulls
  (`LiveSessionServiceTests.Status_without_a_session_is_inactive_with_explicit_nulls`).
- **Impact:** a client written against the example (`'sessionId' in data`) and one written against the table
  (`data.sessionId === null`) behave differently.
- **Proposal:** fix the example to show the null keys — the same rule the SignalR section already states.

## F4 — `/series` grid can be one point shorter than `tickCount`

- **Evidence:** grid length is `floor((LastTickUtc − FirstTickUtc)/interval) + 1`. Gateway timestamps jitter, so five
  ticks spanning 3.998 s give four grid points
  (`SeriesStepExpanderTests.Grid_drops_the_newest_tick_when_jitter_shortens_the_span`); the smoke `basic` scenario
  allows `t.length ∈ {tickCount, tickCount − 1}` for this reason.
- **Impact:** the newest tick is missing from the seed until the next one arrives; harmless for the chart, but
  `t.length === tickCount` is not a safe assertion for the frontend.
- **Proposal:** `round` instead of `floor`, or document the relation.

## F5 — Group names are case-sensitive and the GUID format is implied, not specified

- **Evidence:** the publisher builds `telemetry:device:{deviceGuid}` from `Guid.ToString()` (lowercase, hyphenated);
  `join-device` uses `deviceId` verbatim (`SignalRNames.DeviceGroup(string)`).
- **Impact:** a frontend passing an upper-case or braced GUID subscribes successfully to a group nobody publishes to.
- **Proposal:** normalise `deviceId` in `join-device`/`leave-device` (`Guid.Parse(...).ToString()`), or specify the format.

## F6 — A delivery after purge resurrects an orphan partition

- **Evidence:** the processor never consults the session record. Event Grid retries a failed delivery for up to 24 h;
  a batch delivered after the cleanup timer purged its session recreates `!current` and rows that no later run will
  find (the timer only visits sessions with `PurgedUtc IS NULL`). The smoke `stop` scenario re-checks the partition
  5 s after purge for the common case (the gateway's partial flush).
- **Impact:** none on the frontend; unbounded orphan rows in Table Storage.
- **Proposal:** have the processor drop batches for sessions that are stopped/purged (one SQL read per batch, or a
  short cache), or let the timer sweep partitions whose session is purged.

## F7 — A reordered batch is lost, not merged

- **Evidence:** with `--reorder`, the later batch arrives first and advances `LastSnapshotTimestampUtc`; the earlier
  batch is then not-newer and skipped (by design, 09-20 §5.2). The smoke `faults` scenario prints
  `[grid N, tickCount M]` with N > M — measured `[grid 35, tickCount 20]`: with every adjacent pair swapped, the
  earlier batch of each pair is skipped, so 15 of 35 ticks exist in the grid only as carried-forward values.
- **Impact:** an alarm transition inside a reordered batch never reaches the store or SignalR; the status column
  shows it late, from the next snapshot, with no event.
- **Proposal:** accept and document, or let an older batch back-fill change points and events (not the head) the way
  an out-of-order baseline already does.

## F8 — `negotiate` without `x-user-id` returns 500, not 400

- **Evidence:** `POST /api/negotiate` with no `x-user-id` header → `500`; the host logs
  `Error while accessing 'x-user-id': property doesn't exist.` The `SignalRConnectionInfoInput` binding expression
  `UserId = "{headers.x-user-id}"` is resolved by the host before function code runs, so the function's own `400`
  check is unreachable. `join-device` / `leave-device` read the header in code and do return `400`.
- **Impact:** a client that forgets the header gets a server error instead of a client error; nothing else.
- **Proposal:** if the real negotiator uses the same binding expression, document the `500`, or negotiate through
  the SignalR Management SDK so the header can be validated first.

## The 09-30 design's §12 known gaps, re-checked

| Gap | Result | Evidence |
|---|---|---|
| Event row Table round-trip never executed | **Not reproduced** — the seam holds | `WriterReaderSeamTests.Event_rows_round_trip_with_absent_label_read_as_null` (null `Label` absent → `null`, `Value` Int64, timestamps). Note the real reader uses `GetDateTime`; this harness's reader uses `GetDateTimeOffset` |
| Transaction headroom of 13 entities | **Confirmed, and exceeded** | F2 |
| `label` null on `cleared` | **Confirmed** (mirrored deliberately) | `LiveEventResolverTests`; smoke `events` |
| SignalR sends `"events": null` | **Confirmed**; the integration doc now says so | `BroadcastEncoderTests.Every_entry_carries_all_five_keys_with_explicit_nulls` |
| Catalog keyed by `ParameterName` alone | **Not applicable** — 18 unique names in a static file | `config/alert-catalog.json` |
| Unknown parameters warned per occurrence | **Confirmed** | `LiveEventResolver` logs per event |
| Failed catalog load retries every batch | **Confirmed** | `AlertDefinitionCatalog` never caches a failure; `LiveTelemetryProcessorTests.Catalog_failure_still_folds_and_broadcasts_with_unknown_metadata` |
| Two entries sharing a `TimestampUtc` | **Not reproduced** — the second is skipped as not-newer; the store also de-duplicates RowKeys | `SnapshotFolderTests.Second_entry_with_same_timestamp_is_skipped` |
| Second baseline mid-session | **Confirmed** — `active` rows land at a non-first-tick timestamp | `SnapshotFolderTests.Second_baseline_mid_session_rebaselines_and_records_active_events` |

## Open verification items (09-20 §16)

| Item | Status here |
|---|---|
| 5. How Event Grid presents a gzip body | Assumed (`data.body` base64, `data.properties`, `data.systemProperties`); isolated in `LiveEventGridDecoder`; still needs one real message |
| 1, 2. Event Grid filter semantics | Not testable locally |
| 3, 7, 8. Real-gateway measurements | Not testable locally |
