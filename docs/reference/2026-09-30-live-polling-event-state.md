# Live Polling Event State Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Carry the gateway's `Event` block through the live polling pipeline — resolved to a human label — so the live chart can draw alarm, warning and motor markers and a reload can replay them.

**Architecture:** The ingestion function parses `Event` beside `Body`/`Changes`, resolves each parameter against a load-once catalog built from `dbo.DeviceAlertDefinitions` + `dbo.DeviceAlertValueDefinitions`, writes one Table Storage row per occurrence inside the existing entity-group transaction, and includes the resolved events in the gzip SignalR broadcast. The backend range-scans those rows and returns them from the series endpoint. Resolution happens once, in the function, so live and reload can never disagree.

**Tech Stack:** .NET 8 isolated worker, Azure Table Storage (`Azure.Data.Tables`), Azure SignalR, `Microsoft.Data.SqlClient`, xunit (plain `Assert` in the function tests, FluentAssertions in the backend tests), ASP.NET Core 8.

**Spec:** `docs/superpowers/specs/2026-09-30-live-polling-event-state-design.md`

## Global Constraints

- Branch is `feature/live-mode` in both repos — already exists, already carries the live polling work. **Confirm the branch before the first commit in each repo.** Never `git push`.
- Commit messages: single concise title line, no body, no `Co-Authored-By`, no attribution lines.
- `LiveSessionKeys` exists in both repos and must stay **byte-identical**. Both copies carry the same tests.
- The live path must never write to `TelemetryRaw`, `TelemetryMeasures`, `TelemetryEvents` or `DeviceLatestTelemetry`.
- Function tests use plain xunit `Assert` and hand-rolled fakes — no Moq, no FluentAssertions.
- XML doc conventions from `CLAUDE.md` apply: `<summary>` before attributes on classes; `<param name="ct">` is always `"Cancellation token for cooperative request cancellation."`
- Event state strings are exactly `triggered`, `cleared`, `active`. Unknown metadata is exactly `unknown`.
- `dbo` is read-only for verification. No INSERT/DELETE/UPDATE against the dev database.

## Review Focus

Five conditions the spec implies that no task's happy path covers. Each has its test added to the owning task.

1. **A metric literally named `evt`** would write `evt|{ts}` and land inside the event range, corrupting the reload. `LiveSessionKeys` must reject it rather than leave the collision latent. — Task 1
2. **`Event` value arriving as a JSON string (`"5"`) or a double (`5.0`)** rather than an integer. `ToClrValue` yields `long` for integers but `double` for anything with a decimal point, and a gateway firmware change could send either. Resolution must coerce, not crash or silently drop. — Task 3
3. **Two events on the same tick for the same parameter** cannot happen from one entry, but two *different* parameters on one tick must not collide in the RowKey. — Task 1
4. **The catalog's SQL failing** must degrade to `unknown` metadata with the events still stored and broadcast. Telemetry must never be blocked on an alert-definition read. — Task 2
5. **A baseline carrying `OperatingAlarmActive: 0`** must write nothing. Without the zero-suppression rule every session opens with a dozen meaningless `cleared` rows. — Task 6

---

## File Structure

### `shape-iot-function-data-ingestion`

| File | Responsibility |
|---|---|
| `LivePolling/LiveSessionKeys.cs` | **modify** — event row keys, `evt` reserved |
| `LivePolling/IAlertDefinitionCatalog.cs` | **create** — `AlertDefinition` record + interface |
| `LivePolling/AlertDefinitionCatalog.cs` | **create** — load-once SQL catalog |
| `LivePolling/LiveEvent.cs` | **create** — resolved event record + state constants |
| `LivePolling/LiveEventResolver.cs` | **create** — `Event` block → `LiveEvent` list |
| `LivePolling/LiveTelemetryBatch.cs` | **modify** — parse `Event` into `LiveEntry.Events` |
| `LivePolling/ILiveSessionStore.cs` | **modify** — `WriteBatchAsync` takes events |
| `LivePolling/TableLiveSessionStore.cs` | **modify** — write event rows in the same transaction |
| `LivePolling/LiveTelemetryProcessor.cs` | **modify** — resolve, zero-suppress, `BroadcastEntry.Events` |
| `LivePolling/DeviceStatusDeriver.cs` | **modify** — XML remark only |
| `Functions/LiveTelemetryProcessorEventGrid.cs` | **modify** — events in the broadcast |
| `Program.cs` | **modify** — register the catalog |

### `shape-iot-backend`

| File | Responsibility |
|---|---|
| `Brilliant.IoT.Infrastructure/LivePolling/LiveSessionKeys.cs` | **modify** — verbatim mirror of the function copy |
| `Brilliant.IoT.Application/LivePolling/ILiveSessionReader.cs` | **modify** — `LiveEventPoint`, `GetEventsAsync` |
| `Brilliant.IoT.Infrastructure/LivePolling/TableLiveSessionReader.cs` | **modify** — range-scan `evt\|` |
| `Brilliant.IoT.Application/DTOs/LivePolling/LivePollingDtos.cs` | **modify** — `LivePollingEventDto`, `Events`, `Status` → int |
| `Brilliant.IoT.API/Controllers/LivePollingController.cs` | **modify** — return events on both series variants |

### Docs

| File | Responsibility |
|---|---|
| `docs/live-polling-frontend-integration.md` | **modify** — events contract, `status[]` type, enumerations |

---

## Task 1: Event row keys in both repos

`LiveSessionKeys` is duplicated verbatim across the two repos. Both copies and both test files change in this one task so they cannot drift.

**Files:**
- Modify: `shape-iot-function-data-ingestion/LivePolling/LiveSessionKeys.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.Infrastructure/LivePolling/LiveSessionKeys.cs`
- Test: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/LiveSessionKeysTests.cs`
- Test: `shape-iot-backend/Brilliant.IoT.UnitTests/LivePolling/LiveSessionKeysTests.cs`

**Interfaces:**
- Consumes: nothing.
- Produces: `LiveSessionKeys.EventPrefix` (`"evt"`), `LiveSessionKeys.EventRowKey(string parameter, DateTime timestampUtc) -> string`, `LiveSessionKeys.EventRange(DateTime? since) -> (string From, string To)`. `ParamRowKey` and `ParamRange` now throw `ArgumentException` when handed `"evt"`.

- [ ] **Step 1: Write the failing tests (function repo)**

Append to `shape-iot-function-data-ingestion.Tests/LivePolling/LiveSessionKeysTests.cs`, inside the existing class:

```csharp
[Fact]
public void EventRowKey_SortsChronologicallyAndCarriesTheParameter()
{
    var early = LiveSessionKeys.EventRowKey(
        "OperatingAlarmActive", new DateTime(2026, 9, 28, 10, 27, 1, DateTimeKind.Utc));
    var late = LiveSessionKeys.EventRowKey(
        "OperatingAlarmActive", new DateTime(2026, 9, 28, 10, 27, 2, DateTimeKind.Utc));

    Assert.StartsWith("evt|", early);
    Assert.EndsWith("|OperatingAlarmActive", early);
    Assert.True(string.CompareOrdinal(early, late) < 0);
}

[Fact]
public void EventRowKey_TwoParametersOnOneTickDoNotCollide()
{
    var ts = new DateTime(2026, 9, 28, 10, 27, 3, DateTimeKind.Utc);

    var alarm = LiveSessionKeys.EventRowKey("OperatingAlarmActive", ts);
    var warning = LiveSessionKeys.EventRowKey("OperatingWarningActive", ts);

    Assert.NotEqual(alarm, warning);
}

[Fact]
public void EventRange_ExcludesTheHeadRowAndParameterRows()
{
    var (from, to) = LiveSessionKeys.EventRange(null);

    var head = LiveSessionKeys.CurrentRowKey;
    var param = LiveSessionKeys.ParamRowKey(
        "OperatingPressure", new DateTime(2026, 9, 28, 10, 27, 1, DateTimeKind.Utc));
    var status = LiveSessionKeys.ParamRowKey(
        LiveSessionKeys.StatusMetric, new DateTime(2026, 9, 28, 10, 27, 1, DateTimeKind.Utc));
    var evt = LiveSessionKeys.EventRowKey(
        "OperatingAlarmActive", new DateTime(2026, 9, 28, 10, 27, 1, DateTimeKind.Utc));

    Assert.False(string.CompareOrdinal(head, from) >= 0 && string.CompareOrdinal(head, to) < 0);
    Assert.False(string.CompareOrdinal(param, from) >= 0 && string.CompareOrdinal(param, to) < 0);
    Assert.False(string.CompareOrdinal(status, from) >= 0 && string.CompareOrdinal(status, to) < 0);
    Assert.True(string.CompareOrdinal(evt, from) >= 0 && string.CompareOrdinal(evt, to) < 0);
}

[Fact]
public void EventRange_WithSince_SkipsEarlierEvents()
{
    var since = new DateTime(2026, 9, 28, 10, 27, 3, DateTimeKind.Utc);
    var (from, to) = LiveSessionKeys.EventRange(since);

    var before = LiveSessionKeys.EventRowKey(
        "OperatingAlarmActive", new DateTime(2026, 9, 28, 10, 27, 1, DateTimeKind.Utc));
    var after = LiveSessionKeys.EventRowKey(
        "OperatingAlarmActive", new DateTime(2026, 9, 28, 10, 27, 5, DateTimeKind.Utc));

    Assert.True(string.CompareOrdinal(before, from) < 0);
    Assert.True(string.CompareOrdinal(after, from) >= 0 && string.CompareOrdinal(after, to) < 0);
}

[Fact]
public void ParamRowKey_RejectsTheReservedEventPrefix()
{
    var ts = new DateTime(2026, 9, 28, 10, 27, 1, DateTimeKind.Utc);

    Assert.Throws<ArgumentException>(() => LiveSessionKeys.ParamRowKey("evt", ts));
    Assert.Throws<ArgumentException>(() => LiveSessionKeys.ParamRowKey("EVT", ts));
    Assert.Throws<ArgumentException>(() => LiveSessionKeys.ParamRange("evt", null));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveSessionKeysTests"`
Expected: FAIL — `'LiveSessionKeys' does not contain a definition for 'EventRowKey'`

- [ ] **Step 3: Implement in the function repo**

In `shape-iot-function-data-ingestion/LivePolling/LiveSessionKeys.cs`, replace `ParamRowKey` and `ParamRange` and add the event members:

```csharp
        /// <summary>RowKey prefix for event rows. Reserved — no metric may use this name.</summary>
        public const string EventPrefix = "evt";

        private const char Delimiter = '|';

        /// <summary>Row key for one change point. Round-trip timestamps sort chronologically.</summary>
        public static string ParamRowKey(string metric, DateTime timestampUtc)
        {
            GuardMetricName(metric);
            return $"{metric}{Delimiter}{timestampUtc.ToUniversalTime():O}";
        }

        /// <summary>
        /// Inclusive-from, exclusive-to range covering exactly one metric's rows.
        /// </summary>
        /// <remarks>
        /// The upper bound uses the character after the delimiter, so a metric whose name is a
        /// prefix of another — StrokeRate against StrokeRateLeft — cannot leak into the range.
        /// </remarks>
        public static (string From, string To) ParamRange(string metric, DateTime? since)
        {
            GuardMetricName(metric);

            var from = since is { } s
                ? $"{metric}{Delimiter}{s.ToUniversalTime():O}"
                : $"{metric}{Delimiter}";
            var to = $"{metric}{(char)(Delimiter + 1)}";
            return (from, to);
        }

        /// <summary>
        /// Row key for one event occurrence.
        /// </summary>
        /// <remarks>
        /// The timestamp leads so the whole set range-scans chronologically under one prefix, which
        /// is how the reload endpoint reads it. The parameter tail keeps two parameters that
        /// transitioned on the same tick from overwriting one another.
        /// </remarks>
        public static string EventRowKey(string parameter, DateTime timestampUtc) =>
            $"{EventPrefix}{Delimiter}{timestampUtc.ToUniversalTime():O}{Delimiter}{parameter}";

        /// <summary>Inclusive-from, exclusive-to range covering every event row in the session.</summary>
        public static (string From, string To) EventRange(DateTime? since)
        {
            var from = since is { } s
                ? $"{EventPrefix}{Delimiter}{s.ToUniversalTime():O}"
                : $"{EventPrefix}{Delimiter}";
            var to = $"{EventPrefix}{(char)(Delimiter + 1)}";
            return (from, to);
        }

        /// <summary>
        /// Rejects the one metric name that would collide with the event range.
        /// </summary>
        /// <remarks>
        /// A metric named "evt" writes <c>evt|{timestamp}</c>, which falls inside
        /// <see cref="EventRange"/> and would be read back as a malformed event. No such row exists
        /// in dbo.ParameterRegistry; this keeps the collision from ever becoming latent.
        /// </remarks>
        private static void GuardMetricName(string metric)
        {
            if (string.Equals(metric, EventPrefix, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException(
                    $"'{EventPrefix}' is reserved for event rows and cannot be used as a metric name.",
                    nameof(metric));
        }
```

- [ ] **Step 4: Run to verify it passes**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveSessionKeysTests"`
Expected: PASS

- [ ] **Step 5: Copy the file verbatim to the backend**

The backend copy differs only in its namespace declaration (`Brilliant.IoT.Infrastructure.LivePolling`, file-scoped) and its class-level `<remarks>` pointing back at the function copy. Copy the member bodies exactly — same text, same order, same comments.

Then append the same five tests to `shape-iot-backend/Brilliant.IoT.UnitTests/LivePolling/LiveSessionKeysTests.cs`, translating assertions to the FluentAssertions style already used in that file:

```csharp
[Fact]
public void EventRowKey_TwoParametersOnOneTickDoNotCollide()
{
    var ts = new DateTime(2026, 9, 28, 10, 27, 3, DateTimeKind.Utc);

    LiveSessionKeys.EventRowKey("OperatingAlarmActive", ts)
        .Should().NotBe(LiveSessionKeys.EventRowKey("OperatingWarningActive", ts));
}

[Fact]
public void ParamRowKey_RejectsTheReservedEventPrefix()
{
    var ts = new DateTime(2026, 9, 28, 10, 27, 1, DateTimeKind.Utc);

    FluentActions.Invoking(() => LiveSessionKeys.ParamRowKey("evt", ts))
        .Should().Throw<ArgumentException>();
}
```

Port the remaining three (`EventRowKey_SortsChronologicallyAndCarriesTheParameter`, `EventRange_ExcludesTheHeadRowAndParameterRows`, `EventRange_WithSince_SkipsEarlierEvents`) the same way — same names, same assertions, FluentAssertions syntax.

- [ ] **Step 6: Run the backend tests**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LiveSessionKeysTests"`
Expected: PASS

- [ ] **Step 7: Commit both repos**

```bash
cd shape-iot-function-data-ingestion
git add LivePolling/LiveSessionKeys.cs shape-iot-function-data-ingestion.Tests/LivePolling/LiveSessionKeysTests.cs
git commit -m "feat: add event row keys to live session key scheme"

cd ../shape-iot-backend
git add Brilliant.IoT.Infrastructure/LivePolling/LiveSessionKeys.cs Brilliant.IoT.UnitTests/LivePolling/LiveSessionKeysTests.cs
git commit -m "feat: mirror event row keys in backend live session keys"
```

---

## Task 2: Alert definition catalog

**Files:**
- Create: `shape-iot-function-data-ingestion/LivePolling/IAlertDefinitionCatalog.cs`
- Create: `shape-iot-function-data-ingestion/LivePolling/AlertDefinitionCatalog.cs`
- Test: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/AlertDefinitionCatalogTests.cs`

**Interfaces:**
- Consumes: `ShapeIot.Ingestion.Infrastructure.ISqlConnectionFactory` — `SqlConnection CreateConnection()`.
- Produces:
  - `AlertDefinition(string ParameterName, int AlertCode, string EventType, string Domain, string Severity, string Description, IReadOnlyDictionary<int, string>? ValueLabels)`
  - `IAlertDefinitionCatalog.GetAsync(CancellationToken ct = default) -> Task<IReadOnlyDictionary<string, AlertDefinition>>` — keyed by `ParameterName`, case-insensitive.

**Why not `AlertCacheService`:** the existing service is keyed by `int` AlertCode, and every method demands a caller-supplied open `SqlConnection` that the live path does not hold. Adapting it would change an interface the regular telemetry pipeline depends on. The SQL is reused; the class is not.

- [ ] **Step 1: Write the interface and record**

Create `LivePolling/IAlertDefinitionCatalog.cs`:

```csharp
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace ShapeIot.Ingestion.LivePolling
{
    /// <summary>One row of dbo.DeviceAlertDefinitions, with its value labels attached.</summary>
    /// <param name="ParameterName">Telemetry parameter this definition describes.</param>
    /// <param name="AlertCode">Modbus register position, e.g. 40006.</param>
    /// <param name="EventType">alarm, warning, maintenance or status.</param>
    /// <param name="Domain">operational or maintenance.</param>
    /// <param name="Severity">critical, major or minor.</param>
    /// <param name="Description">Fallback text when no value label applies.</param>
    /// <param name="ValueLabels">
    /// ValueCode to label. Null for the 16 boolean definitions — only 40006 and 40007 have rows in
    /// dbo.DeviceAlertValueDefinitions.
    /// </param>
    public sealed record AlertDefinition(
        string ParameterName,
        int AlertCode,
        string EventType,
        string Domain,
        string Severity,
        string Description,
        IReadOnlyDictionary<int, string>? ValueLabels);

    /// <summary>
    /// Alert definitions keyed by parameter name, for resolving a live session's event block.
    /// </summary>
    public interface IAlertDefinitionCatalog
    {
        /// <summary>Every definition that has a parameter name, compared case-insensitively.</summary>
        /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
        /// <returns>The definitions, or an empty map when the load failed.</returns>
        Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default);
    }
}
```

- [ ] **Step 2: Write the failing tests**

Create `shape-iot-function-data-ingestion.Tests/LivePolling/AlertDefinitionCatalogTests.cs`. The catalog is exercised through a seam rather than a real database — `AlertDefinitionCatalog` exposes a protected virtual loader the test overrides:

```csharp
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests
{
    public class AlertDefinitionCatalogTests
    {
        private sealed class StubCatalog : AlertDefinitionCatalog
        {
            private readonly Func<IReadOnlyDictionary<string, AlertDefinition>> _load;
            public int LoadCount;

            public StubCatalog(Func<IReadOnlyDictionary<string, AlertDefinition>> load)
                : base(null!, NullLogger<AlertDefinitionCatalog>.Instance) => _load = load;

            protected override Task<IReadOnlyDictionary<string, AlertDefinition>> LoadAsync(CancellationToken ct)
            {
                LoadCount++;
                return Task.FromResult(_load());
            }
        }

        private static IReadOnlyDictionary<string, AlertDefinition> OneDefinition() =>
            new Dictionary<string, AlertDefinition>(StringComparer.OrdinalIgnoreCase)
            {
                ["OperatingAlarmActive"] = new(
                    "OperatingAlarmActive", 40006, "alarm", "operational", "critical",
                    "Indicates an active operational fault condition.",
                    new Dictionary<int, string> { [0] = "No Fault Active", [5] = "Emergency Stop Active" })
            };

        [Fact]
        public async Task GetAsync_LoadsOnceAndReusesTheResult()
        {
            var catalog = new StubCatalog(OneDefinition);

            var first = await catalog.GetAsync();
            var second = await catalog.GetAsync();

            Assert.Same(first, second);
            Assert.Equal(1, catalog.LoadCount);
        }

        [Fact]
        public async Task GetAsync_IsCaseInsensitiveOnParameterName()
        {
            var catalog = new StubCatalog(OneDefinition);

            var map = await catalog.GetAsync();

            Assert.True(map.ContainsKey("operatingalarmactive"));
            Assert.Equal("Emergency Stop Active", map["OperatingAlarmActive"].ValueLabels![5]);
        }

        [Fact]
        public async Task GetAsync_DoesNotCacheAFailedLoad()
        {
            var fail = true;
            var catalog = new StubCatalog(() =>
                fail ? throw new InvalidOperationException("sql down") : OneDefinition());

            var first = await catalog.GetAsync();
            Assert.Empty(first);

            fail = false;
            var second = await catalog.GetAsync();

            Assert.Single(second);
            Assert.Equal(2, catalog.LoadCount);
        }
    }
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~AlertDefinitionCatalogTests"`
Expected: FAIL — `The type or namespace name 'AlertDefinitionCatalog' could not be found`

- [ ] **Step 4: Implement the catalog**

Create `LivePolling/AlertDefinitionCatalog.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ShapeIot.Ingestion.Infrastructure;

namespace ShapeIot.Ingestion.LivePolling
{
    /// <summary>
    /// Alert definitions, read once from SQL and held for the life of the process.
    /// </summary>
    /// <remarks>
    /// 18 definitions and 57 value labels. There is no expiry because these rows change with a
    /// firmware or model release, which ships with a deployment, and a deployment restarts this
    /// process.
    /// <para>
    /// A failed load is deliberately <em>not</em> cached, and never throws to the caller: live
    /// telemetry must not stop flowing because an alert-definition read failed. The batch resolves
    /// with unknown metadata and the next batch retries the load.
    /// </para>
    /// </remarks>
    public class AlertDefinitionCatalog : IAlertDefinitionCatalog
    {
        private static readonly IReadOnlyDictionary<string, AlertDefinition> Empty =
            new Dictionary<string, AlertDefinition>(StringComparer.OrdinalIgnoreCase);

        private readonly ISqlConnectionFactory _connectionFactory;
        private readonly ILogger<AlertDefinitionCatalog> _logger;
        private readonly SemaphoreSlim _gate = new(1, 1);

        private IReadOnlyDictionary<string, AlertDefinition>? _definitions;

        public AlertDefinitionCatalog(
            ISqlConnectionFactory connectionFactory,
            ILogger<AlertDefinitionCatalog> logger)
        {
            _connectionFactory = connectionFactory;
            _logger = logger;
        }

        /// <inheritdoc />
        public async Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default)
        {
            var loaded = _definitions;
            if (loaded is not null) return loaded;

            await _gate.WaitAsync(ct);
            try
            {
                // Another caller may have loaded it while this one waited on the gate.
                if (_definitions is not null) return _definitions;

                try
                {
                    _definitions = await LoadAsync(ct);
                }
                catch (Exception ex)
                {
                    _logger.LogError(
                        ex, "Could not load alert definitions; this batch's events resolve as unknown.");
                    return Empty;
                }

                _logger.LogInformation(
                    "Loaded {Count} alert definitions for live polling event resolution.", _definitions.Count);

                return _definitions;
            }
            finally
            {
                _gate.Release();
            }
        }

        /// <summary>Reads both definition tables in one query.</summary>
        /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
        /// <returns>Definitions keyed by parameter name.</returns>
        protected virtual async Task<IReadOnlyDictionary<string, AlertDefinition>> LoadAsync(CancellationToken ct)
        {
            const string query = @"
SELECT d.AlertDefinitionId, d.AlertCode, d.ParameterName, d.EventType, d.Domain,
       d.Severity, d.Description, v.ValueCode, v.ValueLabel
FROM dbo.DeviceAlertDefinitions d
LEFT JOIN dbo.DeviceAlertValueDefinitions v ON v.AlertDefinitionId = d.AlertDefinitionId
WHERE d.ParameterName IS NOT NULL;";

            var rows = new Dictionary<string, (AlertDefinition Def, Dictionary<int, string> Labels)>(
                StringComparer.OrdinalIgnoreCase);

            using var conn = _connectionFactory.CreateConnection();
            await conn.OpenAsync(ct);

            using var cmd = new SqlCommand(query, conn);
            using var reader = await cmd.ExecuteReaderAsync(ct);

            while (await reader.ReadAsync(ct))
            {
                var parameter = reader.GetString(2);

                if (!rows.TryGetValue(parameter, out var row))
                {
                    var def = new AlertDefinition(
                        parameter,
                        reader.GetInt32(1),
                        reader.IsDBNull(3) ? LiveEventResolver.Unknown : reader.GetString(3),
                        reader.IsDBNull(4) ? LiveEventResolver.Unknown : reader.GetString(4),
                        reader.IsDBNull(5) ? LiveEventResolver.Unknown : reader.GetString(5),
                        reader.IsDBNull(6) ? string.Empty : reader.GetString(6),
                        null);

                    row = (def, new Dictionary<int, string>());
                    rows[parameter] = row;
                }

                if (!reader.IsDBNull(7) && !reader.IsDBNull(8))
                    row.Labels[reader.GetInt32(7)] = reader.GetString(8);
            }

            var result = new Dictionary<string, AlertDefinition>(StringComparer.OrdinalIgnoreCase);
            foreach (var (parameter, row) in rows)
                result[parameter] = row.Labels.Count > 0
                    ? row.Def with { ValueLabels = row.Labels }
                    : row.Def;

            return result;
        }
    }
}
```

- [ ] **Step 5: Run to verify it passes**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~AlertDefinitionCatalogTests"`
Expected: PASS. `LiveEventResolver.Unknown` does not exist yet — add a temporary `private const string Unknown = "unknown";` in this file, and replace the three references with `LiveEventResolver.Unknown` at the end of Task 3.

- [ ] **Step 6: Commit**

```bash
git add LivePolling/IAlertDefinitionCatalog.cs LivePolling/AlertDefinitionCatalog.cs shape-iot-function-data-ingestion.Tests/LivePolling/AlertDefinitionCatalogTests.cs
git commit -m "feat: add load-once alert definition catalog for live polling"
```

---

## Task 3: Event resolution

**Files:**
- Create: `shape-iot-function-data-ingestion/LivePolling/LiveEvent.cs`
- Create: `shape-iot-function-data-ingestion/LivePolling/LiveEventResolver.cs`
- Modify: `shape-iot-function-data-ingestion/LivePolling/AlertDefinitionCatalog.cs` (drop the temporary `Unknown`)
- Test: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/LiveEventResolverTests.cs`

**Interfaces:**
- Consumes: `AlertDefinition` from Task 2.
- Produces:
  - `LiveEvent(DateTime TimestampUtc, string Parameter, long Value, string State, string EventType, string Domain, string Severity, string? Label)` — one record used for both storage and broadcast.
  - `LiveEventStates.Triggered` / `.Cleared` / `.Active`
  - `LiveEventResolver.Unknown` (`"unknown"`)
  - `LiveEventResolver.Resolve(DateTime timestampUtc, IDictionary<string, object?> events, bool isBaseline, IReadOnlyDictionary<string, AlertDefinition> catalog) -> IReadOnlyList<LiveEvent>`

- [ ] **Step 1: Write the failing tests**

Create `shape-iot-function-data-ingestion.Tests/LivePolling/LiveEventResolverTests.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Linq;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests
{
    public class LiveEventResolverTests
    {
        private static readonly DateTime Ts = new(2026, 9, 28, 10, 27, 1, DateTimeKind.Utc);

        private static IReadOnlyDictionary<string, AlertDefinition> Catalog() =>
            new Dictionary<string, AlertDefinition>(StringComparer.OrdinalIgnoreCase)
            {
                ["OperatingAlarmActive"] = new(
                    "OperatingAlarmActive", 40006, "alarm", "operational", "critical",
                    "Indicates an active operational fault condition.",
                    new Dictionary<int, string> { [0] = "No Fault Active", [5] = "Emergency Stop Active" }),
                ["MaintenanceAlarmActive"] = new(
                    "MaintenanceAlarmActive", 40008, "alarm", "maintenance", "critical",
                    "Wear parts lifetime reached.", null),
                ["MotorRunning"] = new(
                    "MotorRunning", 40001, "status", "operational", "minor",
                    "Indicates if the Motor is Running.", null)
            };

        private static IDictionary<string, object?> Block(params (string Key, object? Value)[] pairs)
        {
            var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var (k, v) in pairs) d[k] = v;
            return d;
        }

        [Fact]
        public void Resolve_NonZeroDelta_IsTriggeredWithALabel()
        {
            var events = LiveEventResolver.Resolve(
                Ts, Block(("OperatingAlarmActive", 5L)), isBaseline: false, Catalog());

            var e = Assert.Single(events);
            Assert.Equal("OperatingAlarmActive", e.Parameter);
            Assert.Equal(5, e.Value);
            Assert.Equal(LiveEventStates.Triggered, e.State);
            Assert.Equal("alarm", e.EventType);
            Assert.Equal("operational", e.Domain);
            Assert.Equal("critical", e.Severity);
            Assert.Equal("Emergency Stop Active", e.Label);
        }

        [Fact]
        public void Resolve_ZeroDelta_IsClearedWithNoLabel()
        {
            var events = LiveEventResolver.Resolve(
                Ts, Block(("OperatingAlarmActive", 0L)), isBaseline: false, Catalog());

            var e = Assert.Single(events);
            Assert.Equal(LiveEventStates.Cleared, e.State);
            Assert.Null(e.Label);
        }

        [Fact]
        public void Resolve_BooleanParameter_CarriesMetadataButNoLabel()
        {
            var events = LiveEventResolver.Resolve(
                Ts, Block(("MaintenanceAlarmActive", 1L)), isBaseline: false, Catalog());

            var e = Assert.Single(events);
            Assert.Equal(LiveEventStates.Triggered, e.State);
            Assert.Equal("alarm", e.EventType);
            Assert.Equal("maintenance", e.Domain);
            Assert.Equal("critical", e.Severity);
            Assert.Null(e.Label);
        }

        [Fact]
        public void Resolve_Baseline_EmitsActiveAndSuppressesZeros()
        {
            var events = LiveEventResolver.Resolve(
                Ts,
                Block(("OperatingAlarmActive", 5L), ("MaintenanceAlarmActive", 0L), ("MotorRunning", 1L)),
                isBaseline: true,
                Catalog());

            Assert.Equal(2, events.Count);
            Assert.All(events, e => Assert.Equal(LiveEventStates.Active, e.State));
            Assert.Contains(events, e => e.Parameter == "OperatingAlarmActive");
            Assert.Contains(events, e => e.Parameter == "MotorRunning");
            Assert.DoesNotContain(events, e => e.Parameter == "MaintenanceAlarmActive");
        }

        [Fact]
        public void Resolve_UnknownParameter_IsEmittedWithUnknownMetadata()
        {
            var events = LiveEventResolver.Resolve(
                Ts, Block(("SomeNewAlarm", 3L)), isBaseline: false, Catalog());

            var e = Assert.Single(events);
            Assert.Equal("SomeNewAlarm", e.Parameter);
            Assert.Equal(LiveEventResolver.Unknown, e.EventType);
            Assert.Equal(LiveEventResolver.Unknown, e.Domain);
            Assert.Equal(LiveEventResolver.Unknown, e.Severity);
            Assert.Null(e.Label);
        }

        [Fact]
        public void Resolve_EmptyCatalog_StillEmitsTheEvent()
        {
            var events = LiveEventResolver.Resolve(
                Ts, Block(("OperatingAlarmActive", 5L)), isBaseline: false,
                new Dictionary<string, AlertDefinition>(StringComparer.OrdinalIgnoreCase));

            var e = Assert.Single(events);
            Assert.Equal(5, e.Value);
            Assert.Equal(LiveEventResolver.Unknown, e.EventType);
        }

        [Theory]
        [InlineData(5L)]
        [InlineData(5.0)]
        [InlineData("5")]
        [InlineData(true)]
        public void Resolve_CoercesTheValueTypesTheWireCanProduce(object raw)
        {
            var expected = raw is bool ? 1L : 5L;

            var events = LiveEventResolver.Resolve(
                Ts, Block(("OperatingAlarmActive", raw)), isBaseline: false, Catalog());

            var e = Assert.Single(events);
            Assert.Equal(expected, e.Value);
            Assert.Equal(LiveEventStates.Triggered, e.State);
        }

        [Fact]
        public void Resolve_UncoercibleValue_IsSkipped()
        {
            var events = LiveEventResolver.Resolve(
                Ts, Block(("OperatingAlarmActive", "not-a-number"), ("MotorRunning", 1L)),
                isBaseline: false, Catalog());

            Assert.Equal("MotorRunning", Assert.Single(events).Parameter);
        }

        [Fact]
        public void Resolve_EmptyBlock_ReturnsNothing()
        {
            Assert.Empty(LiveEventResolver.Resolve(Ts, Block(), isBaseline: false, Catalog()));
        }

        [Fact]
        public void Resolve_OrdersByParameterSoRowKeysAreDeterministic()
        {
            var events = LiveEventResolver.Resolve(
                Ts, Block(("MotorRunning", 1L), ("OperatingAlarmActive", 5L)),
                isBaseline: false, Catalog());

            Assert.Equal(new[] { "MotorRunning", "OperatingAlarmActive" },
                         events.Select(e => e.Parameter).ToArray());
        }
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveEventResolverTests"`
Expected: FAIL — `The type or namespace name 'LiveEvent' could not be found`

- [ ] **Step 3: Write the record**

Create `LivePolling/LiveEvent.cs`:

```csharp
using System;

namespace ShapeIot.Ingestion.LivePolling
{
    /// <summary>The three states an event row can hold.</summary>
    public static class LiveEventStates
    {
        /// <summary>The condition became active on this tick.</summary>
        public const string Triggered = "triggered";

        /// <summary>The condition resolved on this tick.</summary>
        public const string Cleared = "cleared";

        /// <summary>
        /// The condition was already active when the session opened.
        /// </summary>
        /// <remarks>
        /// Baseline events carry state that became true before the session started, so drawing a
        /// marker for them would place a transition at a time it did not happen. They are still
        /// recorded, because otherwise a session that opens in alarm has no record of which alarm.
        /// </remarks>
        public const string Active = "active";
    }

    /// <summary>One resolved event occurrence — what gets stored and what gets broadcast.</summary>
    /// <param name="TimestampUtc">Tick the transition was reported on.</param>
    /// <param name="Parameter">Telemetry parameter name, e.g. OperatingAlarmActive.</param>
    /// <param name="Value">Raw register value, e.g. 5.</param>
    /// <param name="State">One of <see cref="LiveEventStates"/>.</param>
    /// <param name="EventType">alarm, warning, maintenance, status, or unknown.</param>
    /// <param name="Domain">operational, maintenance, or unknown.</param>
    /// <param name="Severity">critical, major, minor, or unknown.</param>
    /// <param name="Label">Human text for the value; null for the boolean parameters.</param>
    public sealed record LiveEvent(
        DateTime TimestampUtc,
        string Parameter,
        long Value,
        string State,
        string EventType,
        string Domain,
        string Severity,
        string? Label);
}
```

- [ ] **Step 4: Write the resolver**

Create `LivePolling/LiveEventResolver.cs`:

```csharp
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;

namespace ShapeIot.Ingestion.LivePolling
{
    /// <summary>
    /// Turns one entry's Event block into resolved occurrences.
    /// </summary>
    /// <remarks>
    /// The gateway performs the edge detection: a parameter appears in a delta's Event block only on
    /// the tick its state moved. Nothing here diffs, and nothing here keeps a list of which
    /// parameters are alerts — presence in the block is what makes a key an event.
    /// </remarks>
    public static class LiveEventResolver
    {
        /// <summary>Stand-in metadata for a parameter with no definition row.</summary>
        public const string Unknown = "unknown";

        /// <summary>
        /// Resolves every parameter in one entry's Event block.
        /// </summary>
        /// <remarks>
        /// A baseline's block is the opening state rather than a set of transitions, so its non-zero
        /// entries resolve as <see cref="LiveEventStates.Active"/> and its zeros are dropped — a
        /// baseline carries all 18 parameters, and without that rule every session would open with a
        /// dozen meaningless cleared rows.
        /// </remarks>
        /// <param name="timestampUtc">The entry's tick.</param>
        /// <param name="events">The raw Event block, parameter name to register value.</param>
        /// <param name="isBaseline">Whether the entry arrived under Body rather than Changes.</param>
        /// <param name="catalog">Alert definitions keyed by parameter name; may be empty.</param>
        /// <returns>Resolved occurrences ordered by parameter name.</returns>
        public static IReadOnlyList<LiveEvent> Resolve(
            DateTime timestampUtc,
            IDictionary<string, object?> events,
            bool isBaseline,
            IReadOnlyDictionary<string, AlertDefinition> catalog)
        {
            if (events.Count == 0) return Array.Empty<LiveEvent>();

            var resolved = new List<LiveEvent>(events.Count);

            foreach (var pair in events)
            {
                // A value that will not coerce is a contract break, not a state: recording it as
                // "cleared" would be a lie, so it is skipped and the rest of the block still lands.
                if (!TryCoerce(pair.Value, out var value)) continue;

                if (isBaseline && value == 0) continue;

                var state = isBaseline
                    ? LiveEventStates.Active
                    : value == 0 ? LiveEventStates.Cleared : LiveEventStates.Triggered;

                catalog.TryGetValue(pair.Key, out var def);

                resolved.Add(new LiveEvent(
                    timestampUtc,
                    pair.Key,
                    value,
                    state,
                    def?.EventType ?? Unknown,
                    def?.Domain ?? Unknown,
                    def?.Severity ?? Unknown,
                    state == LiveEventStates.Cleared ? null : LookUpLabel(def, value)));
            }

            // Ordered so a batch always produces the same row keys in the same order, which keeps
            // an entity group transaction reproducible when it is retried.
            resolved.Sort((a, b) => string.CompareOrdinal(a.Parameter, b.Parameter));
            return resolved;
        }

        private static string? LookUpLabel(AlertDefinition? def, long value) =>
            def?.ValueLabels is { } labels && value <= int.MaxValue && value >= int.MinValue &&
            labels.TryGetValue((int)value, out var label)
                ? label
                : null;

        /// <summary>
        /// Coerces a parsed JSON value to the register value it represents.
        /// </summary>
        /// <remarks>
        /// The block reaches here through the same converter as the telemetry fields, so an integer
        /// arrives as long, a decimal as double, and a value read back from storage as JsonElement.
        /// A firmware revision quoting the number would send a string. All of them mean the same
        /// register value.
        /// </remarks>
        private static bool TryCoerce(object? raw, out long value)
        {
            switch (raw)
            {
                case null:
                    value = 0;
                    return false;
                case bool b:
                    value = b ? 1 : 0;
                    return true;
                case long l:
                    value = l;
                    return true;
                case int i:
                    value = i;
                    return true;
                case double d:
                    value = (long)Math.Round(d);
                    return true;
                case string s:
                    return long.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
                case JsonElement je:
                    switch (je.ValueKind)
                    {
                        case JsonValueKind.True: value = 1; return true;
                        case JsonValueKind.False: value = 0; return true;
                        case JsonValueKind.Number:
                            if (je.TryGetInt64(out value)) return true;
                            if (je.TryGetDouble(out var jd)) { value = (long)Math.Round(jd); return true; }
                            return false;
                        case JsonValueKind.String:
                            return TryCoerce(je.GetString(), out value);
                        default:
                            value = 0;
                            return false;
                    }
                default:
                    value = 0;
                    return false;
            }
        }
    }
}
```

- [ ] **Step 5: Remove the temporary constant from Task 2**

In `LivePolling/AlertDefinitionCatalog.cs`, delete the temporary `private const string Unknown = "unknown";` added in Task 2 Step 5. The three `LiveEventResolver.Unknown` references now resolve for real.

- [ ] **Step 6: Run to verify it passes**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveEventResolverTests|FullyQualifiedName~AlertDefinitionCatalogTests"`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add LivePolling/LiveEvent.cs LivePolling/LiveEventResolver.cs LivePolling/AlertDefinitionCatalog.cs shape-iot-function-data-ingestion.Tests/LivePolling/LiveEventResolverTests.cs
git commit -m "feat: resolve live polling event blocks against alert definitions"
```

---

## Task 4: Parse the Event block

**Files:**
- Modify: `shape-iot-function-data-ingestion/LivePolling/LiveTelemetryBatch.cs`
- Modify: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/LiveBatchPayloadSizeTests.cs:126`
- Modify: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/LiveTelemetryProcessorTests.cs:23-27`
- Test: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/LiveTelemetryBatchTests.cs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `LiveEntry(DateTime TimestampUtc, IDictionary<string, object?> Fields, bool IsBaseline, IDictionary<string, object?> Events)` — a fourth positional member. `Events` is empty, never null, when no block was present.

- [ ] **Step 1: Write the failing tests**

Append to `shape-iot-function-data-ingestion.Tests/LivePolling/LiveTelemetryBatchTests.cs`, inside the existing class:

```csharp
private const string EventBatch = """
{ "Entries": [
  { "TimestampUtc":"2026-09-28T10:27:01.2200000Z",
    "Body": { "MotorRunning": true, "OperatingPressurePSI": 54351 },
    "Event": { "OperatingAlarmActive": 0, "MotorRunning": 1 } },
  { "TimestampUtc":"2026-09-28T10:27:02.2180000Z",
    "Changes": { "OperatingPressurePSI": 54360 },
    "Event": { "OperatingAlarmActive": 5 } },
  { "TimestampUtc":"2026-09-28T10:27:03.1230000Z", "Changes": {} },
  { "TimestampUtc":"2026-09-28T10:27:04.2010000Z",
    "Changes": { "StrokeRateLeft": 97 }, "Event": {} },
  { "TimestampUtc":"2026-09-28T10:27:05.1900000Z",
    "Changes": {}, "Event": "not-an-object" } ] }
""";

[Fact]
public void TryParse_ReadsTheEventBlockBesideBodyAndChanges()
{
    var batch = LiveTelemetryBatch.TryParse(Event(Envelope(EventBatch)));

    Assert.NotNull(batch);
    Assert.Equal(5, batch!.Entries.Count);

    Assert.True(batch.Entries[0].IsBaseline);
    Assert.Equal(2, batch.Entries[0].Events.Count);
    Assert.Equal(0L, batch.Entries[0].Events["OperatingAlarmActive"]);

    Assert.False(batch.Entries[1].IsBaseline);
    Assert.Equal(5L, Assert.Single(batch.Entries[1].Events).Value);
}

[Fact]
public void TryParse_EntryWithNoEventBlock_HasAnEmptyMap()
{
    var batch = LiveTelemetryBatch.TryParse(Event(Envelope(EventBatch)));

    Assert.NotNull(batch!.Entries[2].Events);
    Assert.Empty(batch.Entries[2].Events);
}

[Fact]
public void TryParse_EmptyEventBlock_IsAcceptedAsEmpty()
{
    var batch = LiveTelemetryBatch.TryParse(Event(Envelope(EventBatch)));

    Assert.Empty(batch!.Entries[3].Events);
    Assert.Single(batch.Entries[3].Fields);
}

[Fact]
public void TryParse_NonObjectEventBlock_IsIgnoredAndTheEntryStillFolds()
{
    var batch = LiveTelemetryBatch.TryParse(Event(Envelope(EventBatch)));

    Assert.Empty(batch!.Entries[4].Events);
    Assert.Empty(batch.Entries[4].Fields);
}

[Fact]
public void TryParse_EventKeysAreCaseInsensitive()
{
    var batch = LiveTelemetryBatch.TryParse(Event(Envelope(EventBatch)));

    Assert.True(batch!.Entries[1].Events.ContainsKey("operatingalarmactive"));
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveTelemetryBatchTests"`
Expected: FAIL — `'LiveEntry' does not contain a definition for 'Events'`

- [ ] **Step 3: Extend the record**

In `LivePolling/LiveTelemetryBatch.cs`, replace the `LiveEntry` declaration:

```csharp
    /// <summary>One tick inside a batch.</summary>
    /// <param name="TimestampUtc">When the gateway sampled it.</param>
    /// <param name="Fields">A full snapshot when <paramref name="IsBaseline"/>, otherwise only what changed; empty for a heartbeat.</param>
    /// <param name="IsBaseline">True when the entry arrived under <c>Body</c> rather than <c>Changes</c>.</param>
    /// <param name="Events">
    /// The entry's <c>Event</c> block — parameter name to raw register value. Empty, never null,
    /// when the gateway sent no block. On a baseline this is the full event-state snapshot; on a
    /// delta it holds only what moved.
    /// </param>
    public sealed record LiveEntry(
        DateTime TimestampUtc,
        IDictionary<string, object?> Fields,
        bool IsBaseline,
        IDictionary<string, object?> Events);
```

- [ ] **Step 4: Parse the block**

In the same file, replace the entry loop body — the block from `var fields = new Dictionary...` through `entries.Add(...)`:

```csharp
                    var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    if (fieldsEl.ValueKind == JsonValueKind.Object)
                        foreach (var p in fieldsEl.EnumerateObject())
                            fields[p.Name] = ToClrValue(p.Value);

                    // Event is a sibling of Body and Changes, never nested inside them. A block that
                    // is present but not an object is a contract break; the entry's telemetry is
                    // still good, so it folds and only the events are dropped.
                    var events = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    if ((entryEl.TryGetProperty("Event", out var eventsEl) ||
                         entryEl.TryGetProperty("event", out eventsEl)) &&
                        eventsEl.ValueKind == JsonValueKind.Object)
                    {
                        foreach (var p in eventsEl.EnumerateObject())
                            events[p.Name] = ToClrValue(p.Value);
                    }

                    entries.Add(new LiveEntry(ts, fields, isBaseline, events));
```

- [ ] **Step 5: Fix the other construction sites**

Two test files construct `LiveEntry` directly and no longer compile.

`LiveBatchPayloadSizeTests.cs:126` — keep the existing second and third arguments exactly as they are and append an empty events map:

```csharp
                entries.Add(new LiveEntry(t0.AddSeconds(i),
                    // ... existing field dictionary argument unchanged ...
                    new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)));
```

`LiveTelemetryProcessorTests.cs:23-27` — the two helpers gain an empty events map, and two event-carrying variants join them for Task 6:

```csharp
        private static LiveEntry Baseline(DateTime ts, params (string K, object? V)[] f) =>
            new(ts, f.ToDictionary(x => x.K, x => x.V, StringComparer.OrdinalIgnoreCase), true,
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

        private static LiveEntry Delta(DateTime ts, params (string K, object? V)[] f) =>
            new(ts, f.ToDictionary(x => x.K, x => x.V, StringComparer.OrdinalIgnoreCase), false,
                new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase));

        private static LiveEntry BaselineWithEvents(
            DateTime ts, (string K, object? V)[] fields, params (string K, object? V)[] events) =>
            new(ts, fields.ToDictionary(x => x.K, x => x.V, StringComparer.OrdinalIgnoreCase), true,
                events.ToDictionary(x => x.K, x => x.V, StringComparer.OrdinalIgnoreCase));

        private static LiveEntry DeltaWithEvents(
            DateTime ts, (string K, object? V)[] fields, params (string K, object? V)[] events) =>
            new(ts, fields.ToDictionary(x => x.K, x => x.V, StringComparer.OrdinalIgnoreCase), false,
                events.ToDictionary(x => x.K, x => x.V, StringComparer.OrdinalIgnoreCase));
```

- [ ] **Step 6: Run the whole function suite**

Run: `dotnet test shape-iot-function-data-ingestion.Tests`
Expected: PASS — 124 existing tests plus the new ones.

- [ ] **Step 7: Commit**

```bash
git add LivePolling/LiveTelemetryBatch.cs shape-iot-function-data-ingestion.Tests/LivePolling/
git commit -m "feat: parse the gateway event block into live entries"
```

---

## Task 5: Store event rows

**Files:**
- Modify: `shape-iot-function-data-ingestion/LivePolling/ILiveSessionStore.cs`
- Modify: `shape-iot-function-data-ingestion/LivePolling/TableLiveSessionStore.cs`
- Modify: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/InMemoryLiveSessionStore.cs`
- Modify: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/InMemoryLiveSessionStoreTests.cs:19`
- Modify: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/LiveSessionCleanerTests.cs:45`
- Modify: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/LiveBatchPayloadSizeTests.cs:153`

**Interfaces:**
- Consumes: `LiveEvent` (Task 3), `LiveSessionKeys.EventRowKey` (Task 1).
- Produces: `ILiveSessionStore.WriteBatchAsync(..., IReadOnlyList<LiveTickWrite> changePoints, IReadOnlyList<LiveEvent> events, bool hasBaseline, string? expectedETag, CancellationToken ct)` — `events` is inserted directly after `changePoints`. `InMemoryLiveSessionStore` exposes `IReadOnlyList<LiveEvent> Events`.

- [ ] **Step 1: Write the failing test**

Append to `shape-iot-function-data-ingestion.Tests/LivePolling/InMemoryLiveSessionStoreTests.cs`:

```csharp
[Fact]
public async Task WriteBatchAsync_RecordsEventsAlongsideChangePoints()
{
    var snapshot = new Dictionary<string, object?> { ["OperatingPressure"] = 100 };
    var points = new[] { new LiveTickWrite("OperatingPressure", 100, Ts) };
    var events = new[]
    {
        new LiveEvent(Ts, "OperatingAlarmActive", 5, LiveEventStates.Triggered,
                      "alarm", "operational", "critical", "Emergency Stop Active")
    };

    await _store.WriteBatchAsync(Session, Device, snapshot, "Alarm", Ts, 1, 100, Ts, points,
                                 events, true, null, CancellationToken.None);

    var stored = Assert.Single(_store.Events);
    Assert.Equal("OperatingAlarmActive", stored.Parameter);
    Assert.Equal(LiveEventStates.Triggered, stored.State);
    Assert.Equal("Emergency Stop Active", stored.Label);
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~InMemoryLiveSessionStoreTests"`
Expected: FAIL — `No overload for method 'WriteBatchAsync' takes 13 arguments`

- [ ] **Step 3: Extend the interface**

In `LivePolling/ILiveSessionStore.cs`, add the parameter and its doc:

```csharp
        /// <summary>
        /// Writes the head row, every change point and every event from one batch as a single
        /// atomic transaction.
        /// </summary>
        /// <remarks>
        /// Returns false when <paramref name="expectedETag"/> no longer matches, meaning another
        /// instance folded first and the caller should re-read and reapply. Everything shares one
        /// partition precisely so the snapshot, its change points and its events can never disagree.
        /// </remarks>
        Task<bool> WriteBatchAsync(
            Guid sessionId,
            Guid deviceGuid,
            IDictionary<string, object?> snapshot,
            string deviceStatus,
            DateTime snapshotTimestampUtc,
            int tickCount,
            long bytesTotal,
            DateTime firstTickUtc,
            IReadOnlyList<LiveTickWrite> changePoints,
            IReadOnlyList<LiveEvent> events,
            bool hasBaseline,
            string? expectedETag,
            CancellationToken ct);
```

- [ ] **Step 4: Write the event rows**

In `LivePolling/TableLiveSessionStore.cs`, change the method signature to match, then add after the `foreach (var cp in changePoints)` loop:

```csharp
            foreach (var evt in events)
            {
                var row = new TableEntity(pk, LiveSessionKeys.EventRowKey(evt.Parameter, evt.TimestampUtc))
                {
                    // The gateway's tick, written explicitly: Table Storage's own Timestamp column
                    // is the service's write time, which is not when the condition changed.
                    ["TimestampUtc"] = new DateTimeOffset(DateTime.SpecifyKind(evt.TimestampUtc, DateTimeKind.Utc)),
                    ["Parameter"] = evt.Parameter,
                    ["Value"] = evt.Value,
                    ["State"] = evt.State,
                    ["EventType"] = evt.EventType,
                    ["Domain"] = evt.Domain,
                    ["Severity"] = evt.Severity,
                    ["Label"] = evt.Label
                };
                actions.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, row));
            }
```

Also update the class `<remarks>`: the EGT limit is 100 entities and a session's first batch now writes roughly 65 change points plus at most 18 events, still inside the limit.

- [ ] **Step 5: Update the test double**

In `shape-iot-function-data-ingestion.Tests/LivePolling/InMemoryLiveSessionStore.cs`, add the parameter to `WriteBatchAsync`, and a list that accumulates:

```csharp
        private readonly List<LiveEvent> _events = new();

        /// <summary>Every event recorded so far, in write order.</summary>
        public IReadOnlyList<LiveEvent> Events => _events;
```

Inside the method body, alongside where change points are recorded:

```csharp
            _events.AddRange(events);
```

- [ ] **Step 6: Fix the remaining call sites**

Add `Array.Empty<LiveEvent>(),` immediately after the `points` argument at:
- `InMemoryLiveSessionStoreTests.cs:19`
- `LiveSessionCleanerTests.cs:45`

And add the `IReadOnlyList<LiveEvent> events` parameter to the stub store's `WriteBatchAsync` at `LiveBatchPayloadSizeTests.cs:153`.

- [ ] **Step 7: Run the whole function suite**

Run: `dotnet test shape-iot-function-data-ingestion.Tests`
Expected: PASS

- [ ] **Step 8: Commit**

```bash
git add LivePolling/ILiveSessionStore.cs LivePolling/TableLiveSessionStore.cs shape-iot-function-data-ingestion.Tests/LivePolling/
git commit -m "feat: write live polling event rows in the session transaction"
```

---

## Task 6: Resolve events during the fold

**Files:**
- Modify: `shape-iot-function-data-ingestion/LivePolling/LiveTelemetryProcessor.cs`
- Modify: `shape-iot-function-data-ingestion/LivePolling/DeviceStatusDeriver.cs`
- Test: `shape-iot-function-data-ingestion/shape-iot-function-data-ingestion.Tests/LivePolling/LiveTelemetryProcessorTests.cs`

**Interfaces:**
- Consumes: `IAlertDefinitionCatalog` (Task 2), `LiveEventResolver.Resolve` (Task 3), `LiveEntry.Events` (Task 4), the extended `WriteBatchAsync` (Task 5).
- Produces: `BroadcastEntry(DateTime TimestampUtc, string DeviceStatus, IDictionary<string, object?>? Snapshot, IDictionary<string, object?>? Changes, IReadOnlyList<LiveEvent> Events)`. `LiveTelemetryProcessor`'s constructor becomes `(ILiveSessionStore store, IAlertDefinitionCatalog catalog, ILogger<LiveTelemetryProcessor> logger)`.

- [ ] **Step 1: Write the failing tests**

Append to `shape-iot-function-data-ingestion.Tests/LivePolling/LiveTelemetryProcessorTests.cs`. Add this fake near the existing test doubles:

```csharp
private sealed class FakeCatalog : IAlertDefinitionCatalog
{
    public static readonly IReadOnlyDictionary<string, AlertDefinition> Definitions =
        new Dictionary<string, AlertDefinition>(StringComparer.OrdinalIgnoreCase)
        {
            ["OperatingAlarmActive"] = new(
                "OperatingAlarmActive", 40006, "alarm", "operational", "critical",
                "Indicates an active operational fault condition.",
                new Dictionary<int, string> { [0] = "No Fault Active", [5] = "Emergency Stop Active" }),
            ["MaintenanceAlarmActive"] = new(
                "MaintenanceAlarmActive", 40008, "alarm", "maintenance", "critical",
                "Wear parts lifetime reached.", null)
        };

    public Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default)
        => Task.FromResult(Definitions);
}
```

Update the fixture's constructor for the new second argument:

```csharp
        public LiveTelemetryProcessorTests() =>
            _processor = new LiveTelemetryProcessor(
                _store, new FakeCatalog(), NullLogger<LiveTelemetryProcessor>.Instance);
```

Then the tests, using the shared `_store` / `_processor` and the `BaselineWithEvents` / `DeltaWithEvents` helpers added in Task 4 Step 5:

```csharp
        [Fact]
        public async Task BaselineEvents_AreRecordedAsActiveAndZerosAreDropped()
        {
            await _processor.ProcessAsync(
                Batch(BaselineWithEvents(T0,
                    new[] { ("OperatingPressure", (object?)100) },
                    ("OperatingAlarmActive", 5L), ("MaintenanceAlarmActive", 0L))),
                Device, CancellationToken.None);

            var stored = Assert.Single(_store.Events);
            Assert.Equal("OperatingAlarmActive", stored.Parameter);
            Assert.Equal(LiveEventStates.Active, stored.State);
            Assert.Equal("Emergency Stop Active", stored.Label);
        }

        [Fact]
        public async Task DeltaEvents_AreTriggeredThenCleared()
        {
            await _processor.ProcessAsync(
                Batch(Baseline(T0, ("OperatingPressure", 100))), Device, CancellationToken.None);

            await _processor.ProcessAsync(
                Batch(
                    DeltaWithEvents(T0.AddSeconds(1), Array.Empty<(string, object?)>(),
                        ("OperatingAlarmActive", 5L)),
                    DeltaWithEvents(T0.AddSeconds(2), Array.Empty<(string, object?)>(),
                        ("OperatingAlarmActive", 0L))),
                Device, CancellationToken.None);

            Assert.Equal(2, _store.Events.Count);
            Assert.Equal(LiveEventStates.Triggered, _store.Events[0].State);
            Assert.Equal("Emergency Stop Active", _store.Events[0].Label);
            Assert.Equal(LiveEventStates.Cleared, _store.Events[1].State);
            Assert.Null(_store.Events[1].Label);
        }

        [Fact]
        public async Task Events_RideAlongInTheBroadcast()
        {
            var broadcast = await _processor.ProcessAsync(
                Batch(BaselineWithEvents(T0,
                    new[] { ("OperatingPressure", (object?)100) },
                    ("OperatingAlarmActive", 5L))),
                Device, CancellationToken.None);

            Assert.NotNull(broadcast);
            var entry = Assert.Single(broadcast!.Entries);
            Assert.Equal("OperatingAlarmActive", Assert.Single(entry.Events).Parameter);
        }

        [Fact]
        public async Task TickWithNoEvents_BroadcastsAnEmptyListAndStoresNothing()
        {
            var broadcast = await _processor.ProcessAsync(
                Batch(Baseline(T0, ("OperatingPressure", 100))), Device, CancellationToken.None);

            Assert.Empty(Assert.Single(broadcast!.Entries).Events);
            Assert.Empty(_store.Events);
        }

        [Fact]
        public async Task RedeliveredBatch_WritesNoDuplicateEvents()
        {
            var batch = Batch(BaselineWithEvents(T0,
                new[] { ("OperatingPressure", (object?)100) },
                ("OperatingAlarmActive", 5L)));

            await _processor.ProcessAsync(batch, Device, CancellationToken.None);
            await _processor.ProcessAsync(batch, Device, CancellationToken.None);

            Assert.Single(_store.Events);
        }

        [Fact]
        public async Task OutOfOrderBaseline_StillRecordsItsActiveEvents()
        {
            // The delta lands first; the baseline that precedes it arrives afterwards.
            await _processor.ProcessAsync(
                Batch(Delta(T0.AddSeconds(5), ("OperatingPressure", 200))), Device, CancellationToken.None);

            await _processor.ProcessAsync(
                Batch(BaselineWithEvents(T0,
                    new[] { ("StrokeRateLeft", (object?)97) },
                    ("OperatingAlarmActive", 5L))),
                Device, CancellationToken.None);

            var stored = Assert.Single(_store.Events);
            Assert.Equal(LiveEventStates.Active, stored.State);
            Assert.Equal(T0, stored.TimestampUtc);
        }

        [Fact]
        public async Task UnknownParameter_IsStoredWithUnknownMetadata()
        {
            await _processor.ProcessAsync(
                Batch(DeltaWithEvents(T0, new[] { ("OperatingPressure", (object?)100) },
                    ("SomeNewAlarm", 3L))),
                Device, CancellationToken.None);

            var stored = Assert.Single(_store.Events);
            Assert.Equal("SomeNewAlarm", stored.Parameter);
            Assert.Equal(LiveEventResolver.Unknown, stored.EventType);
        }
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveTelemetryProcessorTests"`
Expected: FAIL — the `LiveTelemetryProcessor` constructor takes two arguments, and `BroadcastEntry` has no `Events`.

- [ ] **Step 3: Extend `BroadcastEntry` and the constructor**

In `LivePolling/LiveTelemetryProcessor.cs`:

```csharp
    /// <summary>One tick as the browser receives it.</summary>
    /// <param name="TimestampUtc">When the gateway sampled it.</param>
    /// <param name="DeviceStatus">Device-level status at that instant.</param>
    /// <param name="Snapshot">Every field's value — set on the first entry of a batch only.</param>
    /// <param name="Changes">Only what moved — set on every entry after the first.</param>
    /// <param name="Events">Resolved event transitions on this tick; empty on most ticks.</param>
    public sealed record BroadcastEntry(
        DateTime TimestampUtc,
        string DeviceStatus,
        IDictionary<string, object?>? Snapshot,
        IDictionary<string, object?>? Changes,
        IReadOnlyList<LiveEvent> Events);
```

And the constructor:

```csharp
        private readonly ILiveSessionStore _store;
        private readonly IAlertDefinitionCatalog _catalog;
        private readonly ILogger<LiveTelemetryProcessor> _logger;

        public LiveTelemetryProcessor(
            ILiveSessionStore store,
            IAlertDefinitionCatalog catalog,
            ILogger<LiveTelemetryProcessor> logger)
        {
            _store = store;
            _catalog = catalog;
            _logger = logger;
        }
```

- [ ] **Step 4: Resolve inside `ProcessAsync`**

Load the catalog once per call, immediately before the retry loop:

```csharp
            // Loaded once per batch rather than per entry. An empty map — a failed load — still
            // produces events, with unknown metadata: live telemetry does not stop because an
            // alert-definition read failed.
            var definitions = await _catalog.GetAsync(ct);
```

Declare the accumulator beside `points` and `broadcast`:

```csharp
                var events = new List<LiveEvent>();
```

In the **back-fill branch** — inside `if (!entry.IsBaseline || hasBaseline) continue;`'s block, after the field loop and before `hasBaseline = true;`:

```csharp
                        events.AddRange(LiveEventResolver.Resolve(
                            entry.TimestampUtc, entry.Events, isBaseline: true, definitions));
```

In the **accepted-entry branch**, after the `foreach (var kv in entry.Fields)` change-point loop:

```csharp
                    var entryEvents = LiveEventResolver.Resolve(
                        entry.TimestampUtc, entry.Events, entry.IsBaseline, definitions);
                    events.AddRange(entryEvents);
```

Then pass `entryEvents` into both `BroadcastEntry` constructions in that branch, and `Array.Empty<LiveEvent>()` into the back-fill-only broadcast near the end:

```csharp
                    broadcast.Add(accepted == 1
                        ? new BroadcastEntry(entry.TimestampUtc, newStatus,
                            new Dictionary<string, object?>(snapshot, StringComparer.OrdinalIgnoreCase), null,
                            entryEvents)
                        : new BroadcastEntry(entry.TimestampUtc, newStatus, null,
                            new Dictionary<string, object?>(entry.Fields, StringComparer.OrdinalIgnoreCase),
                            entryEvents));
```

```csharp
                if (broadcast.Count == 0)
                    broadcast.Add(new BroadcastEntry(lastAccepted, status,
                        new Dictionary<string, object?>(snapshot, StringComparer.OrdinalIgnoreCase), null,
                        events));
```

Finally pass `events` to the store, directly after `points`:

```csharp
                    points,
                    events,
                    hasBaseline,
```

- [ ] **Step 5: Correct the `DeviceStatusDeriver` remark**

In `LivePolling/DeviceStatusDeriver.cs`, the class `<remarks>` claims the live path keeps zero database dependencies. That is no longer true of the pipeline. Replace that sentence:

```csharp
    /// <remarks>
    /// These four fields are already in the payload — they are TelemetryMeasures columns — so this
    /// needs no SQL and no alert-definition lookup, even though the pipeline around it now reads
    /// dbo.DeviceAlertDefinitions to resolve the event block. This is deliberately device-level,
    /// not per-metric: a per-metric status would mean evaluating alert thresholds, which is a
    /// different and much larger dependency.
    /// </remarks>
```

- [ ] **Step 6: Run the whole function suite**

Run: `dotnet test shape-iot-function-data-ingestion.Tests`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add LivePolling/LiveTelemetryProcessor.cs LivePolling/DeviceStatusDeriver.cs shape-iot-function-data-ingestion.Tests/LivePolling/LiveTelemetryProcessorTests.cs
git commit -m "feat: resolve and store event transitions during the live fold"
```

---

## Task 7: Broadcast events and register the catalog

**Files:**
- Modify: `shape-iot-function-data-ingestion/Functions/LiveTelemetryProcessorEventGrid.cs`
- Modify: `shape-iot-function-data-ingestion/Program.cs`

**Interfaces:**
- Consumes: `BroadcastEntry.Events` (Task 6), `IAlertDefinitionCatalog` (Task 2).
- Produces: the SignalR `liveTelemetryUpdate` payload now carries `events` per entry.

There is no unit test here — this function is a thin composition over `LiveTelemetryProcessor`, which Task 6 covers, and the existing suite has no test for it. Verification is the build plus the whole suite.

- [ ] **Step 1: Add events to the broadcast body**

In `Functions/LiveTelemetryProcessorEventGrid.cs`, extend the anonymous projection:

```csharp
                entries = broadcast.Entries.Select(e => new
                {
                    timestampUtc = e.TimestampUtc,
                    deviceStatus = e.DeviceStatus,
                    snapshot = e.Snapshot,
                    changes = e.Changes,
                    events = e.Events.Count == 0 ? null : e.Events.Select(v => new
                    {
                        parameter = v.Parameter,
                        value = v.Value,
                        state = v.State,
                        eventType = v.EventType,
                        domain = v.Domain,
                        severity = v.Severity,
                        label = v.Label
                    })
                })
```

`null` rather than an empty array on quiet ticks: `System.Text.Json` writes `"events":null`, which costs six bytes against a batch that is billed in 2 KB SignalR chunks, and the client tests for it the same way either way.

- [ ] **Step 2: Register the catalog**

In `Program.cs`, beside the other live polling registrations (after line 79, `AddSingleton<LiveTelemetryProcessor>()`):

```csharp
builder.Services.AddSingleton<IAlertDefinitionCatalog, AlertDefinitionCatalog>();
```

Singleton, so the load-once cache spans the host's lifetime. `LiveTelemetryProcessor` is already a singleton and now takes the catalog; no other registration changes.

- [ ] **Step 3: Build and run the whole suite**

Run: `dotnet build && dotnet test shape-iot-function-data-ingestion.Tests`
Expected: build succeeds, all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add Functions/LiveTelemetryProcessorEventGrid.cs Program.cs
git commit -m "feat: broadcast live polling events over SignalR"
```

---

## Task 8: Read event rows in the backend

**Files:**
- Modify: `shape-iot-backend/Brilliant.IoT.Application/LivePolling/ILiveSessionReader.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.Infrastructure/LivePolling/TableLiveSessionReader.cs`

**Interfaces:**
- Consumes: `LiveSessionKeys.EventRange` (Task 1), the row columns written in Task 5.
- Produces:
  - `LiveEventPoint(DateTime TimestampUtc, string Parameter, long Value, string State, string EventType, string Domain, string Severity, string? Label)`
  - `ILiveSessionReader.GetEventsAsync(Guid sessionId, DateTime? since = null, CancellationToken ct = default) -> Task<IReadOnlyList<LiveEventPoint>>`

- [ ] **Step 1: Add the record and interface method**

In `Brilliant.IoT.Application/LivePolling/ILiveSessionReader.cs`, after `LiveSeriesPoint`:

```csharp
/// <summary>One recorded event occurrence, already resolved by the ingestion function.</summary>
/// <param name="TimestampUtc">Tick the transition was reported on.</param>
/// <param name="Parameter">Telemetry parameter name, e.g. OperatingAlarmActive.</param>
/// <param name="Value">Raw register value, e.g. 5.</param>
/// <param name="State">triggered, cleared, or active.</param>
/// <param name="EventType">alarm, warning, maintenance, status, or unknown.</param>
/// <param name="Domain">operational, maintenance, or unknown.</param>
/// <param name="Severity">critical, major, minor, or unknown.</param>
/// <param name="Label">Human text for the value; null for the boolean parameters.</param>
public sealed record LiveEventPoint(
    DateTime TimestampUtc,
    string Parameter,
    long Value,
    string State,
    string EventType,
    string Domain,
    string Severity,
    string? Label);
```

And on the interface:

```csharp
    /// <summary>Every event recorded in the session, oldest first.</summary>
    /// <param name="sessionId">Session whose partition to read.</param>
    /// <param name="since">Optional lower bound; events before it are skipped.</param>
    /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
    /// <returns>The occurrences, ordered by timestamp.</returns>
    Task<IReadOnlyList<LiveEventPoint>> GetEventsAsync(
        Guid sessionId, DateTime? since = null, CancellationToken ct = default);
```

- [ ] **Step 2: Implement the range scan**

In `Brilliant.IoT.Infrastructure/LivePolling/TableLiveSessionReader.cs`:

```csharp
    /// <inheritdoc />
    public async Task<IReadOnlyList<LiveEventPoint>> GetEventsAsync(
        Guid sessionId, DateTime? since = null, CancellationToken ct = default)
    {
        var pk = sessionId.ToString();
        var (from, to) = LiveSessionKeys.EventRange(since);

        var filter = TableClient.CreateQueryFilter(
            $"PartitionKey eq {pk} and RowKey ge {from} and RowKey lt {to}");

        var events = new List<LiveEventPoint>();
        await foreach (var e in _table.QueryAsync<TableEntity>(filter, cancellationToken: ct))
        {
            // The timestamp is read from the columns rather than parsed back out of the RowKey:
            // the key exists to make the range scan cheap, not to be the record.
            var parameter = e.GetString("Parameter");
            if (string.IsNullOrEmpty(parameter)) continue;

            events.Add(new LiveEventPoint(
                Normalise(e.GetDateTime("TimestampUtc") ?? DateTime.MinValue),
                parameter,
                e.GetInt64("Value") ?? 0,
                e.GetString("State") ?? string.Empty,
                e.GetString("EventType") ?? string.Empty,
                e.GetString("Domain") ?? string.Empty,
                e.GetString("Severity") ?? string.Empty,
                e.GetString("Label")));
        }

        events.Sort((a, b) => a.TimestampUtc.CompareTo(b.TimestampUtc));
        return events;
    }

    private static DateTime Normalise(DateTime value) => value.Kind switch
    {
        DateTimeKind.Utc => value,
        DateTimeKind.Local => value.ToUniversalTime(),
        _ => DateTime.SpecifyKind(value, DateTimeKind.Utc)
    };
```

- [ ] **Step 3: Build**

Run: `dotnet build`
Expected: succeeds. The reader has no unit tests today — its behaviour is covered through the controller tests in Task 9, which use a fake reader.

- [ ] **Step 4: Commit**

```bash
git add Brilliant.IoT.Application/LivePolling/ILiveSessionReader.cs Brilliant.IoT.Infrastructure/LivePolling/TableLiveSessionReader.cs
git commit -m "feat: read live polling event rows from the session partition"
```

---

## Task 9: Return events from the series endpoint

**Files:**
- Modify: `shape-iot-backend/Brilliant.IoT.Application/DTOs/LivePolling/LivePollingDtos.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.API/Controllers/LivePollingController.cs`
- Test: `shape-iot-backend/Brilliant.IoT.UnitTests/Controllers/LivePollingControllerTests.cs`

**Interfaces:**
- Consumes: `ILiveSessionReader.GetEventsAsync` (Task 8).
- Produces: `LivePollingSeriesDto.Status` is now `IReadOnlyList<int>`; `LivePollingSeriesDto.Events` is `IReadOnlyList<LivePollingEventDto>`.

- [ ] **Step 1: Unblock the existing series tests**

`_reader` is `Mock<ILiveSessionReader>(MockBehavior.Strict)`. A strict mock throws on any call with no matching setup, so the moment the controller starts calling `GetEventsAsync` **every existing series test fails**. Add this helper beside `GivenSnapshotSpanning` in `Brilliant.IoT.UnitTests/Controllers/LivePollingControllerTests.cs`:

```csharp
    private void GivenEvents(params LiveEventPoint[] events) =>
        _reader.Setup(r => r.GetEventsAsync(
                   It.IsAny<Guid>(), It.IsAny<DateTime?>(), It.IsAny<CancellationToken>()))
               .ReturnsAsync(events.ToList());
```

Then add a `GivenEvents();` call to each existing `Series_*` test, immediately after its `GivenSnapshotSpanning(...)` line. Find them with:

Run: `grep -n "public async Task Series_" Brilliant.IoT.UnitTests/Controllers/LivePollingControllerTests.cs`

- [ ] **Step 2: Write the failing tests**

Append to the same file, following its existing Moq + FluentAssertions style:

```csharp
    [Fact]
    public async Task Series_ReturnsEventsOnTheDefaultCall()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        var t0 = new DateTime(2026, 9, 28, 10, 0, 0, DateTimeKind.Utc);
        GivenSnapshotSpanning(t0, t0.AddSeconds(2), 3);
        GivenEvents(new LiveEventPoint(
            t0.AddSeconds(2), "OperatingAlarmActive", 5, "triggered",
            "alarm", "operational", "critical", "Emergency Stop Active"));

        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "DeviceStatus", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, "Normal") });
        foreach (var m in new[] { "OperatingPressurePSI", "StrokeRateLeft" })
            _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), m, null, It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, 1) });

        var result = await _controller.GetSeriesAsync(SerialNumber, null, null, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingSeriesDto>>().Subject;
        body.Data!.Events.Should().HaveCount(1);
        body.Data.Events[0].Parameter.Should().Be("OperatingAlarmActive");
        body.Data.Events[0].Value.Should().Be(5);
        body.Data.Events[0].State.Should().Be("triggered");
        body.Data.Events[0].Label.Should().Be("Emergency Stop Active");
    }

    [Fact]
    public async Task Series_NamedMetric_StillReturnsEvents()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        var t0 = new DateTime(2026, 9, 28, 10, 0, 0, DateTimeKind.Utc);
        GivenSnapshotSpanning(t0, t0.AddSeconds(2), 3);
        GivenEvents(new LiveEventPoint(
            t0.AddSeconds(2), "MaintenanceAlarmActive", 1, "triggered",
            "alarm", "maintenance", "critical", null));

        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "DeviceStatus", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, "Normal") });
        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "OperatingPressurePSI", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, 1) });

        var result = await _controller.GetSeriesAsync(
            SerialNumber, new[] { "OperatingPressurePSI" }, null, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingSeriesDto>>().Subject;
        body.Data!.Series.Should().HaveCount(1, "only one metric was asked for");
        body.Data.Events.Should().HaveCount(1, "markers must survive a legend change");
        body.Data.Events[0].Label.Should().BeNull("boolean parameters have no value table");
    }

    [Fact]
    public async Task Series_NoEvents_ReturnsAnEmptyListNotNull()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        var t0 = new DateTime(2026, 9, 28, 10, 0, 0, DateTimeKind.Utc);
        GivenSnapshotSpanning(t0, t0.AddSeconds(2), 3);
        GivenEvents();

        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "DeviceStatus", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, "Normal") });
        foreach (var m in new[] { "OperatingPressurePSI", "StrokeRateLeft" })
            _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), m, null, It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, 1) });

        var result = await _controller.GetSeriesAsync(SerialNumber, null, null, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingSeriesDto>>().Subject;
        body.Data!.Events.Should().NotBeNull().And.BeEmpty();
    }

    [Fact]
    public async Task Series_PassesSinceThroughToTheEventRead()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        var t0 = new DateTime(2026, 9, 28, 10, 0, 0, DateTimeKind.Utc);
        var since = t0.AddSeconds(1);
        GivenSnapshotSpanning(t0, t0.AddSeconds(2), 3);
        GivenEvents();

        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "DeviceStatus", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, "Normal") });
        foreach (var m in new[] { "OperatingPressurePSI", "StrokeRateLeft" })
            _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), m, null, It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, 1) });

        await _controller.GetSeriesAsync(SerialNumber, null, since, CancellationToken.None);

        _reader.Verify(r => r.GetEventsAsync(It.IsAny<Guid>(), since, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Series_MapsDeviceStatusToIntegers()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        var t0 = new DateTime(2026, 9, 28, 10, 0, 0, DateTimeKind.Utc);
        GivenSnapshotSpanning(t0, t0.AddSeconds(2), 3);
        GivenEvents();

        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "DeviceStatus", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint>
               {
                   new(t0, "Normal"), new(t0.AddSeconds(1), "Warning"), new(t0.AddSeconds(2), "Alarm")
               });
        foreach (var m in new[] { "OperatingPressurePSI", "StrokeRateLeft" })
            _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), m, null, It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, 1) });

        var result = await _controller.GetSeriesAsync(SerialNumber, null, null, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingSeriesDto>>().Subject;
        body.Data!.Status.Should().Equal(0, 1, 2);
    }

    [Fact]
    public async Task Series_StatusBeforeTheFirstReading_IsNormal()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        var t0 = new DateTime(2026, 9, 28, 10, 0, 0, DateTimeKind.Utc);
        GivenSnapshotSpanning(t0, t0.AddSeconds(2), 3);
        GivenEvents();

        // No status point at all: every grid point expands to null.
        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "DeviceStatus", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint>());
        foreach (var m in new[] { "OperatingPressurePSI", "StrokeRateLeft" })
            _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), m, null, It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new List<LiveSeriesPoint> { new(t0, 1) });

        var result = await _controller.GetSeriesAsync(SerialNumber, null, null, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingSeriesDto>>().Subject;
        body.Data!.Status.Should().Equal(0, 0, 0);
    }
```

- [ ] **Step 3: Run to verify it fails**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePollingControllerTests"`
Expected: FAIL — `'LivePollingSeriesDto' does not contain a definition for 'Events'`

- [ ] **Step 4: Add the DTO and change the status type**

In `Brilliant.IoT.Application/DTOs/LivePolling/LivePollingDtos.cs`:

```csharp
/// <summary>One event occurrence during the session.</summary>
public sealed class LivePollingEventDto
{
    /// <summary>Tick the transition was reported on.</summary>
    public DateTime TimestampUtc { get; set; }

    /// <summary>Telemetry parameter name, e.g. OperatingAlarmActive.</summary>
    public string Parameter { get; set; } = string.Empty;

    /// <summary>Raw register value, e.g. 5.</summary>
    public long Value { get; set; }

    /// <summary>
    /// <c>triggered</c>, <c>cleared</c>, or <c>active</c> — the last meaning the condition was
    /// already in effect when the session opened, so no transition should be drawn for it.
    /// </summary>
    public string State { get; set; } = string.Empty;

    /// <summary><c>alarm</c>, <c>warning</c>, <c>maintenance</c>, <c>status</c>, or <c>unknown</c>.</summary>
    public string EventType { get; set; } = string.Empty;

    /// <summary><c>operational</c>, <c>maintenance</c>, or <c>unknown</c>.</summary>
    public string Domain { get; set; } = string.Empty;

    /// <summary><c>critical</c>, <c>major</c>, <c>minor</c>, or <c>unknown</c>.</summary>
    public string Severity { get; set; } = string.Empty;

    /// <summary>
    /// Human text for this value. Non-null only for OperatingAlarmActive and OperatingWarningActive
    /// — the other definitions are booleans with no value table.
    /// </summary>
    public string? Label { get; set; }
}
```

And on `LivePollingSeriesDto`, replace `Status` and add `Events`:

```csharp
    /// <summary>Device status per grid point: 0 Normal, 1 Warning, 2 Alarm.</summary>
    public IReadOnlyList<int> Status { get; set; } = Array.Empty<int>();

    /// <summary>
    /// Event transitions during the session, oldest first. Not aligned to the grid — these are
    /// discrete occurrences, and two may share one timestamp.
    /// </summary>
    public IReadOnlyList<LivePollingEventDto> Events { get; set; } = Array.Empty<LivePollingEventDto>();
```

- [ ] **Step 5: Populate them in the controller**

In `Brilliant.IoT.API/Controllers/LivePollingController.cs`, replace the status projection:

```csharp
        var status = SeriesStepExpander.Expand(grid, statusPoints)
                                       .Select(ToStatusCode)
                                       .ToList();
```

Add the mapper as a private static member on the controller:

```csharp
    /// <summary>
    /// Maps the stored status string onto the wire's integer encoding.
    /// </summary>
    /// <remarks>
    /// A grid point before the first recorded status carries null, which is Normal: the session had
    /// not yet reported anything, and no alarm was known.
    /// </remarks>
    private static int ToStatusCode(object? value) => value as string switch
    {
        "Alarm" => 2,
        "Warning" => 1,
        _ => 0
    };
```

Then read the events after the series loop, and add them to the response:

```csharp
        var events = await _reader.GetEventsAsync(session.SessionId, since, ct);
```

```csharp
            Status              = status,
            Series              = series,
            Events              = events.Select(e => new LivePollingEventDto
            {
                TimestampUtc = e.TimestampUtc,
                Parameter    = e.Parameter,
                Value        = e.Value,
                State        = e.State,
                EventType    = e.EventType,
                Domain       = e.Domain,
                Severity     = e.Severity,
                Label        = e.Label
            }).ToList()
```

- [ ] **Step 6: Update the endpoint's XML docs**

The `<remarks>` on `GetSeriesAsync` currently says "With one, returns only that metric." Replace that paragraph:

```csharp
    /// <remarks>
    /// With no <c>metric</c>, returns the configured default set. With one or more, returns exactly
    /// those, up to ten. Timestamps, device status and events are shared across every metric rather
    /// than repeated inside each, and are returned whichever variant is called — the chart draws
    /// status and event markers regardless of which metrics are selected.
    /// Storage holds only the moments a value changed; values are carried forward onto the tick grid
    /// so the series is evenly spaced. Events are not carried forward — they are discrete
    /// occurrences with their own timestamps.
    /// Sample: <c>GET /api/v1/devices/PUMP-00417/live-session/series?metric=OperatingPressure&amp;metric=StrokeRateLeft</c>
    /// </remarks>
```

- [ ] **Step 7: Run the whole backend suite**

Run: `dotnet test`
Expected: PASS — 393 existing tests plus the new ones.

- [ ] **Step 8: Commit**

```bash
git add Brilliant.IoT.Application/DTOs/LivePolling/LivePollingDtos.cs Brilliant.IoT.API/Controllers/LivePollingController.cs Brilliant.IoT.UnitTests/Controllers/LivePollingControllerTests.cs
git commit -m "feat: return live polling events and integer status from the series endpoint"
```

---

## Task 10: Update the frontend API reference

**Files:**
- Modify: `docs/live-polling-frontend-integration.md`

No tests. This document is the frontend's contract and is currently wrong in two places.

- [ ] **Step 1: Correct the series response schema**

Find the `GET …/live-session/series` response table. Change the `status` row's type from an array of strings to an array of integers, and document the encoding: `0` Normal, `1` Warning, `2` Alarm. Add an `events` row: array of event objects, always present, empty when nothing occurred, returned on both the default and the filtered call.

- [ ] **Step 2: Add the event object schema**

Directly beneath the series response, add a field table for the event object: `timestampUtc` (ISO-8601 UTC), `parameter` (string), `value` (integer), `state` (string), `eventType` (string), `domain` (string), `severity` (string), `label` (string or null). Note that `label` is non-null only for `OperatingAlarmActive` and `OperatingWarningActive`, and that events are not grid-aligned, so two may share a timestamp.

- [ ] **Step 3: Add the enumerations**

In the document's enumerations section:

- `state`: `triggered`, `cleared`, `active`. `active` means the condition was already in effect when the session opened — record it, do not draw a transition marker for it.
- `eventType`: `alarm`, `warning`, `maintenance`, `status`, `unknown`. `status` is `MotorRunning`; it never affects `status[]`.
- `domain`: `operational`, `maintenance`, `unknown`.
- `severity`: `critical`, `major`, `minor`, `unknown`.

Explain `unknown`: the gateway reported a parameter with no row in `DeviceAlertDefinitions`. The event is still delivered rather than dropped, so the frontend should render it defensively rather than assume the metadata is present.

- [ ] **Step 4: Update the SignalR contract**

In the SignalR section, add `events` to the per-entry shape with the same field table, and state that the key is `null` — not an empty array — on ticks with no transition. The gzip envelope is unchanged.

- [ ] **Step 5: Commit**

Docs live at the `D:\Shape-IOT` root, which is not a git repository, so there is nothing to commit. Confirm the file is saved and move on.

---

## Verification

After Task 10, from `D:\Shape-IOT`:

```bash
cd shape-iot-function-data-ingestion && dotnet build && dotnet test
cd ../shape-iot-backend && dotnet build && dotnet test
```

Both suites green. Then confirm no unintended repo picked up changes:

```bash
cd shape-iot-function-data-ingestion && git status --short && git log --oneline dev..HEAD
cd ../shape-iot-backend && git status --short && git log --oneline dev..HEAD
```

`migrations` must be untouched — this feature adds no schema. Nothing is pushed.

## Deferred

Not in this plan, carried forward from the live polling work:

- Azure Event Grid subscription filters and app settings (original plan's Task 14).
- `Brilliant.IoT.IntegrationTests` needs Docker, unavailable on this machine; `LivePollingSessionRepositoryTests` remains unrun, and an event-row integration test should join it when Docker exists.
- The real gateway module id — `PollerModule` is still a placeholder.
- The four `DefaultSeriesMetrics` values.
