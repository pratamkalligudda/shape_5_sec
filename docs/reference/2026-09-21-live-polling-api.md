# Live Polling API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an authorised user start a bounded 1-second telemetry session on one device, watch it live in the browser, and read back per-parameter series — without a single row reaching the telemetry database.

**Architecture:** The gateway module already owns the fast-polling cadence and its own auto-shutoff; the backend only issues direct methods and keeps a `LivePollingSession` row for gating. Fast-polling messages are split off at the Event Grid subscription so the existing ingestion pipeline never sees them; a new function folds each delta onto a running snapshot held in Azure Table Storage, derives a device status, and pushes the full snapshot over SignalR. A cleanup timer deletes each session's partition once the session is over.

**Tech Stack:** .NET 8, ASP.NET Core (backend), Azure Functions isolated worker v2, Azure Table Storage (`Azure.Data.Tables`), Azure SignalR, Azure SQL, xunit + FluentAssertions + Moq.

**Spec:** `docs/superpowers/specs/2026-09-20-live-polling-api-design.md`

## Global Constraints

- **Never write to `TelemetryRaw`, `TelemetryMeasures`, `TelemetryEvents` or `DeviceLatestTelemetry` from any code in this plan.** This is the feature's central acceptance criterion.
- Module parameter ranges are fixed by the gateway: `sessionDurationMinutes` **10–30**, `pollIntervalSeconds` **1–60**. Backend clamps into these before calling.
- The gateway batches **5 ticks per message**, gzip-compressed, as `{ "Entries": [ {TimestampUtc, Body|Changes} ] }`. Properties are `batchSentUtc`, `session-id`, `telemetry-type`.
- Extend is **`Extend-FastPolling`**, requires `{ sessionId, extendSessionDurationMinutes }`, extends **total** duration from the original start, capped at **30 minutes total** by the module. New expiry is `StartedUtc + totalDurationMinutes` from its response — never `ExpiresUtc + N`.
- SignalR batch must serialise to **under 4,096 bytes** (a batch is ~2.6 KB). A test enforces this.
- Storage client must retry **503 Server Busy** and **500 Operation Timeout** with exponential backoff.
- Entity group transactions: **≤100 entities, <4 MiB, single PartitionKey**.
- Response envelope is `ApiResponse<T>` (`Succeeded`, `Message`, `Errors`, `Data`) with `.Success(data, message)` / `.Failure(message, errors)`.
- Authorisation attribute is `[RequirePrivilege("LivePolling")]` from `Brilliant.IoT.API.Authorization`.
- Repositories use `IDbConnectionFactory` + raw `SqlCommand` with named parameters. No Dapper, no EF for these.
- Tests: xunit, FluentAssertions, `Mock<T>(MockBehavior.Strict)`.
- XML doc conventions from `CLAUDE.md` apply to every controller action and `[Function]` method: `<summary>` before attributes on classes; `<summary>`/`<remarks>`/`<param>`/`<returns>` in that order on methods; `ct` is always "Cancellation token for cooperative request cancellation."; timer `timer`/`timerInfo` is "Timer metadata provided by the Azure Functions runtime."
- Commit messages: single short title line, no body, no `Co-Authored-By`.

## Branches — confirm before any code

Three repos are touched. Each needs its own branch off an up-to-date `dev`. **Confirm with the user before creating them.**

| Repo | Branch |
|---|---|
| `migrations` | `feature/live-polling-session` |
| `shape-iot-backend` | `feature/live-polling-api` |
| `shape-iot-function-data-ingestion` | `feature/live-polling-processor` |

## Task order and dependencies

```
T1 migrations ─┬─► T2..T7 backend (control plane)
               └─► T8..T11 data-ingestion (data plane)
                          └─► T12..T13 backend (read plane, needs the store shape)
                                      └─► T14 Azure config + dev verification
```

T8's `LiveSessionKeys` and entity shape are reproduced verbatim in T12, so backend and function agree without a shared library. `ShapeIoT.Telemetry.Shared` is vendored into three repos and guarded by `tools/Verify-TelemetryShared.ps1` — **do not add to it.**

---

## Task 1: Database migration — session table and privilege

**Files:**
- Create: `migrations/2026-09-21-01-live-polling-session.sql`
- Create: `migrations/2026-09-21-02-live-polling-privilege.sql`

**Interfaces:**
- Consumes: nothing
- Produces: table `dbo.LivePollingSession`; privilege code `LivePolling` mapped to roles `Admin`, `Engineering`, `Service`

Migrations in this repo are flat files named `YYYY-MM-DD-NN-description.sql`, idempotent, with `GO` between batches.

- [ ] **Step 1: Write the session table migration**

```sql
-- Migration: LivePollingSession
-- Date:      2026-09-21
-- Idempotent.

IF OBJECT_ID('dbo.LivePollingSession') IS NULL
BEGIN
    CREATE TABLE dbo.LivePollingSession
    (
        SessionId              UNIQUEIDENTIFIER NOT NULL
            CONSTRAINT PK_LivePollingSession PRIMARY KEY,
        DeviceGuid             UNIQUEIDENTIFIER NOT NULL,
        DeviceId               NVARCHAR(128)    NOT NULL,
        GatewayId              NVARCHAR(128)    NOT NULL,
        StartedUtc             DATETIME2        NOT NULL,
        StartedByUserId        NVARCHAR(300)    NULL,
        PollIntervalSeconds    INT              NOT NULL,
        SessionDurationMinutes INT              NOT NULL,
        ExpiresUtc             DATETIME2        NOT NULL,
        ExtensionsUsed         INT              NOT NULL
            CONSTRAINT DF_LivePollingSession_ExtensionsUsed DEFAULT 0,
        LastExtendedUtc        DATETIME2        NULL,
        StoppedUtc             DATETIME2        NULL,
        StopReason             VARCHAR(20)      NULL,
        TickCount              INT              NULL,
        BytesTotal             BIGINT           NULL,
        PurgedUtc              DATETIME2        NULL
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'IX_LivePollingSession_Device_Expires'
                 AND object_id = OBJECT_ID('dbo.LivePollingSession'))
    CREATE NONCLUSTERED INDEX IX_LivePollingSession_Device_Expires
        ON dbo.LivePollingSession (DeviceGuid, ExpiresUtc)
        INCLUDE (StoppedUtc, SessionId);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes
               WHERE name = 'IX_LivePollingSession_Unpurged'
                 AND object_id = OBJECT_ID('dbo.LivePollingSession'))
    CREATE NONCLUSTERED INDEX IX_LivePollingSession_Unpurged
        ON dbo.LivePollingSession (ExpiresUtc)
        INCLUDE (SessionId, StoppedUtc)
        WHERE PurgedUtc IS NULL;
GO
```

- [ ] **Step 2: Write the privilege migration**

Copy the exact shape of `2026-04-29-manage-roles-privilege.sql`.

```sql
-- Migration: LivePolling Privilege
-- Date:      2026-09-21
-- Idempotent.

IF NOT EXISTS (SELECT 1 FROM Privileges WHERE PrivilegeCode = 'LivePolling')
    INSERT INTO Privileges (PrivilegeCode, Description)
    VALUES ('LivePolling', 'Start, extend and stop live telemetry polling sessions on a device.');
GO

IF NOT EXISTS (SELECT 1 FROM PlatformRolePrivileges
               WHERE PlatformRoleCode = 'Admin' AND PrivilegeCode = 'LivePolling')
    INSERT INTO PlatformRolePrivileges (PlatformRoleCode, PrivilegeCode)
    VALUES ('Admin', 'LivePolling');
GO

IF NOT EXISTS (SELECT 1 FROM PlatformRolePrivileges
               WHERE PlatformRoleCode = 'Engineering' AND PrivilegeCode = 'LivePolling')
    INSERT INTO PlatformRolePrivileges (PlatformRoleCode, PrivilegeCode)
    VALUES ('Engineering', 'LivePolling');
GO

IF NOT EXISTS (SELECT 1 FROM PlatformRolePrivileges
               WHERE PlatformRoleCode = 'Service' AND PrivilegeCode = 'LivePolling')
    INSERT INTO PlatformRolePrivileges (PlatformRoleCode, PrivilegeCode)
    VALUES ('Service', 'LivePolling');
GO
```

- [ ] **Step 3: Verify idempotency by reading it back — do not run inserts against dev**

The dev database credential in `CLAUDE.md` is read-only and the project rule is **no INSERT/DELETE probes**. Verify by inspection: every statement is guarded by `IF NOT EXISTS` / `IF OBJECT_ID(...) IS NULL`, so a second run is a no-op. Role codes `Admin`, `Engineering`, `Service` were confirmed present in `dbo.PlatformRoles` on 2026-09-21.

- [ ] **Step 4: Commit**

```bash
git add migrations/2026-09-21-01-live-polling-session.sql migrations/2026-09-21-02-live-polling-privilege.sql
git commit -m "feat: add LivePollingSession table and LivePolling privilege"
```

---

## Task 2: Direct method call that returns status and body

**Files:**
- Modify: `shape-iot-backend/Brilliant.IoT.Application/IoTHub/IIoTHubDeviceService.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.Infrastructure/IoTHub/IoTHubDeviceService.cs`
- Create: `shape-iot-backend/Brilliant.IoT.Application/IoTHub/DirectMethodResult.cs`
- Test: `shape-iot-backend/Brilliant.IoT.UnitTests/IoTHub/DirectMethodResultTests.cs`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `record DirectMethodResult(int Status, string? PayloadJson, bool Reached)` in `Brilliant.IoT.Application.IoTHub`
  - `Task<DirectMethodResult> InvokeDirectMethodWithResultAsync(string deviceId, string methodName, string? payload, string? moduleId, int timeoutSeconds, CancellationToken ct)` on `IIoTHubDeviceService`

The existing `InvokeDirectMethodAsync` returns `bool`, discarding the module's status code and body — so a 409 (already active) cannot be told from a 400 (bad parameters), and `sessionId` cannot be read. Add a new method; leave the old signature untouched for its current callers in `DevicesController`, `GatewaysController`, `EulaConsentController` and `PublicEulaController`.

`Reached` is false when the device could not be contacted at all (not found, offline, timeout), which the controller maps to 503. When `Reached` is true, `Status` is the module's own status code.

- [ ] **Step 1: Write the failing test**

```csharp
using Brilliant.IoT.Application.IoTHub;
using FluentAssertions;
using Xunit;

namespace Brilliant.IoT.UnitTests.IoTHub;

public class DirectMethodResultTests
{
    [Fact]
    public void Unreachable_HasNoStatusAndNoPayload()
    {
        var result = DirectMethodResult.Unreachable();

        result.Reached.Should().BeFalse();
        result.Status.Should().Be(0);
        result.PayloadJson.Should().BeNull();
    }

    [Fact]
    public void FromModule_CarriesStatusAndPayload()
    {
        var result = DirectMethodResult.FromModule(409, "{\"result\":\"rejected\"}");

        result.Reached.Should().BeTrue();
        result.Status.Should().Be(409);
        result.PayloadJson.Should().Be("{\"result\":\"rejected\"}");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~DirectMethodResultTests"`
Expected: FAIL — `DirectMethodResult` does not exist.

- [ ] **Step 3: Create the result type**

```csharp
namespace Brilliant.IoT.Application.IoTHub;

/// <summary>Outcome of a direct method call, preserving the module's own status code and body.</summary>
/// <param name="Status">The module's status code, or 0 when the device was never reached.</param>
/// <param name="PayloadJson">The module's response body, or null.</param>
/// <param name="Reached">False when the device was not found, was offline, or the call timed out.</param>
public sealed record DirectMethodResult(int Status, string? PayloadJson, bool Reached)
{
    /// <summary>The device could not be contacted.</summary>
    public static DirectMethodResult Unreachable() => new(0, null, false);

    /// <summary>The module answered with this status and body.</summary>
    public static DirectMethodResult FromModule(int status, string? payloadJson) =>
        new(status, payloadJson, true);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~DirectMethodResultTests"`
Expected: PASS

- [ ] **Step 5: Add the interface member**

Append to `IIoTHubDeviceService`:

```csharp
    /// <summary>
    /// Invokes a direct method and returns the module's status code and response body.
    /// Never throws; an unreachable device yields <see cref="DirectMethodResult.Unreachable"/>.
    /// </summary>
    Task<DirectMethodResult> InvokeDirectMethodWithResultAsync(
        string deviceId,
        string methodName,
        string? payload = null,
        string? moduleId = null,
        int timeoutSeconds = 30,
        CancellationToken ct = default);
```

- [ ] **Step 6: Implement it**

Add to `IoTHubDeviceService`, mirroring the existing method's exception handling:

```csharp
    public async Task<DirectMethodResult> InvokeDirectMethodWithResultAsync(
        string deviceId,
        string methodName,
        string? payload = null,
        string? moduleId = null,
        int timeoutSeconds = 30,
        CancellationToken ct = default)
    {
        try
        {
            var method = new CloudToDeviceMethod(methodName)
            {
                ResponseTimeout = TimeSpan.FromSeconds(timeoutSeconds)
            };
            if (payload is not null)
                method.SetPayloadJson(payload);

            var result = moduleId is null
                ? await _serviceClient.InvokeDeviceMethodAsync(deviceId, method, ct)
                : await _serviceClient.InvokeDeviceMethodAsync(deviceId, moduleId, method, ct);

            return DirectMethodResult.FromModule(result.Status, result.GetPayloadAsJson());
        }
        catch (DeviceNotFoundException)
        {
            _logger.LogWarning("Device {DeviceId} not found invoking {Method}", deviceId, methodName);
            return DirectMethodResult.Unreachable();
        }
        catch (IotHubException ex)
        {
            _logger.LogWarning(ex, "IoT Hub exception invoking {Method} on {DeviceId}", methodName, deviceId);
            return DirectMethodResult.Unreachable();
        }
        catch (TimeoutException ex)
        {
            _logger.LogWarning(ex, "Timeout invoking {Method} on {DeviceId}", methodName, deviceId);
            return DirectMethodResult.Unreachable();
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Unexpected error invoking {Method} on {DeviceId}", methodName, deviceId);
            return DirectMethodResult.Unreachable();
        }
    }
```

- [ ] **Step 7: Build and run the full unit suite**

Run: `dotnet build && dotnet test Brilliant.IoT.UnitTests`
Expected: build succeeds, all tests pass — existing callers are untouched.

- [ ] **Step 8: Commit**

```bash
git add Brilliant.IoT.Application/IoTHub Brilliant.IoT.Infrastructure/IoTHub Brilliant.IoT.UnitTests/IoTHub
git commit -m "feat: direct method call returning module status and body"
```

---

## Task 3: Live polling options and parameter clamping

**Files:**
- Create: `shape-iot-backend/Brilliant.IoT.Application/LivePolling/LivePollingOptions.cs`
- Create: `shape-iot-backend/Brilliant.IoT.Application/LivePolling/LivePollingParameters.cs`
- Test: `shape-iot-backend/Brilliant.IoT.UnitTests/LivePolling/LivePollingParametersTests.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.API/Program.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.API/appsettings.Development.json`

**Interfaces:**
- Consumes: nothing
- Produces:
  - `LivePollingOptions` with `SectionName = "LivePolling"` and properties `PollIntervalSeconds`, `SessionDurationMinutes`, `ExtensionMinutes`, `MaxConcurrentSessions`, `DefaultSeriesMetrics` (`string[]`), `ModuleId`, `DirectMethodTimeoutSeconds`, `CleanupSchedule`
  - `static ParameterResolution LivePollingParameters.Resolve(int? requestedInterval, int? requestedDuration, LivePollingOptions options)`
  - `record ParameterResolution(int IntervalSeconds, int DurationMinutes, IReadOnlyList<string> Errors)`

The module rejects an out-of-range value that was *supplied*, but silently defaults a *missing* one. The backend mirrors that: a caller-supplied value outside the range is an error we return ourselves rather than forwarding; a configured value outside the range is clamped so a bad config can never produce an opaque module rejection.

- [ ] **Step 1: Write the failing tests**

```csharp
using Brilliant.IoT.Application.LivePolling;
using FluentAssertions;
using Xunit;

namespace Brilliant.IoT.UnitTests.LivePolling;

public class LivePollingParametersTests
{
    private static LivePollingOptions Options() => new()
    {
        PollIntervalSeconds = 1,
        SessionDurationMinutes = 15,
        ExtensionMinutes = 10,
        MaxConcurrentSessions = 10,
        ModuleId = "PollerModule",
        DirectMethodTimeoutSeconds = 30
    };

    [Fact]
    public void BothOmitted_UsesConfiguredDefaults()
    {
        var r = LivePollingParameters.Resolve(null, null, Options());

        r.Errors.Should().BeEmpty();
        r.IntervalSeconds.Should().Be(1);
        r.DurationMinutes.Should().Be(15);
    }

    [Fact]
    public void SuppliedValuesInRange_AreUsed()
    {
        var r = LivePollingParameters.Resolve(5, 30, Options());

        r.Errors.Should().BeEmpty();
        r.IntervalSeconds.Should().Be(5);
        r.DurationMinutes.Should().Be(30);
    }

    [Fact]
    public void SuppliedDurationOutOfRange_IsRejectedWithBothProblemsListed()
    {
        var r = LivePollingParameters.Resolve(0, 45, Options());

        r.Errors.Should().HaveCount(2);
        r.Errors.Should().Contain(e => e.Contains("sessionDurationMinutes") && e.Contains("10-30"));
        r.Errors.Should().Contain(e => e.Contains("pollIntervalSeconds") && e.Contains("1-60"));
    }

    [Fact]
    public void ConfiguredValueOutOfRange_IsClampedNotRejected()
    {
        var options = Options();
        options.SessionDurationMinutes = 90;
        options.PollIntervalSeconds = 0;

        var r = LivePollingParameters.Resolve(null, null, options);

        r.Errors.Should().BeEmpty();
        r.DurationMinutes.Should().Be(30);
        r.IntervalSeconds.Should().Be(1);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePollingParametersTests"`
Expected: FAIL — types do not exist.

- [ ] **Step 3: Write the options type**

```csharp
namespace Brilliant.IoT.Application.LivePolling;

/// <summary>Configuration for live polling sessions. Every value is runtime-changeable.</summary>
public sealed class LivePollingOptions
{
    public const string SectionName = "LivePolling";

    /// <summary>Default tick interval. Clamped into the module's 1-60 second range.</summary>
    public int PollIntervalSeconds { get; set; } = 1;

    /// <summary>Default session length. Clamped into the module's 10-30 minute range.</summary>
    public int SessionDurationMinutes { get; set; } = 10;

    /// <summary>Minutes requested per extend call. The module caps TOTAL duration at 30 minutes
    /// and rejects anything beyond it, so there is deliberately no backend extension counter.</summary>
    public int ExtensionMinutes { get; set; } = 10;

    /// <summary>Fleet-wide ceiling on simultaneous sessions. Bounds wireless cost.</summary>
    public int MaxConcurrentSessions { get; set; } = 10;

    /// <summary>Metric names the series endpoint returns when none is requested.</summary>
    public string[] DefaultSeriesMetrics { get; set; } = [];

    /// <summary>IoT Hub module id hosting the fast polling direct methods.</summary>
    public string ModuleId { get; set; } = string.Empty;

    /// <summary>Direct method response timeout.</summary>
    public int DirectMethodTimeoutSeconds { get; set; } = 30;

    /// <summary>NCRONTAB schedule for the cleanup timer (read by the function app).</summary>
    public string CleanupSchedule { get; set; } = "0 */2 * * * *";
}
```

- [ ] **Step 4: Write the resolver**

```csharp
namespace Brilliant.IoT.Application.LivePolling;

/// <summary>Resolved session parameters, or the reasons the request was rejected.</summary>
public sealed record ParameterResolution(
    int IntervalSeconds,
    int DurationMinutes,
    IReadOnlyList<string> Errors);

/// <summary>
/// Resolves requested session parameters against configuration and the module's fixed ranges.
/// A supplied out-of-range value is an error; a configured out-of-range value is clamped.
/// </summary>
public static class LivePollingParameters
{
    public const int MinIntervalSeconds = 1;
    public const int MaxIntervalSeconds = 60;
    public const int MinDurationMinutes = 10;
    public const int MaxDurationMinutes = 30;

    public static ParameterResolution Resolve(
        int? requestedIntervalSeconds,
        int? requestedDurationMinutes,
        LivePollingOptions options)
    {
        var errors = new List<string>();

        int interval;
        if (requestedIntervalSeconds is { } ri)
        {
            if (ri < MinIntervalSeconds || ri > MaxIntervalSeconds)
            {
                errors.Add($"'pollIntervalSeconds'={ri} is outside the allowed range ({MinIntervalSeconds}-{MaxIntervalSeconds}).");
                interval = MinIntervalSeconds;
            }
            else interval = ri;
        }
        else interval = Math.Clamp(options.PollIntervalSeconds, MinIntervalSeconds, MaxIntervalSeconds);

        int duration;
        if (requestedDurationMinutes is { } rd)
        {
            if (rd < MinDurationMinutes || rd > MaxDurationMinutes)
            {
                errors.Add($"'sessionDurationMinutes'={rd} is outside the allowed range ({MinDurationMinutes}-{MaxDurationMinutes}).");
                duration = MinDurationMinutes;
            }
            else duration = rd;
        }
        else duration = Math.Clamp(options.SessionDurationMinutes, MinDurationMinutes, MaxDurationMinutes);

        return new ParameterResolution(interval, duration, errors);
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePollingParametersTests"`
Expected: PASS (4 tests)

- [ ] **Step 6: Register the options and add development config**

In `Program.cs`, beside the other infrastructure registrations (near `builder.Services.AddSingleton<IDbConnectionFactory, DbConnectionFactory>();`):

```csharp
builder.Services.Configure<LivePollingOptions>(
    builder.Configuration.GetSection(LivePollingOptions.SectionName));
```

Add the `using Brilliant.IoT.Application.LivePolling;` import. Then add to `appsettings.Development.json`:

```json
  "LivePolling": {
    "PollIntervalSeconds": 1,
    "SessionDurationMinutes": 10,
    "ExtensionMinutes": 10,
    "MaxConcurrentSessions": 10,
    "DefaultSeriesMetrics": [
      "OperatingPressurePSI",
      "StrokeRateLeft",
      "StrokeRateRight",
      "HydraulicOilTemperatureC"
    ],
    "ModuleId": "PollerModule",
    "DirectMethodTimeoutSeconds": 30,
    "CleanupSchedule": "0 */2 * * * *"
  }
```

`ModuleId` is a placeholder pending open item 4 in the spec — the module hosting `FastPollingService` must be confirmed against IoT Hub before the dev-gateway test in Task 14. `DefaultSeriesMetrics` is pending the product decision on which four; these four are valid `ParameterRegistry` names and can change without a release.

- [ ] **Step 7: Build and commit**

```bash
dotnet build
git add Brilliant.IoT.Application/LivePolling Brilliant.IoT.UnitTests/LivePolling Brilliant.IoT.API/Program.cs Brilliant.IoT.API/appsettings.Development.json
git commit -m "feat: live polling options and parameter clamping"
```

---

## Task 4: Session repository

**Files:**
- Create: `shape-iot-backend/Brilliant.IoT.Domain/Entities/LivePollingSession.cs`
- Create: `shape-iot-backend/Brilliant.IoT.Application/Repositories/ILivePollingSessionRepository.cs`
- Create: `shape-iot-backend/Brilliant.IoT.Infrastructure/Repositories/LivePollingSessionRepository.cs`
- Test: `shape-iot-backend/Brilliant.IoT.IntegrationTests/Repositories/LivePollingSessionRepositoryTests.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.API/Program.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.IntegrationTests/test-schema.sql`

**Interfaces:**
- Consumes: `IDbConnectionFactory`
- Produces `ILivePollingSessionRepository`:
  - `Task<LivePollingSession?> GetActiveForDeviceAsync(Guid deviceGuid, DateTime nowUtc, CancellationToken ct)`
  - `Task<LivePollingSession?> GetBySessionIdAsync(Guid sessionId, CancellationToken ct)`
  - `Task<int> CountActiveAsync(DateTime nowUtc, CancellationToken ct)`
  - `Task InsertAsync(LivePollingSession session, CancellationToken ct)`
  - `Task ExtendAsync(Guid sessionId, DateTime newExpiresUtc, DateTime extendedUtc, CancellationToken ct)`
  - `Task StopAsync(Guid sessionId, DateTime stoppedUtc, string stopReason, CancellationToken ct)`

"Active" means `StoppedUtc IS NULL AND ExpiresUtc > @NowUtc` — expiry is evaluated on read, so a session that ended without telling anyone never blocks a new start.

- [ ] **Step 1: Write the entity**

```csharp
namespace Brilliant.IoT.Domain.Entities;

/// <summary>One live polling session, from start through to storage purge.</summary>
public sealed class LivePollingSession
{
    public Guid SessionId { get; set; }
    public Guid DeviceGuid { get; set; }
    public string DeviceId { get; set; } = string.Empty;
    public string GatewayId { get; set; } = string.Empty;
    public DateTime StartedUtc { get; set; }
    public string? StartedByUserId { get; set; }
    public int PollIntervalSeconds { get; set; }
    public int SessionDurationMinutes { get; set; }
    public DateTime ExpiresUtc { get; set; }
    public int ExtensionsUsed { get; set; }
    public DateTime? LastExtendedUtc { get; set; }
    public DateTime? StoppedUtc { get; set; }
    public string? StopReason { get; set; }
    public int? TickCount { get; set; }
    public long? BytesTotal { get; set; }
    public DateTime? PurgedUtc { get; set; }
}
```

- [ ] **Step 2: Write the interface**

```csharp
using Brilliant.IoT.Domain.Entities;

namespace Brilliant.IoT.Application.Repositories;

/// <summary>Reads and writes the live polling session record that gates concurrency.</summary>
public interface ILivePollingSessionRepository
{
    /// <summary>The device's active session, or null. Active means not stopped and not yet expired.</summary>
    Task<LivePollingSession?> GetActiveForDeviceAsync(Guid deviceGuid, DateTime nowUtc, CancellationToken ct = default);

    /// <summary>One session by id, whatever its state.</summary>
    Task<LivePollingSession?> GetBySessionIdAsync(Guid sessionId, CancellationToken ct = default);

    /// <summary>How many sessions are active fleet-wide.</summary>
    Task<int> CountActiveAsync(DateTime nowUtc, CancellationToken ct = default);

    /// <summary>Records a newly started session.</summary>
    Task InsertAsync(LivePollingSession session, CancellationToken ct = default);

    /// <summary>Pushes out the expiry and increments the extension count.</summary>
    Task ExtendAsync(Guid sessionId, DateTime newExpiresUtc, DateTime extendedUtc, CancellationToken ct = default);

    /// <summary>Closes the session with a reason.</summary>
    Task StopAsync(Guid sessionId, DateTime stoppedUtc, string stopReason, CancellationToken ct = default);
}
```

- [ ] **Step 3: Add the table to the integration test schema**

Append to `Brilliant.IoT.IntegrationTests/test-schema.sql` the same `CREATE TABLE dbo.LivePollingSession` body from Task 1 Step 1 (without the `IF OBJECT_ID` guard — the test schema is built fresh).

- [ ] **Step 4: Write the failing integration test**

```csharp
using Brilliant.IoT.Application.Repositories;
using Brilliant.IoT.Domain.Entities;
using FluentAssertions;
using Xunit;

namespace Brilliant.IoT.IntegrationTests.Repositories;

public class LivePollingSessionRepositoryTests : IClassFixture<IntegrationTestFactory>
{
    private readonly ILivePollingSessionRepository _repo;

    public LivePollingSessionRepositoryTests(IntegrationTestFactory factory)
    {
        _repo = factory.Services.GetRequiredService<ILivePollingSessionRepository>();
    }

    private static LivePollingSession NewSession(Guid deviceGuid, DateTime now, int minutes = 10) => new()
    {
        SessionId = Guid.NewGuid(),
        DeviceGuid = deviceGuid,
        DeviceId = "PUMP-TEST",
        GatewayId = "GW-TEST",
        StartedUtc = now,
        StartedByUserId = "test-user",
        PollIntervalSeconds = 1,
        SessionDurationMinutes = minutes,
        ExpiresUtc = now.AddMinutes(minutes),
        ExtensionsUsed = 0
    };

    [Fact]
    public async Task InsertedSession_IsReturnedAsActive()
    {
        var now = DateTime.UtcNow;
        var device = Guid.NewGuid();
        var session = NewSession(device, now);

        await _repo.InsertAsync(session);
        var active = await _repo.GetActiveForDeviceAsync(device, now);

        active.Should().NotBeNull();
        active!.SessionId.Should().Be(session.SessionId);
        active.PollIntervalSeconds.Should().Be(1);
    }

    [Fact]
    public async Task ExpiredSession_IsNotActive()
    {
        var now = DateTime.UtcNow;
        var device = Guid.NewGuid();
        await _repo.InsertAsync(NewSession(device, now.AddMinutes(-30)));

        var active = await _repo.GetActiveForDeviceAsync(device, now);

        active.Should().BeNull();
    }

    [Fact]
    public async Task StoppedSession_IsNotActive()
    {
        var now = DateTime.UtcNow;
        var device = Guid.NewGuid();
        var session = NewSession(device, now);
        await _repo.InsertAsync(session);

        await _repo.StopAsync(session.SessionId, now.AddMinutes(2), "ManualStop");
        var active = await _repo.GetActiveForDeviceAsync(device, now.AddMinutes(3));

        active.Should().BeNull();
    }

    [Fact]
    public async Task Extend_PushesExpiryAndCountsTheExtension()
    {
        var now = DateTime.UtcNow;
        var device = Guid.NewGuid();
        var session = NewSession(device, now);
        await _repo.InsertAsync(session);

        var newExpiry = session.ExpiresUtc.AddMinutes(10);
        await _repo.ExtendAsync(session.SessionId, newExpiry, now.AddMinutes(1));

        var reloaded = await _repo.GetBySessionIdAsync(session.SessionId);
        reloaded!.ExtensionsUsed.Should().Be(1);
        reloaded.ExpiresUtc.Should().BeCloseTo(newExpiry, TimeSpan.FromSeconds(1));
        reloaded.LastExtendedUtc.Should().NotBeNull();
    }

    [Fact]
    public async Task CountActive_CountsOnlyLiveSessions()
    {
        var now = DateTime.UtcNow;
        await _repo.InsertAsync(NewSession(Guid.NewGuid(), now));
        await _repo.InsertAsync(NewSession(Guid.NewGuid(), now.AddMinutes(-60)));

        var count = await _repo.CountActiveAsync(now);

        count.Should().BeGreaterThanOrEqualTo(1);
    }
}
```

- [ ] **Step 5: Run to verify it fails**

Run: `dotnet test Brilliant.IoT.IntegrationTests --filter "FullyQualifiedName~LivePollingSessionRepositoryTests"`
Expected: FAIL — `ILivePollingSessionRepository` is not registered.

- [ ] **Step 6: Implement the repository**

Follow `GatewayRepository`: `IDbConnectionFactory`, cast to `SqlConnection`, raw `SqlCommand`, named parameters.

```csharp
using Brilliant.IoT.Application.Abstractions;
using Brilliant.IoT.Application.Repositories;
using Brilliant.IoT.Domain.Entities;
using Microsoft.Data.SqlClient;

namespace Brilliant.IoT.Infrastructure.Repositories;

public class LivePollingSessionRepository : ILivePollingSessionRepository
{
    private readonly IDbConnectionFactory _connectionFactory;

    public LivePollingSessionRepository(IDbConnectionFactory connectionFactory)
        => _connectionFactory = connectionFactory;

    private const string SelectColumns = @"
        SessionId, DeviceGuid, DeviceId, GatewayId, StartedUtc, StartedByUserId,
        PollIntervalSeconds, SessionDurationMinutes, ExpiresUtc, ExtensionsUsed,
        LastExtendedUtc, StoppedUtc, StopReason, TickCount, BytesTotal, PurgedUtc";

    private static LivePollingSession Map(SqlDataReader r) => new()
    {
        SessionId              = r.GetGuid(0),
        DeviceGuid             = r.GetGuid(1),
        DeviceId               = r.GetString(2),
        GatewayId              = r.GetString(3),
        StartedUtc             = r.GetDateTime(4),
        StartedByUserId        = r.IsDBNull(5) ? null : r.GetString(5),
        PollIntervalSeconds    = r.GetInt32(6),
        SessionDurationMinutes = r.GetInt32(7),
        ExpiresUtc             = r.GetDateTime(8),
        ExtensionsUsed         = r.GetInt32(9),
        LastExtendedUtc        = r.IsDBNull(10) ? null : r.GetDateTime(10),
        StoppedUtc             = r.IsDBNull(11) ? null : r.GetDateTime(11),
        StopReason             = r.IsDBNull(12) ? null : r.GetString(12),
        TickCount              = r.IsDBNull(13) ? null : r.GetInt32(13),
        BytesTotal             = r.IsDBNull(14) ? null : r.GetInt64(14),
        PurgedUtc              = r.IsDBNull(15) ? null : r.GetDateTime(15)
    };

    private SqlConnection Open()
    {
        var connection = _connectionFactory.CreateConnection();
        if (connection is not SqlConnection sql)
            throw new InvalidOperationException("Requires SQL Connection");
        return sql;
    }

    /// <inheritdoc />
    public async Task<LivePollingSession?> GetActiveForDeviceAsync(Guid deviceGuid, DateTime nowUtc, CancellationToken ct = default)
    {
        using var conn = Open();
        await conn.OpenAsync(ct);

        var sql = $@"SELECT TOP 1 {SelectColumns} FROM dbo.LivePollingSession
                     WHERE DeviceGuid = @DeviceGuid AND StoppedUtc IS NULL AND ExpiresUtc > @NowUtc
                     ORDER BY StartedUtc DESC;";
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@DeviceGuid", deviceGuid));
        cmd.Parameters.Add(new SqlParameter("@NowUtc", nowUtc));

        using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Map(reader) : null;
    }

    /// <inheritdoc />
    public async Task<LivePollingSession?> GetBySessionIdAsync(Guid sessionId, CancellationToken ct = default)
    {
        using var conn = Open();
        await conn.OpenAsync(ct);

        var sql = $"SELECT {SelectColumns} FROM dbo.LivePollingSession WHERE SessionId = @SessionId;";
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@SessionId", sessionId));

        using var reader = await cmd.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? Map(reader) : null;
    }

    /// <inheritdoc />
    public async Task<int> CountActiveAsync(DateTime nowUtc, CancellationToken ct = default)
    {
        using var conn = Open();
        await conn.OpenAsync(ct);

        const string sql = @"SELECT COUNT(*) FROM dbo.LivePollingSession
                             WHERE StoppedUtc IS NULL AND ExpiresUtc > @NowUtc;";
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@NowUtc", nowUtc));

        return (int)(await cmd.ExecuteScalarAsync(ct) ?? 0);
    }

    /// <inheritdoc />
    public async Task InsertAsync(LivePollingSession s, CancellationToken ct = default)
    {
        using var conn = Open();
        await conn.OpenAsync(ct);

        const string sql = @"
            INSERT INTO dbo.LivePollingSession
                (SessionId, DeviceGuid, DeviceId, GatewayId, StartedUtc, StartedByUserId,
                 PollIntervalSeconds, SessionDurationMinutes, ExpiresUtc, ExtensionsUsed)
            VALUES
                (@SessionId, @DeviceGuid, @DeviceId, @GatewayId, @StartedUtc, @StartedByUserId,
                 @PollIntervalSeconds, @SessionDurationMinutes, @ExpiresUtc, 0);";
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@SessionId", s.SessionId));
        cmd.Parameters.Add(new SqlParameter("@DeviceGuid", s.DeviceGuid));
        cmd.Parameters.Add(new SqlParameter("@DeviceId", s.DeviceId));
        cmd.Parameters.Add(new SqlParameter("@GatewayId", s.GatewayId));
        cmd.Parameters.Add(new SqlParameter("@StartedUtc", s.StartedUtc));
        cmd.Parameters.Add(new SqlParameter("@StartedByUserId", (object?)s.StartedByUserId ?? DBNull.Value));
        cmd.Parameters.Add(new SqlParameter("@PollIntervalSeconds", s.PollIntervalSeconds));
        cmd.Parameters.Add(new SqlParameter("@SessionDurationMinutes", s.SessionDurationMinutes));
        cmd.Parameters.Add(new SqlParameter("@ExpiresUtc", s.ExpiresUtc));

        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task ExtendAsync(Guid sessionId, DateTime newExpiresUtc, DateTime extendedUtc, CancellationToken ct = default)
    {
        using var conn = Open();
        await conn.OpenAsync(ct);

        const string sql = @"UPDATE dbo.LivePollingSession
                             SET ExpiresUtc = @ExpiresUtc,
                                 ExtensionsUsed = ExtensionsUsed + 1,
                                 LastExtendedUtc = @ExtendedUtc
                             WHERE SessionId = @SessionId;";
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@ExpiresUtc", newExpiresUtc));
        cmd.Parameters.Add(new SqlParameter("@ExtendedUtc", extendedUtc));
        cmd.Parameters.Add(new SqlParameter("@SessionId", sessionId));

        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <inheritdoc />
    public async Task StopAsync(Guid sessionId, DateTime stoppedUtc, string stopReason, CancellationToken ct = default)
    {
        using var conn = Open();
        await conn.OpenAsync(ct);

        const string sql = @"UPDATE dbo.LivePollingSession
                             SET StoppedUtc = @StoppedUtc, StopReason = @StopReason
                             WHERE SessionId = @SessionId;";
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@StoppedUtc", stoppedUtc));
        cmd.Parameters.Add(new SqlParameter("@StopReason", stopReason));
        cmd.Parameters.Add(new SqlParameter("@SessionId", sessionId));

        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

- [ ] **Step 7: Register it**

In `Program.cs`, beside the other scoped repositories:

```csharp
builder.Services.AddScoped<ILivePollingSessionRepository, LivePollingSessionRepository>();
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `dotnet test Brilliant.IoT.IntegrationTests --filter "FullyQualifiedName~LivePollingSessionRepositoryTests"`
Expected: PASS (5 tests)

- [ ] **Step 9: Commit**

```bash
git add Brilliant.IoT.Domain/Entities/LivePollingSession.cs Brilliant.IoT.Application/Repositories/ILivePollingSessionRepository.cs Brilliant.IoT.Infrastructure/Repositories/LivePollingSessionRepository.cs Brilliant.IoT.IntegrationTests Brilliant.IoT.API/Program.cs
git commit -m "feat: live polling session repository"
```

---

## Task 5: Contracts — DTOs and error codes

**Files:**
- Create: `shape-iot-backend/Brilliant.IoT.Application/DTOs/LivePolling/LivePollingDtos.cs`
- Create: `shape-iot-backend/Brilliant.IoT.Application/LivePolling/LivePollingErrorCodes.cs`
- Test: `shape-iot-backend/Brilliant.IoT.UnitTests/LivePolling/ModuleResponseParserTests.cs`
- Create: `shape-iot-backend/Brilliant.IoT.Application/LivePolling/ModuleResponseParser.cs`

**Interfaces:**
- Consumes: `DirectMethodResult` (Task 2)
- Produces:
  - `StartLivePollingRequest { int? PollIntervalSeconds; int? SessionDurationMinutes; }`
  - `LivePollingSessionDto`, `LivePollingStatusDto`, `LivePollingSeriesDto`, `LivePollingSeriesMetricDto`
  - `static class LivePollingErrorCodes` with the seven codes
  - `static ModuleEnableResponse ModuleResponseParser.ParseEnable(string? payloadJson)` returning `record ModuleEnableResponse(Guid? SessionId, int? DurationMinutes, int? IntervalSeconds, IReadOnlyList<string> Errors)`

The module's `FastPollingEnable` 200 body carries the `sessionId` we must record, and its 400 body carries an `errors` array we echo back rather than inventing our own wording. Parsing is separated from the controller so it can be tested against the exact bodies in the gateway spec.

- [ ] **Step 1: Write the failing parser tests**

```csharp
using Brilliant.IoT.Application.LivePolling;
using FluentAssertions;
using Xunit;

namespace Brilliant.IoT.UnitTests.LivePolling;

public class ModuleResponseParserTests
{
    [Fact]
    public void ParseEnable_AcceptedBody_ReadsSessionIdAndResolvedValues()
    {
        const string json = """
        {"result":"accepted","durationMinutes":15,"intervalSeconds":5,
         "sessionId":"cc27db6b-508b-488e-a816-faae684491d8","defaultsApplied":[]}
        """;

        var parsed = ModuleResponseParser.ParseEnable(json);

        parsed.SessionId.Should().Be(Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8"));
        parsed.DurationMinutes.Should().Be(15);
        parsed.IntervalSeconds.Should().Be(5);
        parsed.Errors.Should().BeEmpty();
    }

    [Fact]
    public void ParseEnable_RejectedBody_ReadsEveryError()
    {
        const string json = """
        {"result":"rejected","reason":"Invalid request.",
         "errors":["'sessionDurationMinutes'=45 is outside the allowed range (10-30)."]}
        """;

        var parsed = ModuleResponseParser.ParseEnable(json);

        parsed.SessionId.Should().BeNull();
        parsed.Errors.Should().ContainSingle()
              .Which.Should().Contain("sessionDurationMinutes");
    }

    [Fact]
    public void ParseEnable_UnparseableBody_YieldsEmptyResultNotAnException()
    {
        var parsed = ModuleResponseParser.ParseEnable("not json at all");

        parsed.SessionId.Should().BeNull();
        parsed.Errors.Should().BeEmpty();
    }

    [Fact]
    public void ParseEnable_NullBody_YieldsEmptyResult()
    {
        var parsed = ModuleResponseParser.ParseEnable(null);

        parsed.SessionId.Should().BeNull();
        parsed.DurationMinutes.Should().BeNull();
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~ModuleResponseParserTests"`
Expected: FAIL — `ModuleResponseParser` does not exist.

- [ ] **Step 3: Write the error codes**

```csharp
namespace Brilliant.IoT.Application.LivePolling;

/// <summary>Machine-readable failure codes returned by the live polling endpoints.</summary>
public static class LivePollingErrorCodes
{
    public const string AlreadyActive       = "LIVE_SESSION_ALREADY_ACTIVE";
    public const string CapacityReached     = "LIVE_SESSION_CAPACITY_REACHED";
    public const string NotActive           = "LIVE_SESSION_NOT_ACTIVE";
    public const string ExtensionLimit      = "LIVE_SESSION_EXTENSION_LIMIT_REACHED";  // module's 30-min total cap, HTTP 400
    public const string InvalidParameters   = "LIVE_SESSION_INVALID_PARAMETERS";
    public const string DeviceNotFound      = "DEVICE_NOT_FOUND";
    public const string GatewayNotPaired    = "GATEWAY_NOT_PAIRED";
    public const string GatewayUnreachable  = "GATEWAY_UNREACHABLE";
}
```

- [ ] **Step 4: Write the parser**

```csharp
using System.Text.Json;

namespace Brilliant.IoT.Application.LivePolling;

/// <summary>What the module said in response to FastPollingEnable.</summary>
public sealed record ModuleEnableResponse(
    Guid? SessionId,
    int? DurationMinutes,
    int? IntervalSeconds,
    IReadOnlyList<string> Errors);

/// <summary>
/// Reads the gateway module's direct method response bodies. A body that cannot be
/// interpreted yields an empty result rather than throwing — the caller decides what
/// an unparseable body means for the status code it already has.
/// </summary>
public static class ModuleResponseParser
{
    public static ModuleEnableResponse ParseEnable(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
            return new ModuleEnableResponse(null, null, null, Array.Empty<string>());

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            var root = doc.RootElement;

            Guid? sessionId = null;
            if (root.TryGetProperty("sessionId", out var sid) &&
                sid.ValueKind == JsonValueKind.String &&
                Guid.TryParse(sid.GetString(), out var parsedId))
                sessionId = parsedId;

            int? duration = root.TryGetProperty("durationMinutes", out var d) && d.TryGetInt32(out var dv) ? dv : null;
            int? interval = root.TryGetProperty("intervalSeconds", out var i) && i.TryGetInt32(out var iv) ? iv : null;

            var errors = new List<string>();
            if (root.TryGetProperty("errors", out var errs) && errs.ValueKind == JsonValueKind.Array)
                foreach (var e in errs.EnumerateArray())
                    if (e.ValueKind == JsonValueKind.String)
                        errors.Add(e.GetString()!);

            return new ModuleEnableResponse(sessionId, duration, interval, errors);
        }
        catch (JsonException)
        {
            return new ModuleEnableResponse(null, null, null, Array.Empty<string>());
        }
    }
}
```

- [ ] **Step 5: Write the DTOs**

```csharp
namespace Brilliant.IoT.Application.DTOs.LivePolling;

/// <summary>Body of a start request. Both fields are optional; omitted values fall back to configuration.</summary>
public sealed class StartLivePollingRequest
{
    /// <summary>Tick interval in seconds. Allowed 1-60. Omit to use the configured default.</summary>
    public int? PollIntervalSeconds { get; set; }

    /// <summary>Session length in minutes. Allowed 10-30. Omit to use the configured default.</summary>
    public int? SessionDurationMinutes { get; set; }
}

/// <summary>A started or extended session.</summary>
public sealed class LivePollingSessionDto
{
    public Guid SessionId { get; set; }
    public string DeviceId { get; set; } = string.Empty;
    public int PollIntervalSeconds { get; set; }
    public int SessionDurationMinutes { get; set; }
    public DateTime StartedUtc { get; set; }
    public DateTime ExpiresUtc { get; set; }

    /// <summary>Total planned duration, measured from StartedUtc. Grows when the session is extended.</summary>
    public int TotalDurationMinutes { get; set; }

    /// <summary>False once the module's 30-minute total-duration cap leaves no room to extend.</summary>
    public bool CanExtend { get; set; }
}

/// <summary>Current state of a device's live session, including the latest values.</summary>
public sealed class LivePollingStatusDto
{
    public bool Active { get; set; }
    public Guid? SessionId { get; set; }
    public int? PollIntervalSeconds { get; set; }
    public DateTime? ExpiresUtc { get; set; }
    public int? SecondsRemaining { get; set; }
    public int? TotalDurationMinutes { get; set; }
    public bool? CanExtend { get; set; }
    public string? DeviceStatus { get; set; }
    public IReadOnlyDictionary<string, object?>? Snapshot { get; set; }
    public DateTime? SnapshotTimestampUtc { get; set; }
    public int? TickCount { get; set; }
    public long? BytesTotal { get; set; }
}

/// <summary>One metric's values across the session.</summary>
public sealed class LivePollingSeriesMetricDto
{
    public string Metric { get; set; } = string.Empty;
    public IReadOnlyList<object?> V { get; set; } = Array.Empty<object?>();
}

/// <summary>Series response. Timestamps and device status are shared across every metric.</summary>
public sealed class LivePollingSeriesDto
{
    public Guid SessionId { get; set; }
    public int PollIntervalSeconds { get; set; }
    public DateTime StartedUtc { get; set; }
    public DateTime? LastTickUtc { get; set; }
    public int TickCount { get; set; }
    public IReadOnlyList<DateTime> T { get; set; } = Array.Empty<DateTime>();
    public IReadOnlyList<string> Status { get; set; } = Array.Empty<string>();
    public IReadOnlyList<LivePollingSeriesMetricDto> Series { get; set; } = Array.Empty<LivePollingSeriesMetricDto>();
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~ModuleResponseParserTests"`
Expected: PASS (4 tests)

- [ ] **Step 7: Commit**

```bash
git add Brilliant.IoT.Application/DTOs/LivePolling Brilliant.IoT.Application/LivePolling Brilliant.IoT.UnitTests/LivePolling
git commit -m "feat: live polling contracts and module response parser"
```

---

## Task 6: Start endpoint

**Files:**
- Create: `shape-iot-backend/Brilliant.IoT.API/Controllers/LivePollingController.cs`
- Test: `shape-iot-backend/Brilliant.IoT.UnitTests/Controllers/LivePollingControllerTests.cs`

**Interfaces:**
- Consumes: `ILivePollingSessionRepository` (T4), `IIoTHubDeviceService.InvokeDirectMethodWithResultAsync` (T2), `LivePollingOptions` + `LivePollingParameters` (T3), `ModuleResponseParser` + `LivePollingErrorCodes` + DTOs (T5), `IDeviceRepository`, `ICurrentUserService`, `TimeProvider`
- Produces: `LivePollingController.StartAsync(string deviceId, StartLivePollingRequest? request, CancellationToken ct)`

`TimeProvider` is injected so expiry is testable without waiting — the acceptance criterion requires auto-shutoff be exercisable in a test. Register `TimeProvider.System` in `Program.cs`.

Device resolution uses the existing `IDeviceRepository`. Confirm the exact member that maps a device id to `DeviceGuid` + paired `GatewayId` before writing this task; if none exists, add `Task<DeviceGatewayRef?> GetGatewayRefAsync(string deviceId, CancellationToken ct)` returning `record DeviceGatewayRef(Guid DeviceGuid, string GatewayId)` to `IDeviceRepository` and implement it against `DeviceGatewayPairing` in the same commit.

- [ ] **Step 1: Write the failing tests**

```csharp
using Brilliant.IoT.API.Controllers;
using Brilliant.IoT.Application.Abstractions;
using Brilliant.IoT.Application.Common.Models;
using Brilliant.IoT.Application.DTOs.LivePolling;
using Brilliant.IoT.Application.IoTHub;
using Brilliant.IoT.Application.LivePolling;
using Brilliant.IoT.Application.Repositories;
using Brilliant.IoT.Domain.Entities;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using Xunit;

namespace Brilliant.IoT.UnitTests.Controllers;

public class LivePollingControllerTests
{
    private readonly Mock<ILivePollingSessionRepository> _sessions = new(MockBehavior.Strict);
    private readonly Mock<IIoTHubDeviceService> _iotHub = new(MockBehavior.Strict);
    private readonly Mock<IDeviceRepository> _devices = new(MockBehavior.Strict);
    private readonly Mock<ICurrentUserService> _user = new(MockBehavior.Strict);
    private readonly FakeTimeProvider _time = new(new DateTime(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc));
    private readonly LivePollingController _controller;

    private static readonly Guid DeviceGuid = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private const string DeviceId = "PUMP-00417";
    private const string GatewayId = "GW-0042";

    public LivePollingControllerTests()
    {
        var options = Options.Create(new LivePollingOptions
        {
            PollIntervalSeconds = 1,
            SessionDurationMinutes = 15,
            ExtensionMinutes = 10,
                MaxConcurrentSessions = 10,
            ModuleId = "PollerModule",
            DirectMethodTimeoutSeconds = 30
        });

        _user.SetupGet(u => u.PrincipalId).Returns(Guid.Parse("22222222-2222-2222-2222-222222222222"));

        _controller = new LivePollingController(
            _sessions.Object, _iotHub.Object, _devices.Object, _user.Object,
            options, _time, new Mock<ILogger<LivePollingController>>().Object);
    }

    private void GivenDeviceResolves() =>
        _devices.Setup(d => d.GetGatewayRefAsync(DeviceId, It.IsAny<CancellationToken>()))
                .ReturnsAsync(new DeviceGatewayRef(DeviceGuid, GatewayId));

    private void GivenNoActiveSession() =>
        _sessions.Setup(s => s.GetActiveForDeviceAsync(DeviceGuid, _time.GetUtcNow().UtcDateTime, It.IsAny<CancellationToken>()))
                 .ReturnsAsync((LivePollingSession?)null);

    [Fact]
    public async Task Start_ModuleAccepts_Returns200AndRecordsSession()
    {
        GivenDeviceResolves();
        GivenNoActiveSession();
        _sessions.Setup(s => s.CountActiveAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _sessions.Setup(s => s.InsertAsync(It.IsAny<LivePollingSession>(), It.IsAny<CancellationToken>()))
                 .Returns(Task.CompletedTask);

        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "FastPollingEnable", It.IsAny<string>(), "PollerModule", 30, It.IsAny<CancellationToken>()))
               .ReturnsAsync(DirectMethodResult.FromModule(200, """
                   {"result":"accepted","durationMinutes":15,"intervalSeconds":1,
                    "sessionId":"cc27db6b-508b-488e-a816-faae684491d8","defaultsApplied":[]}
                   """));

        var result = await _controller.StartAsync(DeviceId, null, CancellationToken.None);

        var ok = result.Result.Should().BeOfType<OkObjectResult>().Subject;
        var body = ok.Value.Should().BeOfType<ApiResponse<LivePollingSessionDto>>().Subject;
        body.Succeeded.Should().BeTrue();
        body.Data!.SessionId.Should().Be(Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8"));
        body.Data.ExpiresUtc.Should().Be(new DateTime(2026, 9, 21, 10, 15, 0, DateTimeKind.Utc));
        body.Data.TotalDurationMinutes.Should().Be(15);
        _sessions.Verify(s => s.InsertAsync(It.IsAny<LivePollingSession>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Start_SessionAlreadyActive_Returns409WithCodeAndDoesNotCallTheGateway()
    {
        GivenDeviceResolves();
        _sessions.Setup(s => s.GetActiveForDeviceAsync(DeviceGuid, It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(new LivePollingSession
                 {
                     SessionId = Guid.NewGuid(),
                     ExpiresUtc = new DateTime(2026, 9, 21, 10, 12, 0, DateTimeKind.Utc)
                 });

        var result = await _controller.StartAsync(DeviceId, null, CancellationToken.None);

        var conflict = result.Result.Should().BeOfType<ConflictObjectResult>().Subject;
        var body = conflict.Value.Should().BeOfType<ApiResponse<object>>().Subject;
        body.Succeeded.Should().BeFalse();
        body.Errors.Should().Contain(LivePollingErrorCodes.AlreadyActive);
        _iotHub.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Start_CapacityReached_Returns409()
    {
        GivenDeviceResolves();
        GivenNoActiveSession();
        _sessions.Setup(s => s.CountActiveAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>())).ReturnsAsync(10);

        var result = await _controller.StartAsync(DeviceId, null, CancellationToken.None);

        var conflict = result.Result.Should().BeOfType<ConflictObjectResult>().Subject;
        conflict.Value.Should().BeOfType<ApiResponse<object>>()
                .Which.Errors.Should().Contain(LivePollingErrorCodes.CapacityReached);
        _iotHub.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Start_SuppliedValueOutOfRange_Returns400BeforeCallingTheGateway()
    {
        GivenDeviceResolves();

        var result = await _controller.StartAsync(
            DeviceId, new StartLivePollingRequest { SessionDurationMinutes = 45 }, CancellationToken.None);

        var bad = result.Result.Should().BeOfType<BadRequestObjectResult>().Subject;
        var body = bad.Value.Should().BeOfType<ApiResponse<object>>().Subject;
        body.Errors.Should().Contain(LivePollingErrorCodes.InvalidParameters);
        body.Errors.Should().Contain(e => e.Contains("10-30"));
        _iotHub.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Start_GatewayUnreachable_Returns503AndRecordsNothing()
    {
        GivenDeviceResolves();
        GivenNoActiveSession();
        _sessions.Setup(s => s.CountActiveAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "FastPollingEnable", It.IsAny<string>(), "PollerModule", 30, It.IsAny<CancellationToken>()))
               .ReturnsAsync(DirectMethodResult.Unreachable());

        var result = await _controller.StartAsync(DeviceId, null, CancellationToken.None);

        var obj = result.Result.Should().BeOfType<ObjectResult>().Subject;
        obj.StatusCode.Should().Be(503);
        obj.Value.Should().BeOfType<ApiResponse<object>>()
           .Which.Errors.Should().Contain(LivePollingErrorCodes.GatewayUnreachable);
        _sessions.Verify(s => s.InsertAsync(It.IsAny<LivePollingSession>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task Start_ModuleReturns409_IsMappedToAlreadyActiveNotAServerError()
    {
        GivenDeviceResolves();
        GivenNoActiveSession();
        _sessions.Setup(s => s.CountActiveAsync(It.IsAny<DateTime>(), It.IsAny<CancellationToken>())).ReturnsAsync(0);
        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "FastPollingEnable", It.IsAny<string>(), "PollerModule", 30, It.IsAny<CancellationToken>()))
               .ReturnsAsync(DirectMethodResult.FromModule(409,
                   """{"result":"rejected","reason":"A fast polling session is already active."}"""));

        var result = await _controller.StartAsync(DeviceId, null, CancellationToken.None);

        result.Result.Should().BeOfType<ConflictObjectResult>()
              .Which.Value.Should().BeOfType<ApiResponse<object>>()
              .Which.Errors.Should().Contain(LivePollingErrorCodes.AlreadyActive);
    }

    [Fact]
    public async Task Start_UnknownDevice_Returns404()
    {
        _devices.Setup(d => d.GetGatewayRefAsync("NOPE", It.IsAny<CancellationToken>()))
                .ReturnsAsync((DeviceGatewayRef?)null);

        var result = await _controller.StartAsync("NOPE", null, CancellationToken.None);

        result.Result.Should().BeOfType<NotFoundObjectResult>()
              .Which.Value.Should().BeOfType<ApiResponse<object>>()
              .Which.Errors.Should().Contain(LivePollingErrorCodes.DeviceNotFound);
    }
}
```

- [ ] **Step 2: Add the fake clock**

Create `shape-iot-backend/Brilliant.IoT.UnitTests/TestDoubles/FakeTimeProvider.cs`:

```csharp
namespace Brilliant.IoT.UnitTests;

/// <summary>A clock the tests move by hand, so expiry can be exercised without waiting.</summary>
public sealed class FakeTimeProvider : TimeProvider
{
    private DateTimeOffset _now;
    public FakeTimeProvider(DateTime utcNow) => _now = new DateTimeOffset(utcNow);
    public override DateTimeOffset GetUtcNow() => _now;
    public void Advance(TimeSpan by) => _now = _now.Add(by);
}
```

- [ ] **Step 3: Run to verify it fails**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePollingControllerTests"`
Expected: FAIL — `LivePollingController` does not exist.

- [ ] **Step 4: Write the controller with the start action**

```csharp
using Asp.Versioning;
using Brilliant.IoT.API.Authorization;
using Brilliant.IoT.Application.Abstractions;
using Brilliant.IoT.Application.Common.Models;
using Brilliant.IoT.Application.DTOs.LivePolling;
using Brilliant.IoT.Application.IoTHub;
using Brilliant.IoT.Application.LivePolling;
using Brilliant.IoT.Application.Repositories;
using Brilliant.IoT.Domain.Entities;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Swashbuckle.AspNetCore.Annotations;
using System.Net;
using System.Text.Json;

namespace Brilliant.IoT.API.Controllers;

/// <summary>
/// Controller for on-demand live telemetry polling sessions on a single device.
/// Live session data is never written to the telemetry database.
/// </summary>
[RequirePrivilege("LivePolling")]
[ApiController]
[ApiVersion("1.0")]
[Route("api/v{version:apiVersion}/devices/{deviceId}/live-session")]
public sealed class LivePollingController : ControllerBase
{
    private readonly ILivePollingSessionRepository _sessions;
    private readonly IIoTHubDeviceService _iotHub;
    private readonly IDeviceRepository _devices;
    private readonly ICurrentUserService _currentUser;
    private readonly LivePollingOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<LivePollingController> _logger;

    public LivePollingController(
        ILivePollingSessionRepository sessions,
        IIoTHubDeviceService iotHub,
        IDeviceRepository devices,
        ICurrentUserService currentUser,
        IOptions<LivePollingOptions> options,
        TimeProvider time,
        ILogger<LivePollingController> logger)
    {
        _sessions    = sessions;
        _iotHub      = iotHub;
        _devices     = devices;
        _currentUser = currentUser;
        _options     = options.Value;
        _time        = time;
        _logger      = logger;
    }

    private ObjectResult Fail(HttpStatusCode status, string code, string message, IEnumerable<string>? extra = null)
    {
        var errors = new List<string> { code };
        if (extra is not null) errors.AddRange(extra);
        var payload = ApiResponse<object>.Failure(message, errors);
        return status switch
        {
            HttpStatusCode.Conflict   => Conflict(payload),
            HttpStatusCode.BadRequest => BadRequest(payload),
            HttpStatusCode.NotFound   => NotFound(payload),
            _                         => StatusCode((int)status, payload)
        };
    }

    /// <summary>Starts a live polling session on a device.</summary>
    /// <remarks>
    /// The body is optional. An omitted field falls back to configuration; a supplied field outside the
    /// allowed range is rejected with 400 rather than forwarded to the gateway.
    /// Sample request: <c>{ "pollIntervalSeconds": 1, "sessionDurationMinutes": 15 }</c>
    /// </remarks>
    /// <param name="deviceId">Device identifier the session runs against.</param>
    /// <param name="request">Optional body. <c>pollIntervalSeconds</c> (int, 1-60) and <c>sessionDurationMinutes</c> (int, 10-30); both optional.</param>
    /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
    /// <returns>
    /// <list type="bullet">
    /// <item>200 OK — session started; body carries sessionId, resolved interval and duration, and expiry.</item>
    /// <item>400 Bad Request — a supplied value is outside the allowed range (LIVE_SESSION_INVALID_PARAMETERS).</item>
    /// <item>404 Not Found — device unknown or not paired to a gateway.</item>
    /// <item>409 Conflict — a session is already active on this device, or the fleet-wide cap is reached.</item>
    /// <item>503 Service Unavailable — the gateway could not be reached.</item>
    /// </list>
    /// </returns>
    [HttpPost]
    [SwaggerOperation(Summary = "Start a live polling session")]
    [Produces("application/json")]
    [ProducesResponseType(typeof(ApiResponse<LivePollingSessionDto>), (int)HttpStatusCode.OK)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.BadRequest)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.NotFound)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.Conflict)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.ServiceUnavailable)]
    public async Task<ActionResult<LivePollingSessionDto>> StartAsync(
        string deviceId,
        [FromBody] StartLivePollingRequest? request,
        CancellationToken ct)
    {
        var device = await _devices.GetGatewayRefAsync(deviceId, ct);
        if (device is null)
            return Fail(HttpStatusCode.NotFound, LivePollingErrorCodes.DeviceNotFound,
                        $"Device '{deviceId}' was not found or has no active gateway pairing.");

        var resolved = LivePollingParameters.Resolve(
            request?.PollIntervalSeconds, request?.SessionDurationMinutes, _options);
        if (resolved.Errors.Count > 0)
            return Fail(HttpStatusCode.BadRequest, LivePollingErrorCodes.InvalidParameters,
                        "Invalid request.", resolved.Errors);

        var now = _time.GetUtcNow().UtcDateTime;

        var existing = await _sessions.GetActiveForDeviceAsync(device.DeviceGuid, now, ct);
        if (existing is not null)
            return Fail(HttpStatusCode.Conflict, LivePollingErrorCodes.AlreadyActive,
                        $"A live session is already active on this device until {existing.ExpiresUtc:O}.");

        if (await _sessions.CountActiveAsync(now, ct) >= _options.MaxConcurrentSessions)
            return Fail(HttpStatusCode.Conflict, LivePollingErrorCodes.CapacityReached,
                        "The maximum number of concurrent live sessions is already running.");

        var payload = JsonSerializer.Serialize(new
        {
            sessionDurationMinutes = resolved.DurationMinutes,
            pollIntervalSeconds    = resolved.IntervalSeconds
        });

        var call = await _iotHub.InvokeDirectMethodWithResultAsync(
            device.GatewayId, "FastPollingEnable", payload,
            _options.ModuleId, _options.DirectMethodTimeoutSeconds, ct);

        if (!call.Reached)
            return Fail(HttpStatusCode.ServiceUnavailable, LivePollingErrorCodes.GatewayUnreachable,
                        "The gateway could not be reached.");

        var parsed = ModuleResponseParser.ParseEnable(call.PayloadJson);

        if (call.Status == 409)
            return Fail(HttpStatusCode.Conflict, LivePollingErrorCodes.AlreadyActive,
                        "A fast polling session is already active on the gateway.");

        if (call.Status is < 200 or >= 300)
            return Fail(HttpStatusCode.BadRequest, LivePollingErrorCodes.InvalidParameters,
                        "The gateway rejected the request.", parsed.Errors);

        var interval = parsed.IntervalSeconds ?? resolved.IntervalSeconds;
        var duration = parsed.DurationMinutes ?? resolved.DurationMinutes;
        var session = new LivePollingSession
        {
            SessionId              = parsed.SessionId ?? Guid.NewGuid(),
            DeviceGuid             = device.DeviceGuid,
            DeviceId               = deviceId,
            GatewayId              = device.GatewayId,
            StartedUtc             = now,
            StartedByUserId        = _currentUser.PrincipalId.ToString(),
            PollIntervalSeconds    = interval,
            SessionDurationMinutes = duration,
            ExpiresUtc             = now.AddMinutes(duration),
            ExtensionsUsed         = 0
        };

        await _sessions.InsertAsync(session, ct);

        _logger.LogInformation(
            "Live polling started. Device={DeviceId} Session={SessionId} Interval={Interval}s Duration={Duration}m",
            deviceId, session.SessionId, interval, duration);

        return Ok(ApiResponse<LivePollingSessionDto>.Success(new LivePollingSessionDto
        {
            SessionId              = session.SessionId,
            DeviceId               = deviceId,
            PollIntervalSeconds    = interval,
            SessionDurationMinutes = duration,
            StartedUtc             = session.StartedUtc,
            ExpiresUtc             = session.ExpiresUtc,
            TotalDurationMinutes   = duration,
            CanExtend              = duration < LivePollingParameters.MaxDurationMinutes
        }));
    }
}
```

- [ ] **Step 5: Register `TimeProvider`**

In `Program.cs`, beside the other singletons:

```csharp
builder.Services.AddSingleton(TimeProvider.System);
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePollingControllerTests"`
Expected: PASS (7 tests)

- [ ] **Step 7: Commit**

```bash
git add Brilliant.IoT.API/Controllers/LivePollingController.cs Brilliant.IoT.UnitTests Brilliant.IoT.API/Program.cs
git commit -m "feat: live polling start endpoint"
```

---

## Task 7: Extend and stop endpoints

**Files:**
- Modify: `shape-iot-backend/Brilliant.IoT.API/Controllers/LivePollingController.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.Application/LivePolling/ModuleResponseParser.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.UnitTests/Controllers/LivePollingControllerTests.cs`
- Modify: `shape-iot-backend/Brilliant.IoT.UnitTests/LivePolling/ModuleResponseParserTests.cs`

**Interfaces:**
- Consumes: everything from Task 6
- Produces:
  - `static ModuleExtendResponse ModuleResponseParser.ParseExtend(string? payloadJson)` → `record ModuleExtendResponse(int? ExtendedByMinutes, int? TotalDurationMinutes, string? Reason, IReadOnlyList<string> Errors)`
  - `LivePollingController.ExtendAsync(string deviceId, CancellationToken ct)`
  - `LivePollingController.StopAsync(string deviceId, CancellationToken ct)`

**Two things here are easy to get wrong.**

**Extend measures from the original start.** The module's method is `Extend-FastPolling`, it requires `{ sessionId, extendSessionDurationMinutes }`, and it extends the session's **total** duration from `StartedUtc` — not from the moment of the call. A session starting at T=0 for 10 minutes, extended by 10 at T=5, ends at **T=20**, not T=25. So the new expiry is `StartedUtc.AddMinutes(totalDurationMinutes)` using the value the module returns, never `ExpiresUtc.AddMinutes(...)`.

**There is no backend extension counter.** The module caps total duration at 30 minutes and returns a `400` naming the numbers when an extension would breach it. A second counter in the backend could disagree with the one that actually stops the gateway, so the backend simply forwards the request and surfaces the rejection.

**The backend never sends `FastPollingDisable` at expiry.** The module has already stopped itself, and a Disable then races a session that may have since started. Stop is only ever a deliberate user action.

- [ ] **Step 1: Write the failing parser tests**

Append to `ModuleResponseParserTests`:

```csharp
    [Fact]
    public void ParseExtend_AcceptedBody_ReadsTheNewTotalDuration()
    {
        const string json = """
        {"result":"accepted","sessionId":"cc27db6b-508b-488e-a816-faae684491d8",
         "extendedByMinutes":10,"totalDurationMinutes":20}
        """;

        var parsed = ModuleResponseParser.ParseExtend(json);

        parsed.ExtendedByMinutes.Should().Be(10);
        parsed.TotalDurationMinutes.Should().Be(20);
        parsed.Errors.Should().BeEmpty();
    }

    [Fact]
    public void ParseExtend_CapExceeded_CarriesTheReason()
    {
        const string json = """
        {"result":"rejected",
         "reason":"Extending by 20 would bring the session's total duration to 40 min, exceeding the 30 min cap (currently 20 min)."}
        """;

        var parsed = ModuleResponseParser.ParseExtend(json);

        parsed.TotalDurationMinutes.Should().BeNull();
        parsed.Reason.Should().Contain("30 min cap");
    }

    [Fact]
    public void ParseExtend_FieldErrors_AreAllListed()
    {
        const string json = """
        {"result":"rejected","reason":"Invalid request.",
         "errors":["'extendSessionDurationMinutes' must be a positive number of minutes."]}
        """;

        var parsed = ModuleResponseParser.ParseExtend(json);

        parsed.Errors.Should().ContainSingle();
    }

    [Fact]
    public void ParseExtend_UnparseableBody_YieldsEmptyResult()
    {
        var parsed = ModuleResponseParser.ParseExtend("}{");

        parsed.TotalDurationMinutes.Should().BeNull();
        parsed.Errors.Should().BeEmpty();
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~ModuleResponseParserTests"`
Expected: FAIL — `ParseExtend` does not exist.

- [ ] **Step 3: Add the extend parser**

Append to `ModuleResponseParser.cs`:

```csharp
/// <summary>What the module said in response to Extend-FastPolling.</summary>
public sealed record ModuleExtendResponse(
    int? ExtendedByMinutes,
    int? TotalDurationMinutes,
    string? Reason,
    IReadOnlyList<string> Errors);
```

and to the `ModuleResponseParser` class:

```csharp
    public static ModuleExtendResponse ParseExtend(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
            return new ModuleExtendResponse(null, null, null, Array.Empty<string>());

        try
        {
            using var doc = JsonDocument.Parse(payloadJson);
            var root = doc.RootElement;

            int? extended = root.TryGetProperty("extendedByMinutes", out var e) && e.TryGetInt32(out var ev) ? ev : null;
            int? total    = root.TryGetProperty("totalDurationMinutes", out var t) && t.TryGetInt32(out var tv) ? tv : null;
            string? reason = root.TryGetProperty("reason", out var r) && r.ValueKind == JsonValueKind.String
                ? r.GetString() : null;

            var errors = new List<string>();
            if (root.TryGetProperty("errors", out var errs) && errs.ValueKind == JsonValueKind.Array)
                foreach (var item in errs.EnumerateArray())
                    if (item.ValueKind == JsonValueKind.String)
                        errors.Add(item.GetString()!);

            return new ModuleExtendResponse(extended, total, reason, errors);
        }
        catch (JsonException)
        {
            return new ModuleExtendResponse(null, null, null, Array.Empty<string>());
        }
    }
```

- [ ] **Step 4: Run to verify they pass**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~ModuleResponseParserTests"`
Expected: PASS (8 tests)

- [ ] **Step 5: Write the failing controller tests**

Append to `LivePollingControllerTests`:

```csharp
    private LivePollingSession ActiveSession(int durationMinutes = 15)
    {
        var started = new DateTime(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc);
        return new LivePollingSession
        {
            SessionId              = Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8"),
            DeviceGuid             = DeviceGuid,
            DeviceId               = DeviceId,
            GatewayId              = GatewayId,
            StartedUtc             = started,
            PollIntervalSeconds    = 1,
            SessionDurationMinutes = durationMinutes,
            ExpiresUtc             = started.AddMinutes(durationMinutes),
            ExtensionsUsed         = 0
        };
    }

    private void GivenActiveSession(LivePollingSession session) =>
        _sessions.Setup(s => s.GetActiveForDeviceAsync(DeviceGuid, It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(session);

    [Fact]
    public async Task Extend_NewExpiryIsMeasuredFromTheOriginalStartNotFromNow()
    {
        // Session started 10:00 for 10 min. Clock is 10:05. Module extends total to 20 min.
        // Correct expiry is 10:20 — NOT 10:25 (now + 10) and NOT 10:20 via ExpiresUtc + 10 by luck.
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession(durationMinutes: 10));
        _time.Advance(TimeSpan.FromMinutes(5));

        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "Extend-FastPolling", It.IsAny<string>(), "PollerModule", 30, It.IsAny<CancellationToken>()))
               .ReturnsAsync(DirectMethodResult.FromModule(200, """
                   {"result":"accepted","sessionId":"cc27db6b-508b-488e-a816-faae684491d8",
                    "extendedByMinutes":10,"totalDurationMinutes":20}
                   """));
        _sessions.Setup(s => s.ExtendAsync(
                    It.IsAny<Guid>(), new DateTime(2026, 9, 21, 10, 20, 0, DateTimeKind.Utc),
                    It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                 .Returns(Task.CompletedTask);

        var result = await _controller.ExtendAsync(DeviceId, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingSessionDto>>().Subject;
        body.Data!.ExpiresUtc.Should().Be(new DateTime(2026, 9, 21, 10, 20, 0, DateTimeKind.Utc));
        body.Data.TotalDurationMinutes.Should().Be(20);
        body.Data.CanExtend.Should().BeTrue("20 is still under the module's 30 minute cap");
    }

    [Fact]
    public async Task Extend_SendsTheSessionIdTheModuleRequires()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        string? captured = null;
        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "Extend-FastPolling", It.IsAny<string>(), "PollerModule", 30, It.IsAny<CancellationToken>()))
               .Callback<string, string, string?, string?, int, CancellationToken>((_, _, payload, _, _, _) => captured = payload)
               .ReturnsAsync(DirectMethodResult.FromModule(200,
                   """{"result":"accepted","extendedByMinutes":10,"totalDurationMinutes":25}"""));
        _sessions.Setup(s => s.ExtendAsync(It.IsAny<Guid>(), It.IsAny<DateTime>(), It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                 .Returns(Task.CompletedTask);

        await _controller.ExtendAsync(DeviceId, CancellationToken.None);

        captured.Should().Contain("cc27db6b-508b-488e-a816-faae684491d8");
        captured.Should().Contain("extendSessionDurationMinutes");
    }

    [Fact]
    public async Task Extend_AtTheModuleCap_Returns400CarryingTheModulesReason()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession(durationMinutes: 25));
        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "Extend-FastPolling", It.IsAny<string>(), "PollerModule", 30, It.IsAny<CancellationToken>()))
               .ReturnsAsync(DirectMethodResult.FromModule(400, """
                   {"result":"rejected","reason":"Extending by 10 would bring the session's total duration to 35 min, exceeding the 30 min cap (currently 25 min)."}
                   """));

        var result = await _controller.ExtendAsync(DeviceId, CancellationToken.None);

        var bad = result.Result.Should().BeOfType<BadRequestObjectResult>().Subject;
        var body = bad.Value.Should().BeOfType<ApiResponse<object>>().Subject;
        body.Errors.Should().Contain(LivePollingErrorCodes.ExtensionLimit);
        body.Errors.Should().Contain(e => e.Contains("30 min cap"));
    }

    [Fact]
    public async Task Extend_NoActiveSession_Returns409AndDoesNotCallTheGateway()
    {
        GivenDeviceResolves();
        GivenNoActiveSession();

        var result = await _controller.ExtendAsync(DeviceId, CancellationToken.None);

        result.Result.Should().BeOfType<ConflictObjectResult>()
              .Which.Value.Should().BeOfType<ApiResponse<object>>()
              .Which.Errors.Should().Contain(LivePollingErrorCodes.NotActive);
        _iotHub.VerifyNoOtherCalls();
    }

    [Fact]
    public async Task Extend_ModuleSessionIdMismatch_Returns409()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "Extend-FastPolling", It.IsAny<string>(), "PollerModule", 30, It.IsAny<CancellationToken>()))
               .ReturnsAsync(DirectMethodResult.FromModule(409,
                   """{"result":"rejected","reason":"sessionId does not match the currently active session."}"""));

        var result = await _controller.ExtendAsync(DeviceId, CancellationToken.None);

        result.Result.Should().BeOfType<ConflictObjectResult>()
              .Which.Value.Should().BeOfType<ApiResponse<object>>()
              .Which.Errors.Should().Contain(LivePollingErrorCodes.NotActive);
    }

    [Fact]
    public async Task Stop_ActiveSession_CallsDisableAndClosesTheRow()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "FastPollingDisable", null, "PollerModule", 30, It.IsAny<CancellationToken>()))
               .ReturnsAsync(DirectMethodResult.FromModule(200, """{"result":"accepted"}"""));
        _sessions.Setup(s => s.StopAsync(It.IsAny<Guid>(), It.IsAny<DateTime>(), "ManualStop", It.IsAny<CancellationToken>()))
                 .Returns(Task.CompletedTask);

        var result = await _controller.StopAsync(DeviceId, CancellationToken.None);

        result.Should().BeOfType<OkObjectResult>();
        _sessions.Verify(s => s.StopAsync(It.IsAny<Guid>(), It.IsAny<DateTime>(), "ManualStop", It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task Stop_GatewayAlreadyStopped_StillClosesTheRow()
    {
        GivenDeviceResolves();
        GivenActiveSession(ActiveSession());
        _iotHub.Setup(h => h.InvokeDirectMethodWithResultAsync(
                    GatewayId, "FastPollingDisable", null, "PollerModule", 30, It.IsAny<CancellationToken>()))
               .ReturnsAsync(DirectMethodResult.FromModule(409,
                   """{"result":"rejected","reason":"No fast polling session is active."}"""));
        _sessions.Setup(s => s.StopAsync(It.IsAny<Guid>(), It.IsAny<DateTime>(), "ManualStop", It.IsAny<CancellationToken>()))
                 .Returns(Task.CompletedTask);

        var result = await _controller.StopAsync(DeviceId, CancellationToken.None);

        result.Should().BeOfType<OkObjectResult>();
        _sessions.Verify(s => s.StopAsync(It.IsAny<Guid>(), It.IsAny<DateTime>(), "ManualStop", It.IsAny<CancellationToken>()), Times.Once);
    }
```

The first test is the one that matters — it advances the clock by 5 minutes precisely so that "from the original start" and "from now" give different answers, and asserts the former.

The last test encodes a deliberate decision: the gateway saying "nothing was running" is not a failure of *stop*. The outcome the caller asked for — no session running, row closed — has been achieved, so returning an error would be wrong and would leave the row open forever.

- [ ] **Step 6: Run to verify they fail**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePollingControllerTests"`
Expected: FAIL — `ExtendAsync` and `StopAsync` do not exist.

- [ ] **Step 7: Implement extend**

```csharp
    /// <summary>Extends the running session's total duration.</summary>
    /// <remarks>
    /// The gateway measures duration from the session's original start, so extending by ten minutes
    /// adds ten minutes to the total plan regardless of when it is called. The module caps total
    /// duration at 30 minutes and rejects anything beyond it; there is no separate backend limit.
    /// Sample success response:
    /// <c>{ "succeeded": true, "data": { "sessionId": "cc27db6b-…", "expiresUtc": "2026-09-21T10:20:00Z", "totalDurationMinutes": 20, "canExtend": true } }</c>
    /// Sample 400 at the cap:
    /// <c>{ "succeeded": false, "message": "The gateway rejected the extension.", "errors": ["LIVE_SESSION_EXTENSION_LIMIT_REACHED", "Extending by 10 would bring the session's total duration to 35 min, exceeding the 30 min cap (currently 25 min)."] }</c>
    /// </remarks>
    /// <param name="deviceId">Device whose session is being extended.</param>
    /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
    /// <returns>
    /// <list type="bullet">
    /// <item>200 OK — extended; body carries the new expiry and total duration.</item>
    /// <item>400 Bad Request — the module's 30-minute cap would be breached (LIVE_SESSION_EXTENSION_LIMIT_REACHED), with its reason echoed.</item>
    /// <item>404 Not Found — device unknown or not paired to a gateway.</item>
    /// <item>409 Conflict — no active session, or the gateway's session id no longer matches (LIVE_SESSION_NOT_ACTIVE).</item>
    /// <item>503 Service Unavailable — the gateway could not be reached.</item>
    /// </list>
    /// </returns>
    [HttpPost("extend")]
    [SwaggerOperation(Summary = "Extend a live polling session")]
    [Produces("application/json")]
    [ProducesResponseType(typeof(ApiResponse<LivePollingSessionDto>), (int)HttpStatusCode.OK)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.BadRequest)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.NotFound)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.Conflict)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.ServiceUnavailable)]
    public async Task<ActionResult<LivePollingSessionDto>> ExtendAsync(string deviceId, CancellationToken ct)
    {
        var device = await _devices.GetGatewayRefAsync(deviceId, ct);
        if (device is null)
            return Fail(HttpStatusCode.NotFound, LivePollingErrorCodes.DeviceNotFound,
                        $"Device '{deviceId}' was not found or has no active gateway pairing.");

        var now = _time.GetUtcNow().UtcDateTime;
        var session = await _sessions.GetActiveForDeviceAsync(device.DeviceGuid, now, ct);
        if (session is null)
            return Fail(HttpStatusCode.Conflict, LivePollingErrorCodes.NotActive,
                        "No live session is active on this device.");

        // The module requires both fields; there is no default fallback on this method.
        var payload = JsonSerializer.Serialize(new
        {
            sessionId                    = session.SessionId,
            extendSessionDurationMinutes = _options.ExtensionMinutes
        });

        var call = await _iotHub.InvokeDirectMethodWithResultAsync(
            device.GatewayId, "Extend-FastPolling", payload,
            _options.ModuleId, _options.DirectMethodTimeoutSeconds, ct);

        if (!call.Reached)
            return Fail(HttpStatusCode.ServiceUnavailable, LivePollingErrorCodes.GatewayUnreachable,
                        "The gateway could not be reached.");

        var parsed = ModuleResponseParser.ParseExtend(call.PayloadJson);

        if (call.Status == 409)
            return Fail(HttpStatusCode.Conflict, LivePollingErrorCodes.NotActive,
                        parsed.Reason ?? "The gateway reports no matching active session.");

        if (call.Status is < 200 or >= 300)
        {
            var detail = new List<string>(parsed.Errors);
            if (parsed.Reason is not null) detail.Add(parsed.Reason);
            return Fail(HttpStatusCode.BadRequest, LivePollingErrorCodes.ExtensionLimit,
                        "The gateway rejected the extension.", detail);
        }

        // Total duration is measured from the ORIGINAL start, so the new expiry is
        // StartedUtc + totalDurationMinutes — never ExpiresUtc + extensionMinutes.
        var totalMinutes = parsed.TotalDurationMinutes
                           ?? session.SessionDurationMinutes + _options.ExtensionMinutes;
        var newExpiry = session.StartedUtc.AddMinutes(totalMinutes);

        await _sessions.ExtendAsync(session.SessionId, newExpiry, now, ct);

        _logger.LogInformation(
            "Live polling extended. Device={DeviceId} Session={SessionId} TotalMinutes={Total} NewExpiry={Expiry:O}",
            deviceId, session.SessionId, totalMinutes, newExpiry);

        return Ok(ApiResponse<LivePollingSessionDto>.Success(new LivePollingSessionDto
        {
            SessionId              = session.SessionId,
            DeviceId               = deviceId,
            PollIntervalSeconds    = session.PollIntervalSeconds,
            SessionDurationMinutes = totalMinutes,
            StartedUtc             = session.StartedUtc,
            ExpiresUtc             = newExpiry,
            TotalDurationMinutes   = totalMinutes,
            CanExtend              = totalMinutes < LivePollingParameters.MaxDurationMinutes
        }));
    }
```

`ExtendAsync` on the repository stores `newExpiry` and increments `ExtensionsUsed`; the counter is kept for audit only and never gates anything.

**Note for Task 4:** `LivePollingSessionRepository.ExtendAsync` must also update `SessionDurationMinutes` to the new total, otherwise status and a second extend would report the original figure. Add `SessionDurationMinutes = @TotalMinutes` to that UPDATE and a `totalMinutes` parameter to the method signature — adjust its integration test to assert the new total alongside the new expiry.

- [ ] **Step 8: Implement stop**

```csharp
    /// <summary>Stops the running session early.</summary>
    /// <remarks>
    /// Only ever a deliberate user action. Auto-expiry needs no call — the gateway stops itself and the
    /// session row is treated as closed once its expiry passes.
    /// A gateway reply of "no session is active" is still a success: the requested end state has been reached.
    /// Sample success response: <c>{ "succeeded": true, "message": "Live session stopped." }</c>
    /// Sample 409: <c>{ "succeeded": false, "message": "No live session is active on this device.", "errors": ["LIVE_SESSION_NOT_ACTIVE"] }</c>
    /// </remarks>
    /// <param name="deviceId">Device whose session is being stopped.</param>
    /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
    /// <returns>
    /// <list type="bullet">
    /// <item>200 OK — session stopped and the record closed.</item>
    /// <item>404 Not Found — device unknown or not paired to a gateway.</item>
    /// <item>409 Conflict — no active session (LIVE_SESSION_NOT_ACTIVE).</item>
    /// <item>503 Service Unavailable — the gateway could not be reached; the record is left open.</item>
    /// </list>
    /// </returns>
    [HttpDelete]
    [SwaggerOperation(Summary = "Stop a live polling session")]
    [Produces("application/json")]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.OK)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.NotFound)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.Conflict)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.ServiceUnavailable)]
    public async Task<IActionResult> StopAsync(string deviceId, CancellationToken ct)
    {
        var device = await _devices.GetGatewayRefAsync(deviceId, ct);
        if (device is null)
            return Fail(HttpStatusCode.NotFound, LivePollingErrorCodes.DeviceNotFound,
                        $"Device '{deviceId}' was not found or has no active gateway pairing.");

        var now = _time.GetUtcNow().UtcDateTime;
        var session = await _sessions.GetActiveForDeviceAsync(device.DeviceGuid, now, ct);
        if (session is null)
            return Fail(HttpStatusCode.Conflict, LivePollingErrorCodes.NotActive,
                        "No live session is active on this device.");

        var call = await _iotHub.InvokeDirectMethodWithResultAsync(
            device.GatewayId, "FastPollingDisable", null,
            _options.ModuleId, _options.DirectMethodTimeoutSeconds, ct);

        if (!call.Reached)
            return Fail(HttpStatusCode.ServiceUnavailable, LivePollingErrorCodes.GatewayUnreachable,
                        "The gateway could not be reached. The session was left open.");

        // A 409 from the module means it had already stopped. The caller's intent is satisfied,
        // so close the record rather than leaving it open forever.
        await _sessions.StopAsync(session.SessionId, now, "ManualStop", ct);

        _logger.LogInformation(
            "Live polling stopped. Device={DeviceId} Session={SessionId} ModuleStatus={Status}",
            deviceId, session.SessionId, call.Status);

        return Ok(ApiResponse<object>.Success(default!, "Live session stopped."));
    }
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePolling"`
Expected: PASS — 14 controller tests plus 8 parser tests plus the clamping tests.

- [ ] **Step 10: Commit**

```bash
git add Brilliant.IoT.API/Controllers/LivePollingController.cs Brilliant.IoT.Application/LivePolling Brilliant.IoT.Infrastructure/Repositories/LivePollingSessionRepository.cs Brilliant.IoT.UnitTests
git commit -m "feat: live polling extend and stop endpoints"
```

---


## Task 8: Key scheme and pure fold logic

**Repo:** `shape-iot-function-data-ingestion`

**Files:**
- Create: `LivePolling/LiveSessionKeys.cs`
- Create: `LivePolling/SnapshotFolder.cs`
- Create: `LivePolling/DeviceStatusDeriver.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/LiveSessionKeysTests.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/SnapshotFolderTests.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/DeviceStatusDeriverTests.cs`

**Interfaces:**
- Consumes: nothing
- Produces (namespace `ShapeIot.Ingestion.LivePolling`):
  - `LiveSessionKeys.CurrentRowKey` = `"!current"`
  - `static string LiveSessionKeys.ParamRowKey(string metric, DateTime timestampUtc)`
  - `static (string From, string To) LiveSessionKeys.ParamRange(string metric, DateTime? since)`
  - `static string LiveSessionKeys.StatusMetric` = `"DeviceStatus"`
  - `static IDictionary<string, object?> SnapshotFolder.Fold(IDictionary<string, object?> current, IDictionary<string, object?> changes)`
  - `static string DeviceStatusDeriver.Derive(IDictionary<string, object?> snapshot)`

These three are pure functions with no Azure dependency, so they carry the correctness of the whole feature in fast unit tests. **Task 12 reproduces `LiveSessionKeys` verbatim in the backend** — the two copies must stay identical; do not extract them into `ShapeIoT.Telemetry.Shared`, which is vendored into three repos and guarded by `tools/Verify-TelemetryShared.ps1`.

- [ ] **Step 1: Write the failing key tests**

```csharp
using FluentAssertions;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests.LivePolling;

public class LiveSessionKeysTests
{
    private static readonly DateTime Ts = new(2026, 9, 21, 10, 27, 2, 218, DateTimeKind.Utc);

    [Fact]
    public void CurrentRowKey_SortsBeforeEveryParameterRow()
    {
        var param = LiveSessionKeys.ParamRowKey("AaaMetric", Ts);

        string.CompareOrdinal(LiveSessionKeys.CurrentRowKey, param).Should().BeNegative();
    }

    [Fact]
    public void ParamRowKey_IsMetricPipeRoundTripTimestamp()
    {
        LiveSessionKeys.ParamRowKey("OperatingPressurePSI", Ts)
            .Should().Be("OperatingPressurePSI|2026-09-21T10:27:02.2180000Z");
    }

    [Fact]
    public void RowKeys_SortChronologicallyWithinAMetric()
    {
        var a = LiveSessionKeys.ParamRowKey("M", Ts);
        var b = LiveSessionKeys.ParamRowKey("M", Ts.AddSeconds(1));

        string.CompareOrdinal(a, b).Should().BeNegative();
    }

    [Fact]
    public void ParamRange_ExcludesMetricsSharingAPrefix()
    {
        var (from, to) = LiveSessionKeys.ParamRange("StrokeRate", null);
        var other = LiveSessionKeys.ParamRowKey("StrokeRateLeft", Ts);

        // StrokeRateLeft must fall OUTSIDE the StrokeRate range.
        (string.CompareOrdinal(other, from) >= 0 && string.CompareOrdinal(other, to) < 0)
            .Should().BeFalse();
    }

    [Fact]
    public void ParamRange_IncludesItsOwnRows()
    {
        var (from, to) = LiveSessionKeys.ParamRange("StrokeRateLeft", null);
        var mine = LiveSessionKeys.ParamRowKey("StrokeRateLeft", Ts);

        string.CompareOrdinal(mine, from).Should().BeGreaterThanOrEqualTo(0);
        string.CompareOrdinal(mine, to).Should().BeNegative();
    }

    [Fact]
    public void ParamRange_WithSince_StartsAtThatTimestamp()
    {
        var (from, _) = LiveSessionKeys.ParamRange("M", Ts);

        from.Should().Be("M|2026-09-21T10:27:02.2180000Z");
    }
}
```

The fourth test is the one that matters: without the `|` delimiter a prefix query for `StrokeRate` would also return `StrokeRateLeft` and `StrokeRateRight`, silently mixing three series into one graph.

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveSessionKeysTests"`
Expected: FAIL — `LiveSessionKeys` does not exist.

- [ ] **Step 3: Write the keys**

```csharp
namespace ShapeIot.Ingestion.LivePolling;

/// <summary>
/// Row key scheme for the live session partition. PartitionKey is always the session id.
/// Reproduced verbatim in the backend (Brilliant.IoT.Infrastructure.LivePolling.LiveSessionKeys);
/// the two copies must stay identical.
/// </summary>
public static class LiveSessionKeys
{
    /// <summary>Head row holding the folded snapshot. '!' (0x21) sorts before every letter.</summary>
    public const string CurrentRowKey = "!current";

    /// <summary>Metric name under which device status is stored as its own series.</summary>
    public const string StatusMetric = "DeviceStatus";

    private const char Delimiter = '|';

    /// <summary>Row key for one change point. Round-trip timestamps sort chronologically.</summary>
    public static string ParamRowKey(string metric, DateTime timestampUtc) =>
        $"{metric}{Delimiter}{timestampUtc.ToUniversalTime():O}";

    /// <summary>
    /// Inclusive-from, exclusive-to range covering exactly one metric's rows.
    /// The upper bound uses the character after the delimiter, so a metric whose name is a
    /// prefix of another (StrokeRate vs StrokeRateLeft) cannot leak into the range.
    /// </summary>
    public static (string From, string To) ParamRange(string metric, DateTime? since)
    {
        var from = since is { } s
            ? $"{metric}{Delimiter}{s.ToUniversalTime():O}"
            : $"{metric}{Delimiter}";
        var to = $"{metric}{(char)(Delimiter + 1)}";
        return (from, to);
    }
}
```

- [ ] **Step 4: Write the failing fold and status tests**

```csharp
using FluentAssertions;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests.LivePolling;

public class SnapshotFolderTests
{
    [Fact]
    public void Fold_AppliesChangesOverCurrent()
    {
        var current = new Dictionary<string, object?> { ["A"] = 1, ["B"] = true };
        var changes = new Dictionary<string, object?> { ["A"] = 2 };

        var folded = SnapshotFolder.Fold(current, changes);

        folded["A"].Should().Be(2);
        folded["B"].Should().Be(true);
    }

    [Fact]
    public void Fold_KeepsUntouchedFields()
    {
        var current = new Dictionary<string, object?> { ["Kept"] = "yes" };

        var folded = SnapshotFolder.Fold(current, new Dictionary<string, object?>());

        folded.Should().ContainKey("Kept");
        folded["Kept"].Should().Be("yes");
    }

    [Fact]
    public void Fold_AddsFieldsNotSeenBefore()
    {
        var folded = SnapshotFolder.Fold(
            new Dictionary<string, object?>(),
            new Dictionary<string, object?> { ["New"] = 7 });

        folded["New"].Should().Be(7);
    }

    [Fact]
    public void Fold_DoesNotMutateTheInput()
    {
        var current = new Dictionary<string, object?> { ["A"] = 1 };

        SnapshotFolder.Fold(current, new Dictionary<string, object?> { ["A"] = 2 });

        current["A"].Should().Be(1);
    }
}

public class DeviceStatusDeriverTests
{
    [Fact]
    public void AllZero_IsNormal() =>
        DeviceStatusDeriver.Derive(new Dictionary<string, object?>
        {
            ["OperatingAlarmActive"] = 0, ["OperatingWarningActive"] = 0,
            ["MaintenanceAlarmActive"] = false, ["MaintenanceWarningActive"] = false
        }).Should().Be("Normal");

    [Fact]
    public void OperatingWarning_IsWarning() =>
        DeviceStatusDeriver.Derive(new Dictionary<string, object?>
        {
            ["OperatingAlarmActive"] = 0, ["OperatingWarningActive"] = 1
        }).Should().Be("Warning");

    [Fact]
    public void MaintenanceWarningBool_IsWarning() =>
        DeviceStatusDeriver.Derive(new Dictionary<string, object?>
        {
            ["MaintenanceWarningActive"] = true
        }).Should().Be("Warning");

    [Fact]
    public void AlarmBeatsWarning() =>
        DeviceStatusDeriver.Derive(new Dictionary<string, object?>
        {
            ["OperatingAlarmActive"] = 2, ["OperatingWarningActive"] = 1
        }).Should().Be("Alarm");

    [Fact]
    public void MissingFields_AreTreatedAsNormal() =>
        DeviceStatusDeriver.Derive(new Dictionary<string, object?>()).Should().Be("Normal");
}
```

- [ ] **Step 5: Run to verify they fail**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~SnapshotFolderTests|FullyQualifiedName~DeviceStatusDeriverTests"`
Expected: FAIL — types do not exist.

- [ ] **Step 6: Write the fold and the status deriver**

```csharp
namespace ShapeIot.Ingestion.LivePolling;

/// <summary>Merges a delta onto the running snapshot. Pure; never mutates its inputs.</summary>
public static class SnapshotFolder
{
    public static IDictionary<string, object?> Fold(
        IDictionary<string, object?> current,
        IDictionary<string, object?> changes)
    {
        var folded = new Dictionary<string, object?>(current, StringComparer.OrdinalIgnoreCase);
        foreach (var kv in changes)
            folded[kv.Key] = kv.Value;
        return folded;
    }
}
```

```csharp
namespace ShapeIot.Ingestion.LivePolling;

/// <summary>
/// Device-level status for one tick, read straight out of the folded snapshot.
/// No SQL, no alert definitions — these four fields are already in the payload.
/// </summary>
public static class DeviceStatusDeriver
{
    public const string Normal  = "Normal";
    public const string Warning = "Warning";
    public const string Alarm   = "Alarm";

    private static bool IsSet(IDictionary<string, object?> snapshot, string key) =>
        snapshot.TryGetValue(key, out var v) && v switch
        {
            null        => false,
            bool b      => b,
            int i       => i > 0,
            long l      => l > 0,
            double d    => d > 0,
            string s    => bool.TryParse(s, out var sb) ? sb
                           : double.TryParse(s, out var sd) && sd > 0,
            _           => false
        };

    public static string Derive(IDictionary<string, object?> snapshot)
    {
        if (IsSet(snapshot, "OperatingAlarmActive") || IsSet(snapshot, "MaintenanceAlarmActive"))
            return Alarm;
        if (IsSet(snapshot, "OperatingWarningActive") || IsSet(snapshot, "MaintenanceWarningActive"))
            return Warning;
        return Normal;
    }
}
```

- [ ] **Step 7: Run all three test classes**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LivePolling"`
Expected: PASS (15 tests)

- [ ] **Step 8: Commit**

```bash
git add LivePolling shape-iot-function-data-ingestion.Tests/LivePolling
git commit -m "feat: live session key scheme, snapshot fold and status derivation"
```

---

## Task 9: Table Storage session store

**Repo:** `shape-iot-function-data-ingestion`

**Files:**
- Modify: `shape-iot-function-data-ingestion.csproj`
- Create: `LivePolling/LiveSessionEntities.cs`
- Create: `LivePolling/ILiveSessionStore.cs`
- Create: `LivePolling/TableLiveSessionStore.cs`
- Create: `shape-iot-function-data-ingestion.Tests/LivePolling/InMemoryLiveSessionStore.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/InMemoryLiveSessionStoreTests.cs`

**Interfaces:**
- Consumes: `LiveSessionKeys` (T8)
- Produces:
  - `record LiveCurrent(Guid SessionId, Guid DeviceGuid, IDictionary<string, object?> Snapshot, string DeviceStatus, DateTime LastSnapshotTimestampUtc, int TickCount, long BytesTotal, DateTime FirstTickUtc, DateTime LastTickUtc, string ETag)`
  - `record LiveTickWrite(string Metric, object? Value, DateTime TimestampUtc)`
  - `ILiveSessionStore` with `GetCurrentAsync`, `WriteBatchAsync`, `DeleteSessionAsync`
  - `TableLiveSessionStore` (production) and `InMemoryLiveSessionStore` (tests)

`WriteBatchAsync` takes the ETag it is writing against and returns `false` on a 412 precondition failure, so the caller re-reads and retries rather than clobbering a concurrent fold.

- [ ] **Step 1: Add the package**

```bash
dotnet add shape-iot-function-data-ingestion.csproj package Azure.Data.Tables --version 12.9.1
```

- [ ] **Step 2: Write the contracts**

```csharp
namespace ShapeIot.Ingestion.LivePolling;

/// <summary>The head row: the folded snapshot plus session counters.</summary>
public sealed record LiveCurrent(
    Guid SessionId,
    Guid DeviceGuid,
    IDictionary<string, object?> Snapshot,
    string DeviceStatus,
    DateTime LastSnapshotTimestampUtc,
    int TickCount,
    long BytesTotal,
    DateTime FirstTickUtc,
    DateTime LastTickUtc,
    string ETag);

/// <summary>One change point to record.</summary>
public sealed record LiveTickWrite(string Metric, object? Value, DateTime TimestampUtc);
```

```csharp
namespace ShapeIot.Ingestion.LivePolling;

/// <summary>Ephemeral per-session store. Nothing here ever reaches the telemetry database.</summary>
public interface ILiveSessionStore
{
    /// <summary>The session's head row, or null when the session has no data yet.</summary>
    Task<LiveCurrent?> GetCurrentAsync(Guid sessionId, CancellationToken ct);

    /// <summary>
    /// Writes the head row and every change point from one batch as a single atomic transaction.
    /// Returns false when <paramref name="expectedETag"/> no longer matches, meaning another
    /// instance folded first and the caller should re-read.
    /// </summary>
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
        string? expectedETag,
        CancellationToken ct);

    /// <summary>Deletes every row in the session's partition.</summary>
    Task DeleteSessionAsync(Guid sessionId, CancellationToken ct);
}
```

- [ ] **Step 3: Write the in-memory double and its tests**

```csharp
using System.Collections.Concurrent;
using ShapeIot.Ingestion.LivePolling;

namespace ShapeIot.Ingestion.Tests.LivePolling;

/// <summary>Mirrors TableLiveSessionStore's semantics, including ETag conflict, without Azure.</summary>
public sealed class InMemoryLiveSessionStore : ILiveSessionStore
{
    private readonly ConcurrentDictionary<Guid, LiveCurrent> _heads = new();
    private readonly ConcurrentDictionary<Guid, List<(string RowKey, object? Value, string Metric, DateTime Ts)>> _rows = new();

    public IReadOnlyList<(string RowKey, object? Value, string Metric, DateTime Ts)> RowsFor(Guid sessionId) =>
        _rows.TryGetValue(sessionId, out var r) ? r : Array.Empty<(string, object?, string, DateTime)>().ToList();

    public Task<LiveCurrent?> GetCurrentAsync(Guid sessionId, CancellationToken ct) =>
        Task.FromResult(_heads.TryGetValue(sessionId, out var c) ? c : null);

    public Task<bool> WriteBatchAsync(
        Guid sessionId, Guid deviceGuid, IDictionary<string, object?> snapshot, string deviceStatus,
        DateTime snapshotTimestampUtc, int tickCount, long bytesTotal, DateTime firstTickUtc,
        IReadOnlyList<LiveTickWrite> changePoints, string? expectedETag, CancellationToken ct)
    {
        if (_heads.TryGetValue(sessionId, out var existing) && existing.ETag != expectedETag)
            return Task.FromResult(false);

        _heads[sessionId] = new LiveCurrent(sessionId, deviceGuid, snapshot, deviceStatus,
            snapshotTimestampUtc, tickCount, bytesTotal, firstTickUtc, snapshotTimestampUtc,
            Guid.NewGuid().ToString());

        var list = _rows.GetOrAdd(sessionId, _ => new List<(string, object?, string, DateTime)>());
        foreach (var cp in changePoints)
            list.Add((LiveSessionKeys.ParamRowKey(cp.Metric, cp.TimestampUtc), cp.Value, cp.Metric, cp.TimestampUtc));

        return Task.FromResult(true);
    }

    public Task DeleteSessionAsync(Guid sessionId, CancellationToken ct)
    {
        _heads.TryRemove(sessionId, out _);
        _rows.TryRemove(sessionId, out _);
        return Task.CompletedTask;
    }
}
```

```csharp
using FluentAssertions;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests.LivePolling;

public class InMemoryLiveSessionStoreTests
{
    private readonly InMemoryLiveSessionStore _store = new();
    private static readonly Guid Session = Guid.NewGuid();
    private static readonly Guid Device = Guid.NewGuid();
    private static readonly DateTime Ts = new(2026, 9, 21, 10, 27, 1, DateTimeKind.Utc);

    private Task<bool> Write(IDictionary<string, object?> snap, string? etag, params LiveTickWrite[] points) =>
        _store.WriteBatchAsync(Session, Device, snap, "Normal", Ts, 1, 100, Ts, points, etag, CancellationToken.None);

    [Fact]
    public async Task FirstWrite_CreatesTheHeadAndItsRows()
    {
        var ok = await Write(new Dictionary<string, object?> { ["A"] = 1 }, null,
                             new LiveTickWrite("A", 1, Ts));

        ok.Should().BeTrue();
        (await _store.GetCurrentAsync(Session, CancellationToken.None)).Should().NotBeNull();
        _store.RowsFor(Session).Should().ContainSingle().Which.RowKey.Should().StartWith("A|");
    }

    [Fact]
    public async Task StaleETag_IsRejected()
    {
        await Write(new Dictionary<string, object?> { ["A"] = 1 }, null, new LiveTickWrite("A", 1, Ts));

        var ok = await Write(new Dictionary<string, object?> { ["A"] = 2 }, "stale-etag",
                             new LiveTickWrite("A", 2, Ts.AddSeconds(1)));

        ok.Should().BeFalse();
    }

    [Fact]
    public async Task Delete_RemovesEverythingForTheSession()
    {
        await Write(new Dictionary<string, object?> { ["A"] = 1 }, null, new LiveTickWrite("A", 1, Ts));

        await _store.DeleteSessionAsync(Session, CancellationToken.None);

        (await _store.GetCurrentAsync(Session, CancellationToken.None)).Should().BeNull();
        _store.RowsFor(Session).Should().BeEmpty();
    }
}
```

- [ ] **Step 4: Run to verify they fail, then pass**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~InMemoryLiveSessionStoreTests"`
Expected: FAIL first (types missing), then PASS (3 tests) once Steps 2-3 are in.

- [ ] **Step 5: Implement the Table store**

```csharp
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging;

namespace ShapeIot.Ingestion.LivePolling;

/// <summary>
/// Azure Table Storage implementation. One partition per session: a head row plus one row
/// per change point, written together as an entity group transaction so the snapshot and its
/// change points can never disagree.
/// </summary>
public sealed class TableLiveSessionStore : ILiveSessionStore
{
    public const string TableName = "LivePollingSessions";

    private readonly TableClient _table;
    private readonly ILogger<TableLiveSessionStore> _logger;

    public TableLiveSessionStore(TableServiceClient service, ILogger<TableLiveSessionStore> logger)
    {
        _table = service.GetTableClient(TableName);
        _table.CreateIfNotExists();
        _logger = logger;
    }

    public async Task<LiveCurrent?> GetCurrentAsync(Guid sessionId, CancellationToken ct)
    {
        try
        {
            var e = await _table.GetEntityAsync<TableEntity>(
                sessionId.ToString(), LiveSessionKeys.CurrentRowKey, cancellationToken: ct);

            var json = e.Value.GetString("Snapshot") ?? "{}";
            var snapshot = JsonSerializer.Deserialize<Dictionary<string, object?>>(json)
                           ?? new Dictionary<string, object?>();

            return new LiveCurrent(
                sessionId,
                e.Value.GetGuid("DeviceGuid") ?? Guid.Empty,
                snapshot,
                e.Value.GetString("DeviceStatus") ?? DeviceStatusDeriver.Normal,
                e.Value.GetDateTime("LastSnapshotTimestampUtc") ?? DateTime.MinValue,
                e.Value.GetInt32("TickCount") ?? 0,
                e.Value.GetInt64("BytesTotal") ?? 0,
                e.Value.GetDateTime("FirstTickUtc") ?? DateTime.MinValue,
                e.Value.GetDateTime("LastTickUtc") ?? DateTime.MinValue,
                e.Value.ETag.ToString());
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<bool> WriteBatchAsync(
        Guid sessionId, Guid deviceGuid, IDictionary<string, object?> snapshot, string deviceStatus,
        DateTime snapshotTimestampUtc, int tickCount, long bytesTotal, DateTime firstTickUtc,
        IReadOnlyList<LiveTickWrite> changePoints, string? expectedETag, CancellationToken ct)
    {
        var pk = sessionId.ToString();

        var head = new TableEntity(pk, LiveSessionKeys.CurrentRowKey)
        {
            ["DeviceGuid"]               = deviceGuid,
            ["Snapshot"]                 = JsonSerializer.Serialize(snapshot),
            ["DeviceStatus"]             = deviceStatus,
            ["LastSnapshotTimestampUtc"] = snapshotTimestampUtc,
            ["TickCount"]                = tickCount,
            ["BytesTotal"]               = bytesTotal,
            ["FirstTickUtc"]             = firstTickUtc,
            ["LastTickUtc"]              = snapshotTimestampUtc
        };

        var actions = new List<TableTransactionAction>
        {
            expectedETag is null
                ? new TableTransactionAction(TableTransactionActionType.Add, head)
                : new TableTransactionAction(TableTransactionActionType.UpdateReplace, head, new ETag(expectedETag))
        };

        foreach (var cp in changePoints)
        {
            var row = new TableEntity(pk, LiveSessionKeys.ParamRowKey(cp.Metric, cp.TimestampUtc))
            {
                ["V"] = cp.Value,
                ["T"] = cp.Value switch
                {
                    bool => "B", int or long => "L", double or float => "D", _ => "S"
                }
            };
            actions.Add(new TableTransactionAction(TableTransactionActionType.UpsertReplace, row));
        }

        try
        {
            await _table.SubmitTransactionAsync(actions, ct);
            return true;
        }
        catch (TableTransactionFailedException ex) when (ex.Status is 409 or 412)
        {
            _logger.LogDebug("Live tick lost the ETag race for session {SessionId}; caller will retry.", sessionId);
            return false;
        }
    }

    public async Task DeleteSessionAsync(Guid sessionId, CancellationToken ct)
    {
        var pk = sessionId.ToString();
        var batch = new List<TableTransactionAction>();

        await foreach (var e in _table.QueryAsync<TableEntity>(
                           x => x.PartitionKey == pk, select: new[] { "RowKey" }, cancellationToken: ct))
        {
            batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, e));
            if (batch.Count == 100)
            {
                await _table.SubmitTransactionAsync(batch, ct);
                batch.Clear();
            }
        }

        if (batch.Count > 0)
            await _table.SubmitTransactionAsync(batch, ct);
    }
}
```

- [ ] **Step 6: Register the store with retry**

In `Program.cs`, after the existing singletons:

```csharp
// Live polling: ephemeral per-session store. Never touches SQL.
builder.Services.AddSingleton(_ =>
{
    var conn = Environment.GetEnvironmentVariable("AzureWebJobsStorage")
               ?? throw new InvalidOperationException("AzureWebJobsStorage is required for live polling.");
    var options = new TableClientOptions();
    // Storage returns 503 Server Busy / 500 Operation Timeout when a partition is pushed past its
    // target rate. Back off rather than failing the tick.
    options.Retry.Mode = Azure.Core.RetryMode.Exponential;
    options.Retry.MaxRetries = 5;
    options.Retry.Delay = TimeSpan.FromMilliseconds(100);
    options.Retry.MaxDelay = TimeSpan.FromSeconds(2);
    return new TableServiceClient(conn, options);
});
builder.Services.AddSingleton<ILiveSessionStore, TableLiveSessionStore>();
```

Add `using Azure.Data.Tables;` and `using ShapeIot.Ingestion.LivePolling;`.

- [ ] **Step 7: Build and run the suite**

Run: `dotnet build && dotnet test shape-iot-function-data-ingestion.Tests`
Expected: build succeeds, all tests pass.

- [ ] **Step 8: Commit**

```bash
git add shape-iot-function-data-ingestion.csproj LivePolling Program.cs shape-iot-function-data-ingestion.Tests/LivePolling
git commit -m "feat: table storage live session store"
```

---

## Task 10: The live telemetry function

**Repo:** `shape-iot-function-data-ingestion`

**Files:**
- Create: `LivePolling/GzipPayload.cs`
- Create: `LivePolling/LiveTelemetryBatch.cs`
- Create: `LivePolling/LiveTelemetryProcessor.cs`
- Create: `Functions/LiveTelemetryProcessorEventGrid.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/GzipPayloadTests.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/LiveTelemetryBatchTests.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/LiveTelemetryProcessorTests.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/LiveBatchPayloadSizeTests.cs`
- Modify: `Program.cs`

**Interfaces:**
- Consumes: `ILiveSessionStore` (T9), `SnapshotFolder`, `DeviceStatusDeriver`, `LiveSessionKeys` (T8), `IDeviceResolver`
- Produces:
  - `static bool GzipPayload.TryDecode(string raw, out string json)`
  - `record LiveEntry(DateTime TimestampUtc, IDictionary<string, object?> Fields, bool IsBaseline)`
  - `record LiveTelemetryBatch(string GatewayId, Guid SessionId, DateTime BatchSentUtc, IReadOnlyList<LiveEntry> Entries, int WireBytes)` with `static LiveTelemetryBatch? TryParse(CloudEvent evt)`
  - `record BroadcastEntry(DateTime TimestampUtc, string DeviceStatus, IDictionary<string, object?>? Snapshot, IDictionary<string, object?>? Changes)`
  - `record LiveBroadcast(Guid SessionId, Guid DeviceGuid, DateTime BatchSentUtc, IReadOnlyList<BroadcastEntry> Entries)`
  - `LiveTelemetryProcessor.ProcessAsync(LiveTelemetryBatch batch, Guid deviceGuid, CancellationToken ct)` → `LiveBroadcast?`

**Three things make this different from a per-tick processor.**

1. **The body is gzip.** The gateway sets `ContentEncoding: "gzip"`, so Event Grid carries the compressed bytes — base64-encoded, since they are not UTF-8 text. Decode then gunzip before any JSON parsing.
2. **Five entries per message.** They fold in `TimestampUtc` order into one running snapshot, and every change point from all five is written in a single entity group transaction.
3. **The broadcast re-materialises the baseline.** The gateway sends the full `Body` once per *session*; the backend puts a full snapshot at the head of *every* batch, because SignalR does not guarantee delivery and a lost delta would otherwise corrupt the view indefinitely.

- [ ] **Step 1: Write the failing decode tests**

```csharp
using System.IO.Compression;
using System.Text;
using FluentAssertions;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests.LivePolling;

public class GzipPayloadTests
{
    private static string GzipBase64(string json)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            var bytes = Encoding.UTF8.GetBytes(json);
            gzip.Write(bytes, 0, bytes.Length);
        }
        return Convert.ToBase64String(output.ToArray());
    }

    [Fact]
    public void Decode_Base64Gzip_ReturnsTheOriginalJson()
    {
        const string json = """{"Entries":[{"TimestampUtc":"2026-09-21T10:00:00Z"}]}""";

        GzipPayload.TryDecode(GzipBase64(json), out var decoded).Should().BeTrue();

        decoded.Should().Be(json);
    }

    [Fact]
    public void Decode_PlainJson_PassesThroughUnchanged()
    {
        const string json = """{"Entries":[]}""";

        GzipPayload.TryDecode(json, out var decoded).Should().BeTrue();

        decoded.Should().Be(json);
    }

    [Fact]
    public void Decode_Base64ThatIsNotGzip_ReturnsTheDecodedText()
    {
        var b64 = Convert.ToBase64String(Encoding.UTF8.GetBytes("""{"Entries":[]}"""));

        GzipPayload.TryDecode(b64, out var decoded).Should().BeTrue();

        decoded.Should().Be("""{"Entries":[]}""");
    }

    [Fact]
    public void Decode_CorruptGzip_FailsInsteadOfThrowing()
    {
        var corrupt = Convert.ToBase64String(new byte[] { 0x1f, 0x8b, 0x08, 0x00, 0xff, 0xfe });

        GzipPayload.TryDecode(corrupt, out _).Should().BeFalse();
    }

    [Fact]
    public void Decode_Empty_Fails()
    {
        GzipPayload.TryDecode("", out _).Should().BeFalse();
    }
}
```

A corrupt body must **fail, not throw** — an exception escaping the function fails the invocation, and Event Grid then redelivers the same poison message indefinitely.

- [ ] **Step 2: Run to verify they fail**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~GzipPayloadTests"`
Expected: FAIL — `GzipPayload` does not exist.

- [ ] **Step 3: Write the decoder**

```csharp
using System.IO.Compression;
using System.Text;

namespace ShapeIot.Ingestion.LivePolling;

/// <summary>
/// Decodes a fast-polling event body. The gateway gzips each batch and sets
/// ContentEncoding: "gzip", so Event Grid carries base64 of the compressed bytes.
/// Plain JSON and un-compressed base64 are both accepted, so the same code path works
/// against a local fixture and against the real hub.
/// </summary>
public static class GzipPayload
{
    private const byte GzipMagic0 = 0x1f;
    private const byte GzipMagic1 = 0x8b;

    public static bool TryDecode(string raw, out string json)
    {
        json = string.Empty;
        if (string.IsNullOrWhiteSpace(raw)) return false;

        var trimmed = raw.TrimStart();
        if (trimmed.StartsWith('{') || trimmed.StartsWith('['))
        {
            json = raw;
            return true;
        }

        byte[] bytes;
        try { bytes = Convert.FromBase64String(raw); }
        catch (FormatException) { return false; }

        if (bytes.Length >= 2 && bytes[0] == GzipMagic0 && bytes[1] == GzipMagic1)
        {
            try
            {
                using var input = new MemoryStream(bytes);
                using var gzip = new GZipStream(input, CompressionMode.Decompress);
                using var output = new MemoryStream();
                gzip.CopyTo(output);
                json = Encoding.UTF8.GetString(output.ToArray());
                return json.Length > 0;
            }
            catch (InvalidDataException) { return false; }
            catch (EndOfStreamException) { return false; }
        }

        json = Encoding.UTF8.GetString(bytes);
        return json.Length > 0;
    }
}
```

- [ ] **Step 4: Write the failing batch-parse tests**

```csharp
using System.IO.Compression;
using System.Text;
using Azure.Messaging;
using FluentAssertions;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests.LivePolling;

public class LiveTelemetryBatchTests
{
    private static CloudEvent Event(string dataJson) =>
        new("/devices/GW-0042", "Microsoft.Devices.DeviceTelemetry",
            BinaryData.FromString(dataJson), "application/json");

    private static string Envelope(string bodyJson) => $$"""
    {"body":{{bodyJson}},
     "properties":{"telemetry-type":"fast-polling",
                   "session-id":"cc27db6b-508b-488e-a816-faae684491d8",
                   "batchSentUtc":"2026-09-21T10:27:05.6159289Z"},
     "systemProperties":{"iothub-connection-device-id":"GW-0042"}}
    """;

    private const string FirstBatch = """
    { "Entries": [
      { "TimestampUtc":"2026-09-21T10:27:01.2200000Z",
        "Body": { "MotorRunning": true, "OperatingPressurePSI": 54351 } },
      { "TimestampUtc":"2026-09-21T10:27:02.2180000Z",
        "Changes": { "OperatingPressurePSI": 54360 } },
      { "TimestampUtc":"2026-09-21T10:27:03.1230000Z", "Changes": {} },
      { "TimestampUtc":"2026-09-21T10:27:04.2010000Z",
        "Changes": { "StrokeRateLeft": 97 } },
      { "TimestampUtc":"2026-09-21T10:27:05.1900000Z", "Changes": {} } ] }
    """;

    [Fact]
    public void TryParse_FirstBatch_ReadsFiveEntriesWithOnlyTheFirstAsBaseline()
    {
        var batch = LiveTelemetryBatch.TryParse(Event(Envelope(FirstBatch)));

        batch.Should().NotBeNull();
        batch!.Entries.Should().HaveCount(5);
        batch.Entries[0].IsBaseline.Should().BeTrue();
        batch.Entries[0].Fields.Should().HaveCount(2);
        batch.Entries[1].IsBaseline.Should().BeFalse();
        batch.Entries[2].Fields.Should().BeEmpty("a heartbeat carries no fields");
        batch.SessionId.Should().Be(Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8"));
        batch.GatewayId.Should().Be("GW-0042");
    }

    [Fact]
    public void TryParse_EntriesAreOrderedByTimestamp()
    {
        var batch = LiveTelemetryBatch.TryParse(Event(Envelope(FirstBatch)));

        batch!.Entries.Select(e => e.TimestampUtc).Should().BeInAscendingOrder();
    }

    [Fact]
    public void TryParse_PartialBatch_IsAccepted()
    {
        const string partial = """
        { "Entries": [
          { "TimestampUtc":"2026-09-21T10:27:01.2200000Z", "Changes": { "StrokeRateLeft": 97 } },
          { "TimestampUtc":"2026-09-21T10:27:02.2180000Z", "Changes": {} } ] }
        """;

        var batch = LiveTelemetryBatch.TryParse(Event(Envelope(partial)));

        batch!.Entries.Should().HaveCount(2);
    }

    [Fact]
    public void TryParse_GzippedBody_IsDecodedFirst()
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
        {
            var bytes = Encoding.UTF8.GetBytes(FirstBatch);
            gzip.Write(bytes, 0, bytes.Length);
        }
        var b64 = Convert.ToBase64String(output.ToArray());
        var envelope = $$"""
        {"body":"{{b64}}",
         "properties":{"telemetry-type":"fast-polling",
                       "session-id":"cc27db6b-508b-488e-a816-faae684491d8",
                       "batchSentUtc":"2026-09-21T10:27:05.6159289Z"},
         "systemProperties":{"iothub-connection-device-id":"GW-0042"}}
        """;

        var batch = LiveTelemetryBatch.TryParse(Event(envelope));

        batch.Should().NotBeNull();
        batch!.Entries.Should().HaveCount(5);
    }

    [Fact]
    public void TryParse_NotFastPolling_ReturnsNull()
    {
        const string other = """
        {"body":{"Entries":[]},"properties":{},
         "systemProperties":{"iothub-connection-device-id":"GW-0042"}}
        """;

        LiveTelemetryBatch.TryParse(Event(other)).Should().BeNull();
    }

    [Fact]
    public void TryParse_MissingSessionId_ReturnsNull()
    {
        const string bad = """
        {"body":{"Entries":[]},"properties":{"telemetry-type":"fast-polling"},
         "systemProperties":{"iothub-connection-device-id":"GW-0042"}}
        """;

        LiveTelemetryBatch.TryParse(Event(bad)).Should().BeNull();
    }

    [Fact]
    public void TryParse_EmptyEntries_ReturnsNull()
    {
        LiveTelemetryBatch.TryParse(Event(Envelope("""{"Entries":[]}"""))).Should().BeNull();
    }
}
```

The `NotFastPolling` test is the Event Grid filter guard in miniature: if the subscription filter is ever removed, the function drops regular telemetry rather than folding it into a live session.

- [ ] **Step 5: Write the parser**

```csharp
using System.Text.Json;
using Azure.Messaging;

namespace ShapeIot.Ingestion.LivePolling;

/// <summary>One tick inside a batch. <paramref name="Fields"/> is a full snapshot when
/// <paramref name="IsBaseline"/>, otherwise the fields that changed; empty for a heartbeat.</summary>
public sealed record LiveEntry(DateTime TimestampUtc, IDictionary<string, object?> Fields, bool IsBaseline);

/// <summary>One gzip batch of up to five ticks off the wire.</summary>
public sealed record LiveTelemetryBatch(
    string GatewayId,
    Guid SessionId,
    DateTime BatchSentUtc,
    IReadOnlyList<LiveEntry> Entries,
    int WireBytes)
{
    private const string FastPolling = "fast-polling";

    /// <summary>
    /// Reads a fast-polling batch out of an Event Grid CloudEvent, or returns null when the event
    /// is not one. Returning null rather than throwing means a stray or corrupt event is dropped
    /// and logged instead of failing the invocation and being redelivered forever.
    /// </summary>
    public static LiveTelemetryBatch? TryParse(CloudEvent evt)
    {
        try
        {
            var rawData = evt.Data?.ToString() ?? "{}";
            using var doc = JsonDocument.Parse(rawData);
            var root = doc.RootElement;

            if (!root.TryGetProperty("properties", out var props) || props.ValueKind != JsonValueKind.Object)
                return null;

            if (!TryGetString(props, "telemetry-type", out var type) ||
                !string.Equals(type, FastPolling, StringComparison.OrdinalIgnoreCase))
                return null;

            if (!TryGetString(props, "session-id", out var sid) || !Guid.TryParse(sid, out var sessionId))
                return null;

            if (!TryGetString(props, "batchSentUtc", out var sentRaw) || !TryParseUtc(sentRaw, out var batchSentUtc))
                return null;

            if (!root.TryGetProperty("systemProperties", out var sys) || sys.ValueKind != JsonValueKind.Object ||
                !TryGetString(sys, "iothub-connection-device-id", out var gatewayId))
                return null;

            if (!root.TryGetProperty("body", out var bodyEl) && !root.TryGetProperty("Body", out bodyEl))
                return null;

            // The body is gzip + base64 on the wire; a local fixture may be plain JSON.
            string bodyJson;
            if (bodyEl.ValueKind == JsonValueKind.String)
            {
                if (!GzipPayload.TryDecode(bodyEl.GetString() ?? "", out bodyJson)) return null;
            }
            else
            {
                bodyJson = bodyEl.GetRawText();
            }

            using var bodyDoc = JsonDocument.Parse(bodyJson);
            if (!bodyDoc.RootElement.TryGetProperty("Entries", out var entriesEl) &&
                !bodyDoc.RootElement.TryGetProperty("entries", out entriesEl))
                return null;
            if (entriesEl.ValueKind != JsonValueKind.Array) return null;

            var entries = new List<LiveEntry>();
            foreach (var entryEl in entriesEl.EnumerateArray())
            {
                if (entryEl.ValueKind != JsonValueKind.Object) continue;
                if (!TryGetString(entryEl, "TimestampUtc", out var tsRaw) || !TryParseUtc(tsRaw, out var ts))
                    continue;

                var isBaseline = entryEl.TryGetProperty("Body", out var fieldsEl) ||
                                 entryEl.TryGetProperty("body", out fieldsEl);
                if (!isBaseline &&
                    !entryEl.TryGetProperty("Changes", out fieldsEl) &&
                    !entryEl.TryGetProperty("changes", out fieldsEl))
                    continue;

                var fields = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                if (fieldsEl.ValueKind == JsonValueKind.Object)
                    foreach (var p in fieldsEl.EnumerateObject())
                        fields[p.Name] = ToClrValue(p.Value);

                entries.Add(new LiveEntry(ts, fields, isBaseline));
            }

            if (entries.Count == 0) return null;
            entries.Sort((a, b) => a.TimestampUtc.CompareTo(b.TimestampUtc));

            return new LiveTelemetryBatch(gatewayId, sessionId, batchSentUtc, entries, rawData.Length);
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static bool TryParseUtc(string raw, out DateTime value) =>
        DateTime.TryParse(raw, null,
            System.Globalization.DateTimeStyles.RoundtripKind |
            System.Globalization.DateTimeStyles.AdjustToUniversal, out value);

    private static bool TryGetString(JsonElement obj, string name, out string value)
    {
        value = "";
        if (!obj.TryGetProperty(name, out var el) || el.ValueKind != JsonValueKind.String) return false;
        value = el.GetString() ?? "";
        return value.Length > 0;
    }

    private static object? ToClrValue(JsonElement el) => el.ValueKind switch
    {
        JsonValueKind.True   => true,
        JsonValueKind.False  => false,
        JsonValueKind.Null   => null,
        JsonValueKind.String => el.GetString(),
        JsonValueKind.Number => el.TryGetInt64(out var l) ? l : el.GetDouble(),
        _                    => el.GetRawText()
    };
}
```

- [ ] **Step 6: Run the parse tests**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~GzipPayloadTests|FullyQualifiedName~LiveTelemetryBatchTests"`
Expected: PASS (12 tests)

- [ ] **Step 7: Write the failing processor tests**

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests.LivePolling;

public class LiveTelemetryProcessorTests
{
    private readonly InMemoryLiveSessionStore _store = new();
    private readonly LiveTelemetryProcessor _processor;
    private static readonly Guid Session = Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8");
    private static readonly Guid Device  = Guid.Parse("11111111-1111-1111-1111-111111111111");
    private static readonly DateTime T0  = new(2026, 9, 21, 10, 27, 1, DateTimeKind.Utc);

    public LiveTelemetryProcessorTests() =>
        _processor = new LiveTelemetryProcessor(_store, NullLogger<LiveTelemetryProcessor>.Instance);

    private static LiveEntry Baseline(DateTime ts, params (string K, object? V)[] f) =>
        new(ts, f.ToDictionary(x => x.K, x => x.V, StringComparer.OrdinalIgnoreCase), true);

    private static LiveEntry Delta(DateTime ts, params (string K, object? V)[] f) =>
        new(ts, f.ToDictionary(x => x.K, x => x.V, StringComparer.OrdinalIgnoreCase), false);

    private static LiveTelemetryBatch Batch(params LiveEntry[] entries) =>
        new("GW-0042", Session, entries[^1].TimestampUtc, entries, 900);

    [Fact]
    public async Task FirstBatch_FoldsEveryEntryInOrder()
    {
        var result = await _processor.ProcessAsync(Batch(
            Baseline(T0, ("MotorRunning", true), ("OperatingPressurePSI", 54351L)),
            Delta(T0.AddSeconds(1), ("OperatingPressurePSI", 54360L)),
            Delta(T0.AddSeconds(2)),
            Delta(T0.AddSeconds(3), ("StrokeRateLeft", 97L)),
            Delta(T0.AddSeconds(4))), Device, CancellationToken.None);

        result.Should().NotBeNull();
        var current = await _store.GetCurrentAsync(Session, CancellationToken.None);
        current!.Snapshot["OperatingPressurePSI"].Should().Be(54360L);
        current.Snapshot["StrokeRateLeft"].Should().Be(97L);
        current.Snapshot["MotorRunning"].Should().Be(true);
        current.TickCount.Should().Be(5);
    }

    [Fact]
    public async Task EveryBroadcastLeadsWithAFullSnapshotEvenWhenTheGatewaySentOnlyDeltas()
    {
        await _processor.ProcessAsync(Batch(
            Baseline(T0, ("MotorRunning", true), ("OperatingPressurePSI", 54351L))),
            Device, CancellationToken.None);

        var result = await _processor.ProcessAsync(Batch(
            Delta(T0.AddSeconds(5), ("OperatingPressurePSI", 54400L)),
            Delta(T0.AddSeconds(6))), Device, CancellationToken.None);

        result!.Entries[0].Snapshot.Should().NotBeNull();
        result.Entries[0].Snapshot!["MotorRunning"].Should().Be(true, "a field the gateway never resent");
        result.Entries[0].Snapshot!["OperatingPressurePSI"].Should().Be(54400L);
        result.Entries[0].Changes.Should().BeNull();
        result.Entries[1].Changes.Should().NotBeNull();
        result.Entries[1].Snapshot.Should().BeNull();
    }

    [Fact]
    public async Task BroadcastCarriesOneEntryPerTickIncludingHeartbeats()
    {
        var result = await _processor.ProcessAsync(Batch(
            Baseline(T0, ("A", 1L)),
            Delta(T0.AddSeconds(1)),
            Delta(T0.AddSeconds(2), ("A", 2L)),
            Delta(T0.AddSeconds(3)),
            Delta(T0.AddSeconds(4), ("A", 3L))), Device, CancellationToken.None);

        result!.Entries.Should().HaveCount(5);
        result.Entries[1].Changes.Should().BeEmpty();
    }

    [Fact]
    public async Task ChangePointsFromEveryEntryAreWrittenOnce()
    {
        await _processor.ProcessAsync(Batch(
            Baseline(T0, ("A", 1L)),
            Delta(T0.AddSeconds(1), ("A", 2L)),
            Delta(T0.AddSeconds(2), ("A", 3L))), Device, CancellationToken.None);

        _store.RowsFor(Session).Count(r => r.Metric == "A").Should().Be(3);
    }

    [Fact]
    public async Task RedeliveredBatch_AppliesNothing()
    {
        var batch = Batch(Baseline(T0, ("A", 1L)), Delta(T0.AddSeconds(1), ("A", 2L)));
        await _processor.ProcessAsync(batch, Device, CancellationToken.None);
        var rowsBefore = _store.RowsFor(Session).Count;

        var result = await _processor.ProcessAsync(batch, Device, CancellationToken.None);

        result.Should().BeNull("every entry is older than or equal to the stored state");
        _store.RowsFor(Session).Count.Should().Be(rowsBefore);
    }

    [Fact]
    public async Task OverlappingBatch_AppliesOnlyItsNewEntries()
    {
        await _processor.ProcessAsync(Batch(
            Baseline(T0, ("A", 1L)),
            Delta(T0.AddSeconds(1), ("A", 2L))), Device, CancellationToken.None);

        var result = await _processor.ProcessAsync(Batch(
            Delta(T0.AddSeconds(1), ("A", 99L)),
            Delta(T0.AddSeconds(2), ("A", 3L))), Device, CancellationToken.None);

        result!.Entries.Should().ContainSingle("only the genuinely new entry survives the guard");
        var current = await _store.GetCurrentAsync(Session, CancellationToken.None);
        current!.Snapshot["A"].Should().Be(3L);
    }

    [Fact]
    public async Task StatusMovesWithinABatchAndIsRecordedPerEntry()
    {
        var result = await _processor.ProcessAsync(Batch(
            Baseline(T0, ("OperatingWarningActive", 0L)),
            Delta(T0.AddSeconds(1), ("OperatingWarningActive", 1L)),
            Delta(T0.AddSeconds(2), ("OperatingAlarmActive", 2L))), Device, CancellationToken.None);

        result!.Entries[0].DeviceStatus.Should().Be("Normal");
        result.Entries[1].DeviceStatus.Should().Be("Warning");
        result.Entries[2].DeviceStatus.Should().Be("Alarm");
        _store.RowsFor(Session).Count(r => r.Metric == LiveSessionKeys.StatusMetric).Should().Be(3);
    }

    [Fact]
    public async Task BytesTotal_AccumulatesTheWireSizeOfEachBatch()
    {
        await _processor.ProcessAsync(Batch(Baseline(T0, ("A", 1L))), Device, CancellationToken.None);
        await _processor.ProcessAsync(Batch(Delta(T0.AddSeconds(1), ("A", 2L))), Device, CancellationToken.None);

        var current = await _store.GetCurrentAsync(Session, CancellationToken.None);
        current!.BytesTotal.Should().Be(1800);
    }
}
```

- [ ] **Step 8: Run to verify they fail**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveTelemetryProcessorTests"`
Expected: FAIL — `LiveTelemetryProcessor` does not exist.

- [ ] **Step 9: Write the processor**

```csharp
using Microsoft.Extensions.Logging;

namespace ShapeIot.Ingestion.LivePolling;

/// <summary>One tick as the browser receives it. Entry 0 of a batch carries the full snapshot;
/// the rest carry only what changed.</summary>
public sealed record BroadcastEntry(
    DateTime TimestampUtc,
    string DeviceStatus,
    IDictionary<string, object?>? Snapshot,
    IDictionary<string, object?>? Changes);

/// <summary>What to push to the browser after a batch was accepted.</summary>
public sealed record LiveBroadcast(
    Guid SessionId,
    Guid DeviceGuid,
    DateTime BatchSentUtc,
    IReadOnlyList<BroadcastEntry> Entries);

/// <summary>
/// Folds one batch of up to five ticks onto the stored snapshot, writes every change point in a
/// single transaction, and produces the batch the browser replays. Returns null when the whole
/// batch was dropped, in which case nothing is broadcast.
/// </summary>
public sealed class LiveTelemetryProcessor
{
    private readonly ILiveSessionStore _store;
    private readonly ILogger<LiveTelemetryProcessor> _logger;

    public LiveTelemetryProcessor(ILiveSessionStore store, ILogger<LiveTelemetryProcessor> logger)
    {
        _store = store;
        _logger = logger;
    }

    public async Task<LiveBroadcast?> ProcessAsync(LiveTelemetryBatch batch, Guid deviceGuid, CancellationToken ct)
    {
        // One retry: an ETag conflict means another instance folded first, so re-read and reapply.
        for (var attempt = 0; attempt < 2; attempt++)
        {
            var current = await _store.GetCurrentAsync(batch.SessionId, ct);

            var snapshot = current?.Snapshot is null
                ? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                : new Dictionary<string, object?>(current.Snapshot, StringComparer.OrdinalIgnoreCase);
            var status = current?.DeviceStatus ?? DeviceStatusDeriver.Normal;
            var lastStored = current?.LastSnapshotTimestampUtc ?? DateTime.MinValue;

            var points = new List<LiveTickWrite>();
            var broadcast = new List<BroadcastEntry>();
            var accepted = 0;
            DateTime lastAccepted = lastStored;

            foreach (var entry in batch.Entries)
            {
                // Event Grid is unordered and at-least-once. An entry no newer than what is stored
                // is a redelivery or an overtake; folding it would move values backwards.
                if (entry.TimestampUtc <= lastStored) continue;

                snapshot = (Dictionary<string, object?>)SnapshotFolder.Fold(snapshot, entry.Fields);
                var newStatus = DeviceStatusDeriver.Derive(snapshot);

                foreach (var kv in entry.Fields)
                    points.Add(new LiveTickWrite(kv.Key, kv.Value, entry.TimestampUtc));

                if (!string.Equals(newStatus, status, StringComparison.Ordinal) || accepted == 0 && current is null)
                    points.Add(new LiveTickWrite(LiveSessionKeys.StatusMetric, newStatus, entry.TimestampUtc));

                status = newStatus;
                lastAccepted = entry.TimestampUtc;
                accepted++;

                // The head of every broadcast carries the full snapshot, re-materialised — the
                // gateway only ever sends the baseline once, but SignalR does not guarantee
                // delivery, so a client must be able to resynchronise from any single batch.
                broadcast.Add(accepted == 1
                    ? new BroadcastEntry(entry.TimestampUtc, status,
                        new Dictionary<string, object?>(snapshot, StringComparer.OrdinalIgnoreCase), null)
                    : new BroadcastEntry(entry.TimestampUtc, status, null,
                        new Dictionary<string, object?>(entry.Fields, StringComparer.OrdinalIgnoreCase)));
            }

            if (accepted == 0)
            {
                _logger.LogDebug(
                    "Dropped whole batch for session {SessionId}: no entry newer than {Stored:O}.",
                    batch.SessionId, lastStored);
                return null;
            }

            var written = await _store.WriteBatchAsync(
                batch.SessionId,
                deviceGuid,
                snapshot,
                status,
                lastAccepted,
                (current?.TickCount ?? 0) + accepted,
                (current?.BytesTotal ?? 0) + batch.WireBytes,
                current?.FirstTickUtc ?? broadcast[0].TimestampUtc,
                points,
                current?.ETag,
                ct);

            if (written)
                return new LiveBroadcast(batch.SessionId, deviceGuid, batch.BatchSentUtc, broadcast);
        }

        _logger.LogWarning(
            "Batch for session {SessionId} lost the ETag race twice; dropping. The next batch carries state forward.",
            batch.SessionId);
        return null;
    }
}
```

**Transaction size.** A first batch writes up to 55 baseline fields + four deltas' changes + status rows + the head — roughly 65 entities against the 100-entity limit. Step 11 asserts this.

- [ ] **Step 10: Run the processor tests**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveTelemetryProcessorTests"`
Expected: PASS (8 tests)

- [ ] **Step 11: Write the size guards**

```csharp
using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests.LivePolling;

public class LiveBatchPayloadSizeTests
{
    /// <summary>The 55 promoted TelemetryMeasures fields, with representative values.</summary>
    internal static Dictionary<string, object?> FullSnapshot()
    {
        var bits = new[]
        {
            "ActivateDumpValveOnHighLow","BoosterPressureFault","ENCheckvalveTemperature",
            "ENProxSwitchIsNormallyClosed","ENRemoteNetworkControl","ENSealLifeTimeTracking",
            "ENSoftStartFunctionality","HydraulicFilterAssyAlarm","HydraulicFilterAssyWarning",
            "MaintenanceAlarmActive","MaintenanceWarningActive","MotorRunning","OilChangeAlarm",
            "OilChangeWarning","OilFilterAlarm","OilFilterWarning","RemoteModbusIPChanged",
            "TemperatureCheckValveAlarmA","TemperatureCheckValveAlarmB","TemperatureCheckValveWarningA",
            "TemperatureCheckValveWarningB","WaterFilterAssyAlarm","WaterFilterAssyWarning","WaterFilterFault"
        };
        var ints = new[]
        {
            "DumpValveOpenTime","HighPressureSetpointBar","HydraulicOilTemperatureC","HydraulicOilTemperatureF",
            "LowPressureSetpointBar","OperatingAlarmActive","OperatingPressureBar","OperatingWarningActive",
            "PLCSoftwareVersion","ProportionalOutputValue","PumpBrandLabel","PumpCoolingVersion","PumpHP",
            "PumpMotorFrequency","PumpType","RemainingMaintenanceTime","ShutoffTime","StrokeRateLeft",
            "StrokeRateRight","StrokeTimeLeft","StrokeTimeRight"
        };
        var longs = new[]
        {
            "HighPressureSetpointPSI","LowPressureSetpointPSI","OperatingHoursCheckValveAssy_A",
            "OperatingHoursCheckValveAssy_B","OperatingHoursMotor","OperatingHoursTW1",
            "OperatingPressurePSI","PumpSerialNumber","StrokeCounterTW1"
        };

        var d = new Dictionary<string, object?>();
        foreach (var k in bits)  d[k] = true;
        foreach (var k in ints)  d[k] = 4231;
        foreach (var k in longs) d[k] = 54351987L;
        d["IPAddressRemoteStation"] = "192.168.100.14";
        return d;
    }

    [Fact]
    public void SnapshotCarriesEveryPromotedField() => FullSnapshot().Should().HaveCount(55);

    [Fact]
    public void BroadcastBatch_StaysUnderTwoSignalRChunks()
    {
        var t0 = new DateTime(2026, 9, 21, 10, 27, 1, DateTimeKind.Utc);
        var entries = new List<BroadcastEntry>
        {
            new(t0, "Normal", FullSnapshot(), null),
            new(t0.AddSeconds(1), "Normal", null, new Dictionary<string, object?> { ["OperatingPressurePSI"] = 54360L }),
            new(t0.AddSeconds(2), "Normal", null, new Dictionary<string, object?>()),
            new(t0.AddSeconds(3), "Warning", null, new Dictionary<string, object?> { ["StrokeRateLeft"] = 97L, ["OperatingWarningActive"] = 1 }),
            new(t0.AddSeconds(4), "Warning", null, new Dictionary<string, object?>())
        };
        var payload = new LiveBroadcast(
            Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8"),
            Guid.Parse("11111111-1111-1111-1111-111111111111"),
            t0.AddSeconds(4), entries);

        var bytes = JsonSerializer.SerializeToUtf8Bytes(payload).Length;

        bytes.Should().BeLessThan(4096,
            "SignalR bills in 2 KB chunks; crossing 4 KB adds a third chunk to every batch");
    }

    [Fact]
    public async Task FirstBatch_StaysUnderTheEntityGroupTransactionLimit()
    {
        var store = new CountingLiveSessionStore();
        var processor = new LiveTelemetryProcessor(store, NullLogger<LiveTelemetryProcessor>.Instance);
        var t0 = new DateTime(2026, 9, 21, 10, 27, 1, DateTimeKind.Utc);

        var entries = new List<LiveEntry> { new(t0, FullSnapshot(), true) };
        for (var i = 1; i < 5; i++)
            entries.Add(new LiveEntry(t0.AddSeconds(i),
                new Dictionary<string, object?> { ["OperatingPressurePSI"] = 54000L + i, ["StrokeRateLeft"] = 90L + i },
                false));

        await processor.ProcessAsync(
            new LiveTelemetryBatch("GW-0042", Guid.NewGuid(), t0.AddSeconds(4), entries, 2600),
            Guid.NewGuid(), CancellationToken.None);

        // +1 for the head row, which shares the transaction.
        (store.LastChangePointCount + 1).Should().BeLessThan(100,
            "an entity group transaction is capped at 100 entities");
    }
}

/// <summary>Records how many change points one batch asked to write.</summary>
internal sealed class CountingLiveSessionStore : ILiveSessionStore
{
    public int LastChangePointCount { get; private set; }

    public Task<LiveCurrent?> GetCurrentAsync(Guid sessionId, CancellationToken ct) =>
        Task.FromResult<LiveCurrent?>(null);

    public Task<bool> WriteBatchAsync(
        Guid sessionId, Guid deviceGuid, IDictionary<string, object?> snapshot, string deviceStatus,
        DateTime snapshotTimestampUtc, int tickCount, long bytesTotal, DateTime firstTickUtc,
        IReadOnlyList<LiveTickWrite> changePoints, string? expectedETag, CancellationToken ct)
    {
        LastChangePointCount = changePoints.Count;
        return Task.FromResult(true);
    }

    public Task DeleteSessionAsync(Guid sessionId, CancellationToken ct) => Task.CompletedTask;
}
```

If the batch test fails after fields are added, the fix is to shrink the envelope — not to raise the bound.

- [ ] **Step 12: Write the function entry point**

```csharp
using Azure.Messaging;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.SignalRService;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using ShapeIot.Ingestion.Infrastructure;
using ShapeIot.Ingestion.LivePolling;
using ShapeIot.Ingestion.Services;

namespace ShapeIot.Ingestion.Functions;

/// <summary>
/// Event Grid entry point for fast-polling telemetry. Decompresses each batch of five ticks,
/// folds them onto the session's stored snapshot, and broadcasts the batch over SignalR with a
/// full snapshot at its head. Writes nothing to SQL: live session data never reaches TelemetryRaw,
/// TelemetryMeasures, TelemetryEvents or DeviceLatestTelemetry.
/// </summary>
/// <remarks>
/// Fed by an Event Grid subscription filtered to data.properties.telemetry-type == 'fast-polling'.
/// The parser re-checks that property and drops anything else, so a removed or mis-edited filter
/// degrades to dropped events rather than contaminating the telemetry pipeline.
/// </remarks>
public sealed class LiveTelemetryProcessorEventGrid
{
    private readonly LiveTelemetryProcessor _processor;
    private readonly IDeviceResolver _deviceResolver;
    private readonly ISqlConnectionFactory _connectionFactory;
    private readonly ILogger<LiveTelemetryProcessorEventGrid> _logger;

    public LiveTelemetryProcessorEventGrid(
        LiveTelemetryProcessor processor,
        IDeviceResolver deviceResolver,
        ISqlConnectionFactory connectionFactory,
        ILogger<LiveTelemetryProcessorEventGrid> logger)
    {
        _processor = processor;
        _deviceResolver = deviceResolver;
        _connectionFactory = connectionFactory;
        _logger = logger;
    }

    /// <summary>Folds one batch of fast-polling ticks and broadcasts it for replay.</summary>
    /// <remarks>
    /// A batch whose entries are all older than the stored snapshot, or that cannot be decoded as
    /// fast polling, is dropped and produces no SignalR action.
    /// </remarks>
    /// <param name="cloudEvent">CloudEvent delivered by IoT Hub via the filtered Event Grid subscription.</param>
    /// <returns>One SignalR action carrying the replayable batch, or none when it was dropped.</returns>
    [Function("LiveTelemetryProcessorEventGrid")]
    [SignalROutput(HubName = "iot_telemetry", ConnectionStringSetting = "AzureSignalRConnectionString")]
    public async Task<IEnumerable<SignalRMessageAction>> Run([EventGridTrigger] CloudEvent cloudEvent)
    {
        var none = Array.Empty<SignalRMessageAction>();

        var batch = LiveTelemetryBatch.TryParse(cloudEvent);
        if (batch is null)
        {
            _logger.LogWarning(
                "LiveTelemetry: event {Id} is not a decodable fast-polling batch. Check the Event Grid subscription filter.",
                cloudEvent.Id);
            return none;
        }

        Guid deviceGuid;
        using (SqlConnection conn = _connectionFactory.CreateConnection())
        {
            await conn.OpenAsync();
            var device = await _deviceResolver.ResolveAsync(conn, batch.GatewayId);
            if (device is null)
            {
                _logger.LogWarning("LiveTelemetry: no device paired to gateway {GatewayId}.", batch.GatewayId);
                return none;
            }
            deviceGuid = device.DeviceGuid;
        }

        var broadcast = await _processor.ProcessAsync(batch, deviceGuid, CancellationToken.None);
        if (broadcast is null) return none;

        var payload = new
        {
            sessionId    = broadcast.SessionId,
            deviceGuid   = broadcast.DeviceGuid,
            batchSentUtc = broadcast.BatchSentUtc,
            entries      = broadcast.Entries.Select(e => new
            {
                timestampUtc = e.TimestampUtc,
                deviceStatus = e.DeviceStatus,
                snapshot     = e.Snapshot,
                changes      = e.Changes
            })
        };

        return new[]
        {
            new SignalRMessageAction("liveTelemetryUpdate")
            {
                GroupName = $"telemetry:device:{broadcast.DeviceGuid}",
                Arguments = new object[] { payload }
            }
        };
    }
}
```

`IDeviceResolver.ResolveAsync` is the member `EventProcessingService` already uses to map a gateway id to a device. Confirm its exact signature and the property holding `DeviceGuid` before writing this file, and match them.

- [ ] **Step 13: Register the processor**

In `Program.cs`, beside the live session store registration:

```csharp
builder.Services.AddSingleton<LiveTelemetryProcessor>();
```

- [ ] **Step 14: Run the whole suite and build**

Run: `dotnet build && dotnet test shape-iot-function-data-ingestion.Tests`
Expected: build succeeds; all tests pass, including the size and transaction guards.

- [ ] **Step 15: Commit**

```bash
git add LivePolling Functions/LiveTelemetryProcessorEventGrid.cs Program.cs shape-iot-function-data-ingestion.Tests/LivePolling
git commit -m "feat: live telemetry batch processor with gzip decode"
```

---


## Task 11: Cleanup timer

**Repo:** `shape-iot-function-data-ingestion`

**Files:**
- Modify: `shape-iot-function-data-ingestion.csproj`
- Create: `LivePolling/ILiveSessionCleanupRepository.cs`
- Create: `LivePolling/LiveSessionCleanupRepository.cs`
- Create: `LivePolling/LiveSessionCleaner.cs`
- Create: `Functions/LivePollingCleanupTimer.cs`
- Test: `shape-iot-function-data-ingestion.Tests/LivePolling/LiveSessionCleanerTests.cs`
- Modify: `Program.cs`, `local.settings.json`

**Interfaces:**
- Consumes: `ILiveSessionStore` (T9), `ISqlConnectionFactory`
- Produces:
  - `record PurgeCandidate(Guid SessionId)`
  - `ILiveSessionCleanupRepository` with `GetPurgeCandidatesAsync(DateTime nowUtc, int max, CancellationToken ct)` and `MarkPurgedAsync(Guid sessionId, int? tickCount, long? bytesTotal, DateTime purgedUtc, CancellationToken ct)`
  - `LiveSessionCleaner.RunAsync(DateTime nowUtc, CancellationToken ct)` → `record CleanupSummary(int Examined, int Purged, int Failed)`

**The cleaner finds its work in SQL, never by scanning Table Storage.** Table has no secondary indexes, so querying by expiry against the table itself would scan every partition on every run. The `LivePollingSession` row already holds `ExpiresUtc`, so the timer gets an exact list of partition keys.

It also harvests `TickCount` and `BytesTotal` into the SQL row **before** deleting the partition. Those counters are the wireless-cost evidence the acceptance criteria require, and the ephemeral partition is about to disappear.

This covers the two exit paths nothing else reports — auto-expiry and module fault.

- [ ] **Step 1: Add the timer extension package**

`data-ingestion` has no timer trigger today.

```bash
dotnet add shape-iot-function-data-ingestion.csproj package Microsoft.Azure.Functions.Worker.Extensions.Timer --version 4.3.1
```

- [ ] **Step 2: Write the failing cleaner tests**

```csharp
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using ShapeIot.Ingestion.LivePolling;
using Xunit;

namespace ShapeIot.Ingestion.Tests.LivePolling;

public class LiveSessionCleanerTests
{
    private readonly Mock<ILiveSessionCleanupRepository> _repo = new(MockBehavior.Strict);
    private readonly InMemoryLiveSessionStore _store = new();
    private readonly LiveSessionCleaner _cleaner;
    private static readonly DateTime Now = new(2026, 9, 21, 11, 0, 0, DateTimeKind.Utc);
    private static readonly Guid Session = Guid.NewGuid();
    private static readonly Guid Device = Guid.NewGuid();

    public LiveSessionCleanerTests() =>
        _cleaner = new LiveSessionCleaner(_repo.Object, _store, NullLogger<LiveSessionCleaner>.Instance);

    private async Task SeedPartition(int tickCount, long bytes)
    {
        await _store.WriteBatchAsync(Session, Device,
            new Dictionary<string, object?> { ["A"] = 1 }, "Normal", Now.AddMinutes(-5),
            tickCount, bytes, Now.AddMinutes(-15),
            new[] { new LiveTickWrite("A", 1, Now.AddMinutes(-5)) }, null, CancellationToken.None);
    }

    [Fact]
    public async Task FinishedSession_IsPurgedAndItsCountersHarvestedFirst()
    {
        await SeedPartition(tickCount: 412, bytes: 97644);
        _repo.Setup(r => r.GetPurgeCandidatesAsync(Now, It.IsAny<int>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(new[] { new PurgeCandidate(Session) });
        _repo.Setup(r => r.MarkPurgedAsync(Session, 412, 97644L, Now, It.IsAny<CancellationToken>()))
             .Returns(Task.CompletedTask);

        var summary = await _cleaner.RunAsync(Now, CancellationToken.None);

        summary.Purged.Should().Be(1);
        (await _store.GetCurrentAsync(Session, CancellationToken.None)).Should().BeNull();
        _store.RowsFor(Session).Should().BeEmpty();
        _repo.Verify(r => r.MarkPurgedAsync(Session, 412, 97644L, Now, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SessionThatNeverProducedData_IsStillMarkedPurged()
    {
        _repo.Setup(r => r.GetPurgeCandidatesAsync(Now, It.IsAny<int>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(new[] { new PurgeCandidate(Session) });
        _repo.Setup(r => r.MarkPurgedAsync(Session, null, null, Now, It.IsAny<CancellationToken>()))
             .Returns(Task.CompletedTask);

        var summary = await _cleaner.RunAsync(Now, CancellationToken.None);

        summary.Purged.Should().Be(1);
        _repo.Verify(r => r.MarkPurgedAsync(Session, null, null, Now, It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task NothingToDo_IsANoOp()
    {
        _repo.Setup(r => r.GetPurgeCandidatesAsync(Now, It.IsAny<int>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(Array.Empty<PurgeCandidate>());

        var summary = await _cleaner.RunAsync(Now, CancellationToken.None);

        summary.Examined.Should().Be(0);
        summary.Purged.Should().Be(0);
    }

    [Fact]
    public async Task OneFailure_DoesNotStopTheRest()
    {
        var other = Guid.NewGuid();
        await SeedPartition(10, 100);
        _repo.Setup(r => r.GetPurgeCandidatesAsync(Now, It.IsAny<int>(), It.IsAny<CancellationToken>()))
             .ReturnsAsync(new[] { new PurgeCandidate(other), new PurgeCandidate(Session) });
        _repo.Setup(r => r.MarkPurgedAsync(other, null, null, Now, It.IsAny<CancellationToken>()))
             .ThrowsAsync(new InvalidOperationException("boom"));
        _repo.Setup(r => r.MarkPurgedAsync(Session, 10, 100L, Now, It.IsAny<CancellationToken>()))
             .Returns(Task.CompletedTask);

        var summary = await _cleaner.RunAsync(Now, CancellationToken.None);

        summary.Failed.Should().Be(1);
        summary.Purged.Should().Be(1);
    }
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveSessionCleanerTests"`
Expected: FAIL — types do not exist.

- [ ] **Step 4: Write the cleanup repository**

```csharp
namespace ShapeIot.Ingestion.LivePolling;

/// <summary>A session whose storage partition can be deleted.</summary>
public sealed record PurgeCandidate(Guid SessionId);

/// <summary>Reads the finished-session worklist from SQL and records the purge.</summary>
public interface ILiveSessionCleanupRepository
{
    /// <summary>Sessions that are stopped or expired and not yet purged.</summary>
    Task<IReadOnlyList<PurgeCandidate>> GetPurgeCandidatesAsync(DateTime nowUtc, int max, CancellationToken ct);

    /// <summary>Stamps the purge and stores the harvested counters.</summary>
    Task MarkPurgedAsync(Guid sessionId, int? tickCount, long? bytesTotal, DateTime purgedUtc, CancellationToken ct);
}
```

```csharp
using Microsoft.Data.SqlClient;
using ShapeIot.Ingestion.Infrastructure;

namespace ShapeIot.Ingestion.LivePolling;

public sealed class LiveSessionCleanupRepository : ILiveSessionCleanupRepository
{
    private readonly ISqlConnectionFactory _connectionFactory;

    public LiveSessionCleanupRepository(ISqlConnectionFactory connectionFactory)
        => _connectionFactory = connectionFactory;

    public async Task<IReadOnlyList<PurgeCandidate>> GetPurgeCandidatesAsync(DateTime nowUtc, int max, CancellationToken ct)
    {
        const string sql = @"
            SELECT TOP (@Max) SessionId
            FROM dbo.LivePollingSession
            WHERE PurgedUtc IS NULL
              AND (StoppedUtc IS NOT NULL OR ExpiresUtc < @NowUtc)
            ORDER BY ExpiresUtc;";

        using var conn = _connectionFactory.CreateConnection();
        await conn.OpenAsync(ct);
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@Max", max));
        cmd.Parameters.Add(new SqlParameter("@NowUtc", nowUtc));

        var results = new List<PurgeCandidate>();
        using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            results.Add(new PurgeCandidate(reader.GetGuid(0)));
        return results;
    }

    public async Task MarkPurgedAsync(Guid sessionId, int? tickCount, long? bytesTotal, DateTime purgedUtc, CancellationToken ct)
    {
        const string sql = @"
            UPDATE dbo.LivePollingSession
            SET PurgedUtc = @PurgedUtc,
                TickCount = COALESCE(@TickCount, TickCount),
                BytesTotal = COALESCE(@BytesTotal, BytesTotal),
                StopReason = COALESCE(StopReason, 'AutoExpiry')
            WHERE SessionId = @SessionId;";

        using var conn = _connectionFactory.CreateConnection();
        await conn.OpenAsync(ct);
        using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add(new SqlParameter("@PurgedUtc", purgedUtc));
        cmd.Parameters.Add(new SqlParameter("@TickCount", (object?)tickCount ?? DBNull.Value));
        cmd.Parameters.Add(new SqlParameter("@BytesTotal", (object?)bytesTotal ?? DBNull.Value));
        cmd.Parameters.Add(new SqlParameter("@SessionId", sessionId));

        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

`COALESCE(StopReason, 'AutoExpiry')` is how a session that ended without anyone telling the cloud gets its reason recorded: a manual stop already wrote `ManualStop`, so only the silent paths fall through to `AutoExpiry`.

- [ ] **Step 5: Write the cleaner**

```csharp
using Microsoft.Extensions.Logging;

namespace ShapeIot.Ingestion.LivePolling;

/// <summary>What one cleanup pass did.</summary>
public sealed record CleanupSummary(int Examined, int Purged, int Failed);

/// <summary>
/// Deletes the storage partition of every finished session, harvesting its counters into SQL first.
/// The worklist comes from SQL, never from a table scan.
/// </summary>
public sealed class LiveSessionCleaner
{
    private const int MaxPerRun = 50;

    private readonly ILiveSessionCleanupRepository _repository;
    private readonly ILiveSessionStore _store;
    private readonly ILogger<LiveSessionCleaner> _logger;

    public LiveSessionCleaner(
        ILiveSessionCleanupRepository repository,
        ILiveSessionStore store,
        ILogger<LiveSessionCleaner> logger)
    {
        _repository = repository;
        _store = store;
        _logger = logger;
    }

    public async Task<CleanupSummary> RunAsync(DateTime nowUtc, CancellationToken ct)
    {
        var candidates = await _repository.GetPurgeCandidatesAsync(nowUtc, MaxPerRun, ct);
        int purged = 0, failed = 0;

        foreach (var candidate in candidates)
        {
            try
            {
                // Harvest before deleting: these counters are the only surviving record of what
                // the session actually cost on the wire.
                var current = await _store.GetCurrentAsync(candidate.SessionId, ct);
                await _repository.MarkPurgedAsync(
                    candidate.SessionId, current?.TickCount, current?.BytesTotal, nowUtc, ct);

                await _store.DeleteSessionAsync(candidate.SessionId, ct);
                purged++;
            }
            catch (Exception ex)
            {
                // One bad session must not strand the rest; it stays unpurged and is retried next run.
                failed++;
                _logger.LogError(ex, "Failed to purge live session {SessionId}.", candidate.SessionId);
            }
        }

        return new CleanupSummary(candidates.Count, purged, failed);
    }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `dotnet test shape-iot-function-data-ingestion.Tests --filter "FullyQualifiedName~LiveSessionCleanerTests"`
Expected: PASS (4 tests)

- [ ] **Step 7: Write the timer function**

```csharp
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using ShapeIot.Ingestion.LivePolling;

namespace ShapeIot.Ingestion.Functions;

/// <summary>
/// Deletes the Table Storage partition of every finished live polling session, after copying its
/// tick and byte counters into the SQL session row.
/// </summary>
/// <remarks>
/// The worklist is a SQL query over LivePollingSession, not a scan of Table Storage — Table has no
/// secondary indexes, so filtering by expiry against the table itself would read every partition on
/// every run. This is also the only cleanup for sessions that ended by module fault or host shutdown,
/// which are never reported to the cloud.
/// </remarks>
public sealed class LivePollingCleanupTimer
{
    private readonly LiveSessionCleaner _cleaner;
    private readonly TimeProvider _time;
    private readonly ILogger<LivePollingCleanupTimer> _logger;

    public LivePollingCleanupTimer(
        LiveSessionCleaner cleaner,
        TimeProvider time,
        ILogger<LivePollingCleanupTimer> logger)
    {
        _cleaner = cleaner;
        _time = time;
        _logger = logger;
    }

    /// <summary>Runs one cleanup pass.</summary>
    /// <remarks>Schedule comes from the LivePolling__CleanupSchedule app setting; default every 2 minutes.</remarks>
    /// <param name="timer">Timer metadata provided by the Azure Functions runtime.</param>
    /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
    /// <returns>Nothing. Failures are logged per session; a bad session is retried on the next pass.</returns>
    [Function("LivePollingCleanupTimer")]
    public async Task Run(
        [TimerTrigger("%LivePolling__CleanupSchedule%")] TimerInfo timer,
        CancellationToken ct)
    {
        var summary = await _cleaner.RunAsync(_time.GetUtcNow().UtcDateTime, ct);

        if (summary.Examined > 0 || summary.Failed > 0)
            _logger.LogInformation(
                "LivePollingCleanupTimer: examined={Examined} purged={Purged} failed={Failed}",
                summary.Examined, summary.Purged, summary.Failed);
    }
}
```

- [ ] **Step 8: Register and configure**

In `Program.cs`:

```csharp
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<ILiveSessionCleanupRepository, LiveSessionCleanupRepository>();
builder.Services.AddSingleton<LiveSessionCleaner>();
```

In `local.settings.json` under `Values`:

```json
    "LivePolling__CleanupSchedule": "0 */2 * * * *"
```

The same setting must be added to the function app's configuration in Azure. Timer triggers need the Azurite storage emulator running locally.

- [ ] **Step 9: Build and run the full suite**

Run: `dotnet build && dotnet test shape-iot-function-data-ingestion.Tests`
Expected: build succeeds, all tests pass.

- [ ] **Step 10: Commit**

```bash
git add shape-iot-function-data-ingestion.csproj LivePolling Functions/LivePollingCleanupTimer.cs Program.cs local.settings.json shape-iot-function-data-ingestion.Tests/LivePolling
git commit -m "feat: live polling cleanup timer"
```

---
## Task 12: Backend store reader and status endpoint

**Repo:** `shape-iot-backend`

**Files:**
- Modify: `Brilliant.IoT.Infrastructure/Brilliant.IoT.Infrastructure.csproj`
- Create: `Brilliant.IoT.Infrastructure/LivePolling/LiveSessionKeys.cs`
- Create: `Brilliant.IoT.Application/LivePolling/ILiveSessionReader.cs`
- Create: `Brilliant.IoT.Infrastructure/LivePolling/TableLiveSessionReader.cs`
- Modify: `Brilliant.IoT.API/Controllers/LivePollingController.cs`
- Test: `Brilliant.IoT.UnitTests/LivePolling/LiveSessionKeysTests.cs`
- Modify: `Brilliant.IoT.UnitTests/Controllers/LivePollingControllerTests.cs`
- Modify: `Brilliant.IoT.API/Program.cs`

**Interfaces:**
- Consumes: `ILivePollingSessionRepository` (T4), `LivePollingStatusDto` (T5)
- Produces:
  - `Brilliant.IoT.Infrastructure.LivePolling.LiveSessionKeys` — **a verbatim copy of the data-ingestion class from Task 8**
  - `ILiveSessionReader` with `GetCurrentAsync(Guid sessionId, CancellationToken ct)` → `LiveSessionSnapshot?` and `GetSeriesAsync(Guid sessionId, string metric, DateTime? since, CancellationToken ct)` → `IReadOnlyList<LiveSeriesPoint>`
  - `record LiveSessionSnapshot(IDictionary<string, object?> Snapshot, string DeviceStatus, DateTime LastSnapshotTimestampUtc, int TickCount, long BytesTotal, DateTime FirstTickUtc, DateTime LastTickUtc)`
  - `record LiveSeriesPoint(DateTime TimestampUtc, object? Value)`
  - `LivePollingController.GetStatusAsync(string deviceId, CancellationToken ct)`

The backend reads the same partition the function writes. It needs **read only** — never write, never delete. `ShapeIoT.Telemetry.Shared` is vendored into three repos and guarded by `tools/Verify-TelemetryShared.ps1`, so the key class is duplicated here rather than extracted.

- [ ] **Step 1: Add the package**

```bash
dotnet add Brilliant.IoT.Infrastructure/Brilliant.IoT.Infrastructure.csproj package Azure.Data.Tables --version 12.9.1
```

- [ ] **Step 2: Copy the key class and prove the copies agree**

Create `Brilliant.IoT.Infrastructure/LivePolling/LiveSessionKeys.cs` with the identical body written in Task 8 Step 3, changing only the namespace to `Brilliant.IoT.Infrastructure.LivePolling` and the cross-reference comment to point back at `ShapeIot.Ingestion.LivePolling.LiveSessionKeys`.

Create `Brilliant.IoT.UnitTests/LivePolling/LiveSessionKeysTests.cs` with the identical six tests from Task 8 Step 1, changing only the namespace and `using`. If the two copies ever drift, one of the two test suites fails.

- [ ] **Step 3: Write the reader contract**

```csharp
namespace Brilliant.IoT.Application.LivePolling;

/// <summary>The folded snapshot for a live session, as last written by the ingestion function.</summary>
public sealed record LiveSessionSnapshot(
    IDictionary<string, object?> Snapshot,
    string DeviceStatus,
    DateTime LastSnapshotTimestampUtc,
    int TickCount,
    long BytesTotal,
    DateTime FirstTickUtc,
    DateTime LastTickUtc);

/// <summary>One recorded change of one metric.</summary>
public sealed record LiveSeriesPoint(DateTime TimestampUtc, object? Value);

/// <summary>
/// Read-only access to the ephemeral live session store. The backend never writes here —
/// the ingestion function owns every write, and the cleanup timer owns deletion.
/// </summary>
public interface ILiveSessionReader
{
    /// <summary>The session's current folded snapshot, or null when it has no data yet.</summary>
    Task<LiveSessionSnapshot?> GetCurrentAsync(Guid sessionId, CancellationToken ct = default);

    /// <summary>Every recorded change of one metric, oldest first.</summary>
    Task<IReadOnlyList<LiveSeriesPoint>> GetSeriesAsync(
        Guid sessionId, string metric, DateTime? since = null, CancellationToken ct = default);
}
```

- [ ] **Step 4: Implement the reader**

```csharp
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Brilliant.IoT.Application.LivePolling;

namespace Brilliant.IoT.Infrastructure.LivePolling;

/// <summary>Reads the live session partition written by the ingestion function.</summary>
public sealed class TableLiveSessionReader : ILiveSessionReader
{
    public const string TableName = "LivePollingSessions";

    private readonly TableClient _table;

    public TableLiveSessionReader(TableServiceClient service)
        => _table = service.GetTableClient(TableName);

    public async Task<LiveSessionSnapshot?> GetCurrentAsync(Guid sessionId, CancellationToken ct = default)
    {
        try
        {
            var e = await _table.GetEntityAsync<TableEntity>(
                sessionId.ToString(), LiveSessionKeys.CurrentRowKey, cancellationToken: ct);

            var snapshot = JsonSerializer.Deserialize<Dictionary<string, object?>>(
                               e.Value.GetString("Snapshot") ?? "{}")
                           ?? new Dictionary<string, object?>();

            return new LiveSessionSnapshot(
                snapshot,
                e.Value.GetString("DeviceStatus") ?? "Normal",
                e.Value.GetDateTime("LastSnapshotTimestampUtc") ?? DateTime.MinValue,
                e.Value.GetInt32("TickCount") ?? 0,
                e.Value.GetInt64("BytesTotal") ?? 0,
                e.Value.GetDateTime("FirstTickUtc") ?? DateTime.MinValue,
                e.Value.GetDateTime("LastTickUtc") ?? DateTime.MinValue);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task<IReadOnlyList<LiveSeriesPoint>> GetSeriesAsync(
        Guid sessionId, string metric, DateTime? since = null, CancellationToken ct = default)
    {
        var pk = sessionId.ToString();
        var (from, to) = LiveSessionKeys.ParamRange(metric, since);

        // A RowKey range inside one partition — the cheapest read the service offers.
        var filter = TableClient.CreateQueryFilter(
            $"PartitionKey eq {pk} and RowKey ge {from} and RowKey lt {to}");

        var points = new List<LiveSeriesPoint>();
        await foreach (var e in _table.QueryAsync<TableEntity>(filter, cancellationToken: ct))
        {
            var rowKey = e.GetString("RowKey") ?? e.RowKey;
            var sep = rowKey.IndexOf('|');
            if (sep < 0) continue;
            if (!DateTime.TryParse(rowKey[(sep + 1)..], null,
                    System.Globalization.DateTimeStyles.RoundtripKind |
                    System.Globalization.DateTimeStyles.AdjustToUniversal, out var ts))
                continue;

            points.Add(new LiveSeriesPoint(ts, e["V"]));
        }

        points.Sort((a, b) => a.TimestampUtc.CompareTo(b.TimestampUtc));
        return points;
    }
}
```

- [ ] **Step 5: Register it**

In `Program.cs`:

```csharp
builder.Services.AddSingleton(_ =>
{
    var conn = builder.Configuration.GetConnectionString("LivePollingStorage")
               ?? throw new InvalidOperationException(
                   "Live polling storage connection string missing. Expected key: ConnectionStrings:LivePollingStorage");
    var tableOptions = new TableClientOptions();
    tableOptions.Retry.Mode = Azure.Core.RetryMode.Exponential;
    tableOptions.Retry.MaxRetries = 5;
    tableOptions.Retry.Delay = TimeSpan.FromMilliseconds(100);
    tableOptions.Retry.MaxDelay = TimeSpan.FromSeconds(2);
    return new TableServiceClient(conn, tableOptions);
});
builder.Services.AddSingleton<ILiveSessionReader, TableLiveSessionReader>();
```

This must point at the **same storage account** as the function app's `AzureWebJobsStorage`. Add `ConnectionStrings:LivePollingStorage` to `appsettings.Development.json` and to the API's Azure configuration.

- [ ] **Step 6: Write the failing status tests**

Append to `LivePollingControllerTests`:

```csharp
    private readonly Mock<ILiveSessionReader> _reader = new(MockBehavior.Strict);

    [Fact]
    public async Task Status_NoSession_ReturnsInactiveNot404()
    {
        GivenDeviceResolves();
        GivenNoActiveSession();

        var result = await _controller.GetStatusAsync(DeviceId, CancellationToken.None);

        var ok = result.Result.Should().BeOfType<OkObjectResult>().Subject;
        var body = ok.Value.Should().BeOfType<ApiResponse<LivePollingStatusDto>>().Subject;
        body.Data!.Active.Should().BeFalse();
        body.Data.SessionId.Should().BeNull();
    }

    [Fact]
    public async Task Status_ActiveSession_ReturnsSnapshotAndCountdown()
    {
        GivenDeviceResolves();
        _sessions.Setup(s => s.GetActiveForDeviceAsync(DeviceGuid, It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(ActiveSession());
        _reader.Setup(r => r.GetCurrentAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
               .ReturnsAsync(new LiveSessionSnapshot(
                   new Dictionary<string, object?> { ["OperatingPressurePSI"] = 54372 },
                   "Warning",
                   new DateTime(2026, 9, 21, 10, 5, 0, DateTimeKind.Utc),
                   412, 97644,
                   new DateTime(2026, 9, 21, 10, 0, 1, DateTimeKind.Utc),
                   new DateTime(2026, 9, 21, 10, 5, 0, DateTimeKind.Utc)));

        var result = await _controller.GetStatusAsync(DeviceId, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingStatusDto>>().Subject;
        body.Data!.Active.Should().BeTrue();
        body.Data.DeviceStatus.Should().Be("Warning");
        body.Data.Snapshot!["OperatingPressurePSI"].Should().Be(54372);
        body.Data.SecondsRemaining.Should().Be(900);   // 10:00 now, expires 10:15
        body.Data.TotalDurationMinutes.Should().Be(15);
        body.Data.CanExtend.Should().BeTrue();
        body.Data.TickCount.Should().Be(412);
    }

    [Fact]
    public async Task Status_ActiveSessionWithNoTicksYet_ReturnsActiveWithNoSnapshot()
    {
        GivenDeviceResolves();
        _sessions.Setup(s => s.GetActiveForDeviceAsync(DeviceGuid, It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(ActiveSession());
        _reader.Setup(r => r.GetCurrentAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
               .ReturnsAsync((LiveSessionSnapshot?)null);

        var result = await _controller.GetStatusAsync(DeviceId, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingStatusDto>>().Subject;
        body.Data!.Active.Should().BeTrue();
        body.Data.Snapshot.Should().BeNull();
    }
```

Add `_reader.Object` to the controller construction in the test fixture's constructor.

- [ ] **Step 7: Run to verify they fail**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePollingControllerTests"`
Expected: FAIL — `GetStatusAsync` does not exist and the constructor has no reader parameter.

- [ ] **Step 8: Add the reader to the controller and implement status**

Add `ILiveSessionReader _reader` as a constructor parameter, then:

```csharp
    /// <summary>Returns the device's live session state and its most recent values.</summary>
    /// <remarks>
    /// This is what a page reload calls. SignalR only delivers what happens next, so without this a
    /// browser that reloads mid-session would show nothing until the next tick — and a field that
    /// never changes is never resent at all.
    /// An inactive device is a 200 with <c>active: false</c>, not a 404.
    /// </remarks>
    /// <param name="deviceId">Device to report on.</param>
    /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
    /// <returns>
    /// <list type="bullet">
    /// <item>200 OK — session state; when active, also the current snapshot and tick counters.</item>
    /// <item>404 Not Found — device unknown or not paired to a gateway.</item>
    /// </list>
    /// </returns>
    [HttpGet]
    [SwaggerOperation(Summary = "Get live polling status and the current snapshot")]
    [Produces("application/json")]
    [ProducesResponseType(typeof(ApiResponse<LivePollingStatusDto>), (int)HttpStatusCode.OK)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.NotFound)]
    public async Task<ActionResult<LivePollingStatusDto>> GetStatusAsync(string deviceId, CancellationToken ct)
    {
        var device = await _devices.GetGatewayRefAsync(deviceId, ct);
        if (device is null)
            return Fail(HttpStatusCode.NotFound, LivePollingErrorCodes.DeviceNotFound,
                        $"Device '{deviceId}' was not found or has no active gateway pairing.");

        var now = _time.GetUtcNow().UtcDateTime;
        var session = await _sessions.GetActiveForDeviceAsync(device.DeviceGuid, now, ct);

        if (session is null)
            return Ok(ApiResponse<LivePollingStatusDto>.Success(new LivePollingStatusDto { Active = false }));

        var current = await _reader.GetCurrentAsync(session.SessionId, ct);

        return Ok(ApiResponse<LivePollingStatusDto>.Success(new LivePollingStatusDto
        {
            Active               = true,
            SessionId            = session.SessionId,
            PollIntervalSeconds  = session.PollIntervalSeconds,
            ExpiresUtc           = session.ExpiresUtc,
            SecondsRemaining     = (int)Math.Max(0, (session.ExpiresUtc - now).TotalSeconds),
            TotalDurationMinutes = session.SessionDurationMinutes,
            CanExtend            = session.SessionDurationMinutes < LivePollingParameters.MaxDurationMinutes,
            DeviceStatus         = current?.DeviceStatus,
            Snapshot             = current?.Snapshot,
            SnapshotTimestampUtc = current?.LastSnapshotTimestampUtc,
            TickCount            = current?.TickCount,
            BytesTotal           = current?.BytesTotal
        }));
    }
```

- [ ] **Step 9: Run tests to verify they pass**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePolling"`
Expected: PASS — 15 controller tests plus the 6 duplicated key tests.

- [ ] **Step 10: Commit**

```bash
git add Brilliant.IoT.Infrastructure Brilliant.IoT.Application/LivePolling Brilliant.IoT.API Brilliant.IoT.UnitTests
git commit -m "feat: live polling status endpoint and table reader"
```

---
## Task 13: Series endpoint

**Repo:** `shape-iot-backend`

**Files:**
- Create: `Brilliant.IoT.Application/LivePolling/SeriesStepExpander.cs`
- Test: `Brilliant.IoT.UnitTests/LivePolling/SeriesStepExpanderTests.cs`
- Modify: `Brilliant.IoT.API/Controllers/LivePollingController.cs`
- Modify: `Brilliant.IoT.UnitTests/Controllers/LivePollingControllerTests.cs`

**Interfaces:**
- Consumes: `ILiveSessionReader` (T12), `LivePollingSeriesDto` / `LivePollingSeriesMetricDto` (T5), `LivePollingOptions.DefaultSeriesMetrics` (T3)
- Produces:
  - `static IReadOnlyList<DateTime> SeriesStepExpander.BuildGrid(DateTime startUtc, DateTime endUtc, int intervalSeconds)`
  - `static IReadOnlyList<object?> SeriesStepExpander.Expand(IReadOnlyList<DateTime> grid, IReadOnlyList<LiveSeriesPoint> points)`
  - `LivePollingController.GetSeriesAsync(string deviceId, string? metric, DateTime? since, CancellationToken ct)`

Storage records **changes**; the graph needs **every second**. Step expansion is the bridge: a value carries forward until the next recorded change, so the line is evenly spaced with no gaps even though a heartbeat tick wrote no rows at all.

- [ ] **Step 1: Write the failing expander tests**

```csharp
using Brilliant.IoT.Application.LivePolling;
using FluentAssertions;
using Xunit;

namespace Brilliant.IoT.UnitTests.LivePolling;

public class SeriesStepExpanderTests
{
    private static readonly DateTime T0 = new(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void BuildGrid_IsEvenlySpacedAndInclusiveOfBothEnds()
    {
        var grid = SeriesStepExpander.BuildGrid(T0, T0.AddSeconds(3), 1);

        grid.Should().HaveCount(4);
        grid[0].Should().Be(T0);
        grid[3].Should().Be(T0.AddSeconds(3));
    }

    [Fact]
    public void Expand_CarriesAValueForwardAcrossTicksThatRecordedNothing()
    {
        var grid = SeriesStepExpander.BuildGrid(T0, T0.AddSeconds(3), 1);
        var points = new List<LiveSeriesPoint>
        {
            new(T0, 54351),
            new(T0.AddSeconds(2), 54360)
        };

        var values = SeriesStepExpander.Expand(grid, points);

        values.Should().Equal(54351, 54351, 54360, 54360);
    }

    [Fact]
    public void Expand_BeforeTheFirstChange_IsNull()
    {
        var grid = SeriesStepExpander.BuildGrid(T0, T0.AddSeconds(2), 1);
        var points = new List<LiveSeriesPoint> { new(T0.AddSeconds(1), 7) };

        var values = SeriesStepExpander.Expand(grid, points);

        values[0].Should().BeNull();
        values[1].Should().Be(7);
        values[2].Should().Be(7);
    }

    [Fact]
    public void Expand_ChangeBetweenGridPoints_TakesEffectAtTheNextPoint()
    {
        var grid = SeriesStepExpander.BuildGrid(T0, T0.AddSeconds(2), 1);
        var points = new List<LiveSeriesPoint> { new(T0.AddMilliseconds(1500), 9) };

        var values = SeriesStepExpander.Expand(grid, points);

        values.Should().Equal(null, null, 9);
    }

    [Fact]
    public void Expand_NoPoints_IsAllNullsNotAnEmptyArray()
    {
        var grid = SeriesStepExpander.BuildGrid(T0, T0.AddSeconds(2), 1);

        var values = SeriesStepExpander.Expand(grid, Array.Empty<LiveSeriesPoint>());

        values.Should().HaveCount(3).And.AllSatisfy(v => v.Should().BeNull());
    }

    [Fact]
    public void BuildGrid_ZeroOrNegativeInterval_IsTreatedAsOneSecond()
    {
        SeriesStepExpander.BuildGrid(T0, T0.AddSeconds(2), 0).Should().HaveCount(3);
    }
}
```

The fourth test pins down an ambiguity: a change recorded between two grid points belongs to the **later** point. Attributing it to the earlier one would show a value before it existed.

- [ ] **Step 2: Run to verify they fail**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~SeriesStepExpanderTests"`
Expected: FAIL — `SeriesStepExpander` does not exist.

- [ ] **Step 3: Write the expander**

```csharp
namespace Brilliant.IoT.Application.LivePolling;

/// <summary>
/// Turns recorded change points into an evenly spaced series. Storage holds only the moments a
/// value moved; a graph needs a point per tick, so each value carries forward until the next change.
/// </summary>
public static class SeriesStepExpander
{
    /// <summary>The tick grid, inclusive of both ends.</summary>
    public static IReadOnlyList<DateTime> BuildGrid(DateTime startUtc, DateTime endUtc, int intervalSeconds)
    {
        if (intervalSeconds <= 0) intervalSeconds = 1;
        if (endUtc < startUtc) endUtc = startUtc;

        var step = TimeSpan.FromSeconds(intervalSeconds);
        var grid = new List<DateTime>();
        for (var t = startUtc; t <= endUtc; t = t.Add(step))
            grid.Add(t);
        return grid;
    }

    /// <summary>
    /// Projects change points onto the grid. A grid point takes the most recent change at or before
    /// it, and null until the first change — a value that has not been reported yet is genuinely
    /// unknown, not zero.
    /// </summary>
    public static IReadOnlyList<object?> Expand(
        IReadOnlyList<DateTime> grid,
        IReadOnlyList<LiveSeriesPoint> points)
    {
        var values = new object?[grid.Count];
        var index = 0;
        object? carried = null;

        for (var g = 0; g < grid.Count; g++)
        {
            while (index < points.Count && points[index].TimestampUtc <= grid[g])
            {
                carried = points[index].Value;
                index++;
            }
            values[g] = carried;
        }

        return values;
    }
}
```

- [ ] **Step 4: Run to verify they pass**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~SeriesStepExpanderTests"`
Expected: PASS (6 tests)

- [ ] **Step 5: Write the failing controller tests**

Append to `LivePollingControllerTests`:

```csharp
    [Fact]
    public async Task Series_NoMetricRequested_ReturnsTheConfiguredDefaults()
    {
        GivenDeviceResolves();
        _sessions.Setup(s => s.GetActiveForDeviceAsync(DeviceGuid, It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(ActiveSession());
        _reader.Setup(r => r.GetCurrentAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
               .ReturnsAsync(new LiveSessionSnapshot(
                   new Dictionary<string, object?>(), "Normal",
                   new DateTime(2026, 9, 21, 10, 0, 2, DateTimeKind.Utc), 3, 600,
                   new DateTime(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc),
                   new DateTime(2026, 9, 21, 10, 0, 2, DateTimeKind.Utc)));
        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "DeviceStatus", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint> { new(new DateTime(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc), "Normal") });
        foreach (var m in new[] { "OperatingPressurePSI", "StrokeRateLeft" })
            _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), m, null, It.IsAny<CancellationToken>()))
                   .ReturnsAsync(new List<LiveSeriesPoint> { new(new DateTime(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc), 1) });

        var result = await _controller.GetSeriesAsync(DeviceId, null, null, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingSeriesDto>>().Subject;
        body.Data!.Series.Should().HaveCount(2);
        body.Data.T.Should().HaveCount(3);
        body.Data.Status.Should().HaveCount(3);
        body.Data.Series[0].V.Should().HaveCount(3, "every metric is expanded onto the same grid");
    }

    [Fact]
    public async Task Series_NamedMetric_ReturnsOnlyThatOne()
    {
        GivenDeviceResolves();
        _sessions.Setup(s => s.GetActiveForDeviceAsync(DeviceGuid, It.IsAny<DateTime>(), It.IsAny<CancellationToken>()))
                 .ReturnsAsync(ActiveSession());
        _reader.Setup(r => r.GetCurrentAsync(It.IsAny<Guid>(), It.IsAny<CancellationToken>()))
               .ReturnsAsync(new LiveSessionSnapshot(
                   new Dictionary<string, object?>(), "Normal",
                   new DateTime(2026, 9, 21, 10, 0, 1, DateTimeKind.Utc), 2, 400,
                   new DateTime(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc),
                   new DateTime(2026, 9, 21, 10, 0, 1, DateTimeKind.Utc)));
        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "DeviceStatus", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(Array.Empty<LiveSeriesPoint>());
        _reader.Setup(r => r.GetSeriesAsync(It.IsAny<Guid>(), "OperatingPressurePSI", null, It.IsAny<CancellationToken>()))
               .ReturnsAsync(new List<LiveSeriesPoint>
               {
                   new(new DateTime(2026, 9, 21, 10, 0, 0, DateTimeKind.Utc), 54351),
                   new(new DateTime(2026, 9, 21, 10, 0, 1, DateTimeKind.Utc), 54360)
               });

        var result = await _controller.GetSeriesAsync(DeviceId, "OperatingPressurePSI", null, CancellationToken.None);

        var body = result.Result.Should().BeOfType<OkObjectResult>()
                         .Which.Value.Should().BeOfType<ApiResponse<LivePollingSeriesDto>>().Subject;
        body.Data!.Series.Should().ContainSingle()
            .Which.V.Should().Equal(54351, 54360);
    }

    [Fact]
    public async Task Series_NoActiveSession_Returns409()
    {
        GivenDeviceResolves();
        GivenNoActiveSession();

        var result = await _controller.GetSeriesAsync(DeviceId, null, null, CancellationToken.None);

        result.Result.Should().BeOfType<ConflictObjectResult>()
              .Which.Value.Should().BeOfType<ApiResponse<object>>()
              .Which.Errors.Should().Contain(LivePollingErrorCodes.NotActive);
    }
```

- [ ] **Step 6: Run to verify they fail**

Run: `dotnet test Brilliant.IoT.UnitTests --filter "FullyQualifiedName~LivePollingControllerTests"`
Expected: FAIL — `GetSeriesAsync` does not exist.

- [ ] **Step 7: Implement the series action**

```csharp
    /// <summary>Returns per-metric time series for the running session.</summary>
    /// <remarks>
    /// With no <c>metric</c>, returns the configured default set. With one, returns only that metric.
    /// Timestamps and device status are shared across every metric rather than repeated inside each,
    /// which is most of the difference between a ~170 KB and a ~116 KB response.
    /// Storage holds only the moments a value changed; values are carried forward onto the tick grid
    /// so the series is evenly spaced.
    /// Sample: <c>GET /api/v1/devices/PUMP-00417/live-session/series?metric=OperatingPressurePSI</c>
    /// </remarks>
    /// <param name="deviceId">Device whose session is being read.</param>
    /// <param name="metric">Optional single metric name. Omit for the configured default set.</param>
    /// <param name="since">Optional lower bound; returns only changes at or after this timestamp.</param>
    /// <param name="ct">Cancellation token for cooperative request cancellation.</param>
    /// <returns>
    /// <list type="bullet">
    /// <item>200 OK — shared timestamp and status arrays plus one value array per metric.</item>
    /// <item>404 Not Found — device unknown or not paired to a gateway.</item>
    /// <item>409 Conflict — no active session (LIVE_SESSION_NOT_ACTIVE). Series are not retained after a session ends.</item>
    /// </list>
    /// </returns>
    [HttpGet("series")]
    [SwaggerOperation(Summary = "Get live polling metric series")]
    [Produces("application/json")]
    [ProducesResponseType(typeof(ApiResponse<LivePollingSeriesDto>), (int)HttpStatusCode.OK)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.NotFound)]
    [ProducesResponseType(typeof(ApiResponse<object>), (int)HttpStatusCode.Conflict)]
    public async Task<ActionResult<LivePollingSeriesDto>> GetSeriesAsync(
        string deviceId,
        [FromQuery] string? metric,
        [FromQuery] DateTime? since,
        CancellationToken ct)
    {
        var device = await _devices.GetGatewayRefAsync(deviceId, ct);
        if (device is null)
            return Fail(HttpStatusCode.NotFound, LivePollingErrorCodes.DeviceNotFound,
                        $"Device '{deviceId}' was not found or has no active gateway pairing.");

        var now = _time.GetUtcNow().UtcDateTime;
        var session = await _sessions.GetActiveForDeviceAsync(device.DeviceGuid, now, ct);
        if (session is null)
            return Fail(HttpStatusCode.Conflict, LivePollingErrorCodes.NotActive,
                        "No live session is active on this device. Series are not retained after a session ends.");

        var metrics = metric is null
            ? _options.DefaultSeriesMetrics
            : new[] { metric };

        var current = await _reader.GetCurrentAsync(session.SessionId, ct);
        var first = current?.FirstTickUtc ?? session.StartedUtc;
        var last  = current?.LastTickUtc  ?? session.StartedUtc;

        var grid = SeriesStepExpander.BuildGrid(first, last, session.PollIntervalSeconds);

        var statusPoints = await _reader.GetSeriesAsync(
            session.SessionId, Infrastructure.LivePolling.LiveSessionKeys.StatusMetric, since, ct);
        var status = SeriesStepExpander.Expand(grid, statusPoints)
                                       .Select(v => v as string ?? "Normal")
                                       .ToList();

        var series = new List<LivePollingSeriesMetricDto>(metrics.Length);
        foreach (var name in metrics)
        {
            var points = await _reader.GetSeriesAsync(session.SessionId, name, since, ct);
            series.Add(new LivePollingSeriesMetricDto
            {
                Metric = name,
                V = SeriesStepExpander.Expand(grid, points)
            });
        }

        return Ok(ApiResponse<LivePollingSeriesDto>.Success(new LivePollingSeriesDto
        {
            SessionId           = session.SessionId,
            PollIntervalSeconds = session.PollIntervalSeconds,
            StartedUtc          = session.StartedUtc,
            LastTickUtc         = current?.LastTickUtc,
            TickCount           = current?.TickCount ?? 0,
            T                   = grid,
            Status              = status,
            Series              = series
        }));
    }
```

`Infrastructure.LivePolling.LiveSessionKeys.StatusMetric` requires the API project to reference Infrastructure, which it already does. If that direction is unwanted, move the `StatusMetric` constant into `Brilliant.IoT.Application.LivePolling` and have both key classes reference it.

- [ ] **Step 8: Run the whole backend suite**

Run: `dotnet build && dotnet test Brilliant.IoT.UnitTests && dotnet test Brilliant.IoT.IntegrationTests`
Expected: build succeeds, all tests pass.

- [ ] **Step 9: Commit**

```bash
git add Brilliant.IoT.Application/LivePolling Brilliant.IoT.API/Controllers/LivePollingController.cs Brilliant.IoT.UnitTests
git commit -m "feat: live polling series endpoint"
```

---

## Task 14: Azure configuration and dev-gateway verification

**Files:** none — this is Azure portal configuration plus a verification runbook. Record results in the spec's open-items section.

**Interfaces:**
- Consumes: everything above, deployed to dev
- Produces: a working end-to-end path, plus the measurements the acceptance criteria require

**The order below is load-bearing.** Enabling fast polling before the existing subscription is filtered puts live data into `TelemetryRaw`, and through `RollupWatermark` that corrupts `SampleCount`, `TransitionCount` and Min/Max in `TelemetryDailyRollup` — not merely extra rows.

- [ ] **Step 1: Filter the existing subscription**

On the IoT Hub's Event Grid **system topic**, edit the subscription feeding `IotTelemetryProcessorEventGrid`. Add an advanced filter:

```
Key:      data.properties.telemetry-type
Operator: String does not contain any of      (StringNotContains / StringNotIn)
Value:    fast-polling
```

- [ ] **Step 2: Verify regular telemetry still flows — before anything else**

Regular telemetry carries **no** `telemetry-type` property at all, and Event Grid's treatment of a missing key in a negative filter is the single most dangerous unknown in this design (spec open item 1). If a missing key does not match, this filter silently stops **all** normal telemetry.

Watch Application Insights for `IotTelemetryProcessorEventGrid` for 10 minutes and confirm invocations continue at the normal rate. If they stop, remove the filter immediately and switch to a positive filter on the new subscription only, plus the in-code guard in the parser.

Record the answer in the spec's open item 1.

- [ ] **Step 3: Create the new subscription**

New event subscription on the same system topic → endpoint: the `LiveTelemetryProcessorEventGrid` function. Advanced filter:

```
Key:      data.properties.telemetry-type
Operator: String is in
Value:    fast-polling
```

Confirm the hyphenated key resolves (spec open item 2) by checking the function receives anything at all in step 6.

- [ ] **Step 4: Application settings**

On the **function app**: `LivePolling__CleanupSchedule`. `AzureWebJobsStorage` and `AzureSignalRConnectionString` already exist.

On the **API**: the whole `LivePolling` section, plus `ConnectionStrings__LivePollingStorage` pointing at the **same storage account** as the function app's `AzureWebJobsStorage`.

- [ ] **Step 4b: Confirm how Event Grid presents the gzip body**

Spec open item 5, and the one thing that can make the function receive events it cannot read.
Capture a real fast-polling event in Application Insights for `LiveTelemetryProcessorEventGrid`
and check:

- is `data.body` a base64 string, or an object?
- does `ContentEncoding: "gzip"` appear in `data.properties`, `data.systemProperties`, or nowhere?

`GzipPayload.TryDecode` handles base64-gzip, base64-plain and raw JSON, so it should cope with any
of these — but confirm rather than assume, and record the actual shape in the spec.

- [ ] **Step 5: Confirm the module id**

Spec open item 4. `LivePolling:ModuleId` is currently a placeholder. Find the module hosting `FastPollingService` on a dev gateway — existing calls target `WatchdogModule`, which is a **different** module. Check the device's modules in IoT Hub, or ask the IoT team. Update the setting.

- [ ] **Step 6: Run one real session against a dev gateway**

The module does not register its direct methods in `Development`, so this cannot be done locally.

```
POST /api/v1/devices/{deviceId}/live-session
GET  /api/v1/devices/{deviceId}/live-session          -> snapshot populates within ~1s
GET  /api/v1/devices/{deviceId}/live-session/series   -> evenly spaced points
POST /api/v1/devices/{deviceId}/live-session/extend   -> expiry moves
DELETE /api/v1/devices/{deviceId}/live-session        -> stops
```

Confirm the browser receives `liveTelemetryUpdate` on `telemetry:device:{deviceGuid}` after joining via the negotiator's `join-device`.

- [ ] **Step 7: Prove nothing was persisted**

Immediately after the session, against the dev database (read-only):

```sql
SELECT COUNT(*) FROM dbo.TelemetryRaw
 WHERE DeviceGuid = @DeviceGuid AND CreatedAt >= @SessionStart;

SELECT COUNT(*) FROM dbo.TelemetryMeasures
 WHERE DeviceGuid = @DeviceGuid AND TimestampUtc >= @SessionStart;

SELECT COUNT(*) FROM dbo.TelemetryEvents
 WHERE DeviceGuid = @DeviceGuid AND EventTimestamp >= @SessionStart;

SELECT UpdatedAtUtc FROM dbo.DeviceLatestTelemetry WHERE DeviceGuid = @DeviceGuid;
```

The first three must show only the normal 10-minute cadence — at most one row for a 10-minute session, never ~600. `DeviceLatestTelemetry.UpdatedAtUtc` must not have moved to a live tick's timestamp.

- [ ] **Step 8: Prove regular sampling was undisturbed**

```sql
SELECT TOP 20 TimestampUtc,
       DATEDIFF(SECOND, LAG(TimestampUtc) OVER (ORDER BY TimestampUtc), TimestampUtc) AS GapSeconds
FROM dbo.TelemetryRaw
WHERE DeviceGuid = @DeviceGuid
  AND TimestampUtc >= DATEADD(HOUR, -2, SYSUTCDATETIME())
ORDER BY TimestampUtc DESC;
```

Gaps must stay at the normal interval across the live session — no compression, no missing sample. This is the acceptance criterion the whole Event Grid split exists to satisfy.

- [ ] **Step 9: Measure the wireless cost and the real parameter volatility**

```
GET /api/v1/devices/{deviceId}/live-session
```

before stopping, and read `tickCount` and `bytesTotal`. After the cleanup timer runs, the same figures are in `LivePollingSession`.

Record:
- bytes per session and the implied bytes/hour at this cadence — the ticket's wireless-cost criterion.
  **The gateway gzips each batch**, so the figure that matters for a cellular plan is the compressed
  size on the wire, not the JSON length
- **average parameters changing per tick** = (rows written − 55 baseline − status rows) ÷ ticks. The
  plan assumes 4 of 55; every cost, row-count and transaction-size figure scales off it (spec open item 3)
- **entities per first-batch transaction**, to confirm the ~65 estimate stays under the 100 limit

Also capture the storage transaction count for the session:

```bash
az monitor metrics list \
  --resource "/subscriptions/<sub>/resourceGroups/<rg>/providers/Microsoft.Storage/storageAccounts/<acct>/tableServices/default" \
  --metric Transactions --interval PT1M --aggregation Total
```

- [ ] **Step 10: Verify auto-shutoff end to end**

Start a session, do not stop it, and confirm at the configured duration that ticks stop arriving, `GET` reports `active: false`, and the cleanup timer purges the partition within its schedule. The module's 10-minute floor means this cannot be shortened (spec open item 9); the backend-side expiry logic is already covered by `TimeProvider` in unit tests.

- [ ] **Step 9b: Confirm the frontend replay**

Watch one batch arrive on `liveTelemetryUpdate` and confirm it has five entries, that entry 0
carries a full `snapshot` and entries 1-4 carry `changes`, and that the serialised message is under
4 KB. Then confirm a mid-session page reload renders correctly from `GET /live-session` plus the
next batch alone — no gap, no missing fields.

- [ ] **Step 11: Update the spec's open items**

Mark items 1, 2, 3, 4, 5, 7 and 8 resolved with the measured values. Role names and the extend contract were already resolved during planning. Item 6 (entity-group-transaction billing granularity) is informational only and changes no decision.

---

## Plan self-review

**Spec coverage.** Every section maps to a task: §5 storage → T8, T9, T12; §5.5 cleanup → T11; §6 lifecycle → T6, T7; §7 API → T6, T7, T12, T13; §8 SignalR → T10; §9 session record → T1, T4; §10 RBAC → T1; §11 configuration → T3; §12 Event Grid → T14; §13 cost → measured in T14 step 9; §14 acceptance criteria → T14 steps 7-10; §15 testing → tests throughout; §16 open items → T14 step 11.

**Known gaps, stated rather than hidden.**

1. **`IDeviceRepository.GetGatewayRefAsync` may not exist.** Task 6 says to confirm it and, if absent, add it against `DeviceGatewayPairing` in the same commit. This is the one interface in the plan not verified against the code.
2. **`IDeviceResolver.ResolveAsync`'s exact signature** is assumed in Task 10 from its use in `EventProcessingService`. Confirm before writing that file.
3. **`Extend-FastPolling` is documented but unexercised.** Task 7 is written against the 2026-09-25 gateway contract; it has not been called against a real module yet.
4. **`LiveSessionKeys` is duplicated** between the two repos by deliberate choice — `ShapeIoT.Telemetry.Shared` is vendored three times and parity-checked by `tools/Verify-TelemetryShared.ps1`. Both copies carry the same six tests, so drift fails a build.

**Type consistency.** `DirectMethodResult`, `ParameterResolution`, `LivePollingOptions`, `ILivePollingSessionRepository`, `LiveCurrent`, `LiveTickWrite`, `ILiveSessionStore`, `GzipPayload`, `LiveEntry`, `LiveTelemetryBatch`, `BroadcastEntry`, `LiveBroadcast`, `ILiveSessionReader`, `LiveSessionSnapshot`, `LiveSeriesPoint`, `PurgeCandidate`, `CleanupSummary` and `LiveSessionKeys` are each defined in exactly one task and referenced with the same names and signatures afterwards.

**Parameter ranges** (1-60, 10-30), the **4,096-byte SignalR batch bound**, the **100-entity EGT limit** and the **retry-on-503 requirement** appear identically wherever they are used.
