# Live Polling Validation Harness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build local stand-ins for the live polling gateway module, ingestion function and backend API — plus a validation frontend — that behave exactly as the reference contracts say, so the backend design can be validated before it is handed to the frontend team.

**Architecture:** Four deployables mirror the real repos: `Shape.Gateway.Simulator` (module contract over an IoT Hub direct-method facade, gzip 5-tick batches delivered as Event Grid events), `Shape.LiveIngestion` (Functions isolated: fold into Azurite Table, resolve events, gzip SignalR broadcast, negotiator, cleanup timer), `Shape.LiveBackend` (ASP.NET Core REST API over SQLite + Table), and `web/` (React validation UI). `LiveSessionKeys` is duplicated between ingestion and backend, and a seam test project checks the writer against the reader.

**Tech Stack:** .NET 8, ASP.NET Core 8, Azure Functions isolated worker (v4 host, Core Tools 4.9), `Azure.Data.Tables`, `Microsoft.Data.Sqlite`, Azure SignalR Emulator, Azurite, xUnit, `Microsoft.Extensions.TimeProvider.Testing`, `Microsoft.AspNetCore.Mvc.Testing`, React + Vite + TypeScript, Recharts, `@microsoft/signalr`, Vitest.

**Spec:** `docs/superpowers/specs/2026-10-01-live-polling-validation-harness-design.md` (reference contracts in `docs/reference/`)

## Global Constraints

- Branch `feat/live-polling-harness`. Commit messages: Conventional Commits, single title line, **no `Co-Authored-By`, no Claude attribution** (user rule).
- Every .NET project targets `net8.0`; `global.json` pins SDK `8.0.425`, `rollForward: latestFeature`.
- Ports: simulator `5100`, backend `5080`, function host `7071`, SignalR emulator `8888`, Azurite `10100`/`10101`/`10102`, Vite `5173`.
- Azurite connection string (everywhere a storage connection is needed):
  `DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;`
- SignalR emulator connection string: `Endpoint=http://localhost;Port=8888;AccessKey=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGH;Version=1.0;`
- Functions packages pinned: `Microsoft.Azure.Functions.Worker` 2.52.0, `Microsoft.Azure.Functions.Worker.Sdk` 2.1.0, `…Extensions.EventGrid` 3.6.0, `…Extensions.SignalRService` **1.15.0**, `…Extensions.Http` 3.3.0, `…Extensions.Timer` **4.3.1**. Newer SignalR/Timer extensions need Microsoft.Extensions 10 which the Core Tools 4.9 host cannot load ("Could not load … Version=10.0.0.0").
- Device (one): serial `PUMP-00417`, DeviceGuid `9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284`, name `PREC-NEO30-MARYL-07`, gateway id `gw-001`, module id `DefaultModeModule`.
- Module ranges: `sessionDurationMinutes` 10–30 default 10; `pollIntervalSeconds` 1–60 default 1; total duration cap 30 min from the original start; batch size 5.
- Wire names verbatim — gateway batch: `Entries`, `TimestampUtc`, `Body`, `Changes`, `Event`; properties `batchSentUtc`, `session-id`, `telemetry-type` = `fast-polling`; system property `iothub-connection-device-id`, `iothub-content-encoding` = `gzip`.
- SignalR: hub `iot_telemetry`, group `telemetry:device:{deviceGuid}`, target `liveTelemetryUpdate`, envelope `{ "encoding": "gzip+base64", "payload": "..." }`; decoded batch `{ sessionId, deviceGuid, batchSentUtc, entries[ timestampUtc, deviceStatus, snapshot, changes, events ] }` — **every entry key always present, explicit `null`**.
- REST: base `/api/v1/devices/{serialNumber}/live-session`, camelCase, ISO-8601 UTC with `Z`, envelope `{ succeeded, message, errors, data }`, `errors` omitted when null, `errors[0]` = code.
- Error codes exactly: `LIVE_SESSION_ALREADY_ACTIVE`, `LIVE_SESSION_CAPACITY_REACHED`, `LIVE_SESSION_NOT_ACTIVE`, `LIVE_SESSION_EXTENSION_LIMIT_REACHED`, `LIVE_SESSION_INVALID_PARAMETERS`, `DEVICE_NOT_FOUND`, `GATEWAY_NOT_PAIRED`, `GATEWAY_UNREACHABLE`.
- Device status integer: `0` Normal, `1` Warning, `2` Alarm. Event states exactly `triggered`, `cleared`, `active`; unknown metadata exactly `unknown`.
- Table: name `livesessions`, PK = session id, head RK `!current`, change RK `{param}|{ts:O}`, event RK `evt|{ts:O}|{param}`; entity-group transaction ≤ 100 entities.
- SQLite file `data/live-polling.db` (repo-relative, resolved by walking up to `ShapeLive.sln`), table `LivePollingSession`, timestamps stored as UTC `O` strings.
- No auth anywhere; `StartedByUserId` = `"local"`.
- Shell: commands are PowerShell 7 from repo root `D:\Work\Shape_5_sec` unless shown as bash.

## Review Focus

1. **Page opened mid-session** — the `/series` seed ends at the last committed tick and live batches overlap it; a reasonable user expects one continuous line, no duplicated or missing second. Pinned in Task 15 (`merges a series seed with overlapping live frames without duplicates`).
2. **Gateway restart mid-session sends a second baseline** (09-30 known gap) — the snapshot re-baselines and its non-zero events are stored as `active`; the UI must not draw them as transition markers. Pinned in Task 7 (`Second_baseline_mid_session_rebaselines_and_records_active_events`) and Task 15 (`markerKind returns null for active events`).
3. **Two entries with the same `TimestampUtc` in one batch** (09-30 known gap: duplicate RowKeys fail the transaction) — the second must be skipped as not-newer, never written. Pinned in Task 7 (`Second_entry_with_same_timestamp_is_skipped`).
4. **Extend or stop just after lazy expiry** — the row is past `ExpiresUtc`; the backend must answer `409 LIVE_SESSION_NOT_ACTIVE` without calling the gateway. Pinned in Task 11 (`Extend_after_lazy_expiry_is_not_active_without_gateway_call`).
5. **Stop while the gateway is unreachable** — `503`, row left open, and a following start returns `409 LIVE_SESSION_ALREADY_ACTIVE` until expiry. Pinned in Task 11 (`Stop_when_unreachable_leaves_session_open_and_blocks_start`).

---

## File Structure

```
ShapeLive.sln
global.json
.gitignore
config/device.json                          one device
config/alert-catalog.json                   18 alert definitions + value labels
config/pump-seed.json                       73-field telemetry seed (moved from seed/pump-sample.json)
data/                                       SQLite file (git-ignored)
src/Shape.Gateway.Simulator/
  DeviceConfig.cs  PumpSeed.cs  EventParameters.cs  EventState.cs  PumpDevice.cs
  FastPollingSession.cs  FastPollingController.cs  MethodResult.cs
  GatewayEntry.cs  EntryEncoder.cs  BatchBuffer.cs  GzipBatchCodec.cs  EventGridEnvelope.cs
  FaultInjector.cs  IBatchPublisher.cs  EventGridPublisher.cs
  SimulatorOptions.cs  SessionRunner.cs  Program.cs
src/Shape.LiveIngestion/
  Program.cs  host.json  local.settings.json
  Infrastructure/RepoPaths.cs  Infrastructure/UtcDateTimeOffsetConverter.cs
  LivePolling/LiveSessionKeys.cs  LiveTelemetryBatch.cs  LiveEventGridDecoder.cs
  LivePolling/DeviceConfig.cs  DeviceResolver.cs
  LivePolling/IAlertDefinitionCatalog.cs  AlertDefinitionCatalog.cs  LiveEvent.cs  LiveEventResolver.cs  DeviceStatusDeriver.cs
  LivePolling/LiveHead.cs  FoldModels.cs  SnapshotFolder.cs
  LivePolling/ILiveSessionStore.cs  LiveRowMapper.cs  TableLiveSessionStore.cs
  LivePolling/BroadcastEncoder.cs  LiveTelemetryProcessor.cs  SignalRNames.cs
  LivePolling/CleanupSessionSource.cs  LivePollingCleanup.cs
  Functions/LiveTelemetryProcessorFunction.cs  NegotiatorFunctions.cs  LivePollingCleanupTimerFunction.cs
src/Shape.LiveBackend/
  Program.cs  appsettings.json
  Api/ApiResponse.cs  ErrorCodes.cs  Dtos.cs  ServiceResult.cs
  Infrastructure/RepoPaths.cs  UtcDateTimeOffsetConverter.cs
  Devices/DeviceConfig.cs  IDeviceDirectory.cs
  Options/LivePollingOptions.cs
  Sessions/LivePollingSessionRecord.cs  ILivePollingSessionRepository.cs  SqliteLivePollingSessionRepository.cs  SessionSchema.cs
  Gateway/IDirectMethodClient.cs  HttpDirectMethodClient.cs
  LiveData/LiveSessionKeys.cs  ILiveSessionReader.cs  TableLiveSessionReader.cs
  Series/MetricRegistry.cs  SeriesStepExpander.cs  LiveSeriesService.cs
  Sessions/LiveSessionService.cs
  Controllers/LivePollingController.cs
src/Shape.SmokeClient/Program.cs  SmokeConfig.cs  Scenarios.cs  LiveFeed.cs
tests/Shape.Gateway.Simulator.Tests/
tests/Shape.LiveIngestion.Tests/
tests/Shape.LiveBackend.Tests/
tests/Shape.Seam.Tests/
web/src/config.ts
web/src/contract/types.ts  decode.ts  inspect.ts
web/src/live/frames.ts  FrameQueue.ts  seriesSeed.ts  events.ts  units.ts  view.ts  connection.ts  useLiveSession.ts
web/src/api/backend.ts  simulator.ts
web/src/components/Header.tsx  ErrorBanner.tsx  ParameterGraph.tsx  StatusStrip.tsx  OpenedWithBanner.tsx
web/src/components/HoursCards.tsx  LiveMetrics.tsx  SimPanel.tsx  ContractInspector.tsx
web/src/App.tsx  main.tsx  index.css
web/scripts/check-ui.mjs                    headless Edge check (playwright-core)
scripts/run-local.ps1  scripts/smoke.ps1
docs/findings.md
README.md
```

Task order: 1–4 simulator · 5–9 ingestion · 10–12 backend · 13 cleanup · 14 smoke + findings · 15–16 web.

---

### Task 1: Reset repo, scaffold solution, simulator device model

**Files:**
- Move: `seed/pump-sample.json` → `config/pump-seed.json`
- Delete: `src/`, `tests/`, `web/`, `scripts/`, `Shape5Sec.sln`, `README.md` (old prototype)
- Create: `ShapeLive.sln`, `config/device.json`, `config/alert-catalog.json`
- Create: `src/Shape.Gateway.Simulator/{DeviceConfig,PumpSeed,EventParameters,EventState,PumpDevice}.cs`
- Test: `tests/Shape.Gateway.Simulator.Tests/{TestConfig,PumpDeviceTests,EventStateTests}.cs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `DeviceConfig(string SerialNumber, Guid DeviceGuid, string Name, string GatewayId, string ModuleId)`; `static DeviceConfig Load(string path)`; `DeviceConfig.DefaultPath`
  - `PumpSeed.Load(string path) → Dictionary<string, JsonElement>`; `PumpSeed.DefaultPath` = `{AppContext.BaseDirectory}/config/pump-seed.json`
  - `EventParameters.All` (18 names), `EventParameters.Initial() → Dictionary<string,int>`
  - `EventState`: `Snapshot() → IReadOnlyDictionary<string,int>`, `TrySet(IReadOnlyDictionary<string,int>, out IReadOnlyList<string> unknown) → bool`, `Reset()`
  - `PumpDevice(IReadOnlyDictionary<string, JsonElement> seed, int? randomSeed = null)`; `Read(IReadOnlyDictionary<string,int> events) → Dictionary<string, JsonElement>`

- [ ] **Step 1: Reset the repo**

```powershell
git mv seed/pump-sample.json config/pump-seed.json
git rm -r -q src tests web scripts Shape5Sec.sln README.md
Get-ChildItem -Force | Select-Object -ExpandProperty Name
```
Expected: remaining entries are `.git`, `.gitignore`, `.superpowers` (if present), `config`, `docs`, `global.json`.

Append to `.gitignore`:
```gitignore
data/
.azurite/
```

- [ ] **Step 2: Scaffold solution and simulator projects**

```powershell
dotnet new sln -n ShapeLive
dotnet new web -n Shape.Gateway.Simulator -o src/Shape.Gateway.Simulator -f net8.0
dotnet new xunit -n Shape.Gateway.Simulator.Tests -o tests/Shape.Gateway.Simulator.Tests -f net8.0
Remove-Item tests/Shape.Gateway.Simulator.Tests/UnitTest1.cs
dotnet sln add src/Shape.Gateway.Simulator tests/Shape.Gateway.Simulator.Tests
dotnet add tests/Shape.Gateway.Simulator.Tests reference src/Shape.Gateway.Simulator
dotnet add tests/Shape.Gateway.Simulator.Tests package Microsoft.Extensions.TimeProvider.Testing
dotnet add tests/Shape.Gateway.Simulator.Tests package Microsoft.AspNetCore.Mvc.Testing --version 8.0.11
```

Replace `src/Shape.Gateway.Simulator/Program.cs` with a placeholder (Task 4 replaces it):
```csharp
var app = WebApplication.CreateBuilder(args).Build();
app.Run();

public partial class Program;
```

Add inside `<Project>` of **both** `src/Shape.Gateway.Simulator/Shape.Gateway.Simulator.csproj` and `tests/Shape.Gateway.Simulator.Tests/Shape.Gateway.Simulator.Tests.csproj`:
```xml
  <ItemGroup>
    <None Include="..\..\config\*.json" Link="config\%(Filename)%(Extension)" CopyToOutputDirectory="PreserveNewest" />
  </ItemGroup>
```
Add to the test csproj only:
```xml
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
  </ItemGroup>
```

- [ ] **Step 3: Write config files**

`config/device.json`:
```json
{
  "serialNumber": "PUMP-00417",
  "deviceGuid": "9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284",
  "name": "PREC-NEO30-MARYL-07",
  "gatewayId": "gw-001",
  "moduleId": "DefaultModeModule"
}
```

`config/alert-catalog.json`:
```json
{
  "definitions": [
    { "parameterName": "OperatingAlarmActive", "alertCode": 40006, "eventType": "alarm", "domain": "operational", "severity": "critical",
      "description": "Indicates an active operational fault condition.", "action": "Investigate fault code, inspect system, and resolve before restart.",
      "valueLabels": { "0": "No Fault Active", "5": "Emergency Stop Active", "6": "Overstroke Alarm" } },
    { "parameterName": "OperatingWarningActive", "alertCode": 40007, "eventType": "warning", "domain": "operational", "severity": "major",
      "description": "Indicates an active operational warning.", "action": "Monitor condition and schedule inspection if warning persists.",
      "valueLabels": { "0": "No Warning Active", "21": "Left Overstroke Warning" } },
    { "parameterName": "MaintenanceAlarmActive", "alertCode": 40008, "eventType": "alarm", "domain": "maintenance", "severity": "critical",
      "description": "Wear parts lifetime reached - replacement or service required.", "action": "Perform required maintenance and replace affected components." },
    { "parameterName": "MaintenanceWarningActive", "alertCode": 40009, "eventType": "warning", "domain": "maintenance", "severity": "major",
      "description": "Wear parts nearing expected lifetime - replacements or maintenance due soon.", "action": "Plan maintenance and prepare replacement parts." },
    { "parameterName": "OilChangeWarning", "alertCode": 40301, "eventType": "warning", "domain": "maintenance", "severity": "major",
      "description": "Oil lifetime is close to replacement threshold.", "action": "Plan oil change at next maintenance window." },
    { "parameterName": "OilChangeAlarm", "alertCode": 40302, "eventType": "alarm", "domain": "maintenance", "severity": "critical",
      "description": "Oil lifetime has been exceeded.", "action": "Stop operation and perform oil change immediately." },
    { "parameterName": "OilFilterWarning", "alertCode": 40303, "eventType": "warning", "domain": "maintenance", "severity": "major",
      "description": "Oil filter lifetime is close to replacement threshold.", "action": "Prepare replacement oil filter." },
    { "parameterName": "OilFilterAlarm", "alertCode": 40304, "eventType": "alarm", "domain": "maintenance", "severity": "critical",
      "description": "Oil filter lifetime has been exceeded.", "action": "Replace oil filter immediately." },
    { "parameterName": "WaterFilterAssyWarning", "alertCode": 40305, "eventType": "warning", "domain": "maintenance", "severity": "major",
      "description": "Water filter lifetime is close to replacement threshold.", "action": "Plan water filter replacement." },
    { "parameterName": "WaterFilterAssyAlarm", "alertCode": 40306, "eventType": "alarm", "domain": "maintenance", "severity": "critical",
      "description": "Water filter lifetime has been exceeded.", "action": "Replace water filter immediately." },
    { "parameterName": "TemperatureCheckValveAlarmA", "alertCode": 40401, "eventType": "alarm", "domain": "operational", "severity": "critical",
      "description": "Check valve assembly A temperature exceeded critical threshold.", "action": "Inspect cooling system and valve assembly." },
    { "parameterName": "TemperatureCheckValveAlarmB", "alertCode": 40402, "eventType": "alarm", "domain": "operational", "severity": "critical",
      "description": "Check valve assembly B temperature exceeded critical threshold.", "action": "Inspect cooling system and valve assembly." },
    { "parameterName": "TemperatureCheckValveWarningA", "alertCode": 40403, "eventType": "warning", "domain": "operational", "severity": "major",
      "description": "Check valve assembly A temperature above warning threshold.", "action": "Monitor temperature and inspect if persists." },
    { "parameterName": "TemperatureCheckValveWarningB", "alertCode": 40404, "eventType": "warning", "domain": "operational", "severity": "major",
      "description": "Check valve assembly B temperature above warning threshold.", "action": "Monitor temperature and inspect if persists." },
    { "parameterName": "BoosterPressureFault", "alertCode": 40405, "eventType": "alarm", "domain": "operational", "severity": "critical",
      "description": "Booster pressure dropped below safe threshold.", "action": "Inspect booster system and pressure supply." },
    { "parameterName": "HydraulicFilterAssyWarning", "alertCode": 40412, "eventType": "warning", "domain": "maintenance", "severity": "major",
      "description": "Indicates an oil filter warning. Expected lifetime is close to being reached.", "action": "Plan hydraulic oil filter replacement during the next maintenance window." },
    { "parameterName": "HydraulicFilterAssyAlarm", "alertCode": 40413, "eventType": "alarm", "domain": "maintenance", "severity": "critical",
      "description": "Indicates an oil filter alarm. Expected lifetime has been reached.", "action": "Stop operation and replace the hydraulic oil filter immediately." },
    { "parameterName": "MotorRunning", "alertCode": 40001, "eventType": "status", "domain": "operational", "severity": "minor",
      "description": "Indicates if the Motor is Running", "action": "" }
  ]
}
```

- [ ] **Step 4: Write failing tests**

`tests/Shape.Gateway.Simulator.Tests/TestConfig.cs`:
```csharp
using System.Text.Json;
using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public static class TestConfig
{
    public static DeviceConfig Device() => DeviceConfig.Load(DeviceConfig.DefaultPath);
    public static Dictionary<string, JsonElement> Seed() => PumpSeed.Load(PumpSeed.DefaultPath);
    public static JsonElement J(object value) => JsonSerializer.SerializeToElement(value);
}
```

`tests/Shape.Gateway.Simulator.Tests/PumpDeviceTests.cs`:
```csharp
using System.Text.Json;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class PumpDeviceTests
{
    [Fact]
    public void Device_config_loads()
    {
        var d = Device();
        Assert.Equal("PUMP-00417", d.SerialNumber);
        Assert.Equal(Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), d.DeviceGuid);
        Assert.Equal("gw-001", d.GatewayId);
        Assert.Equal("DefaultModeModule", d.ModuleId);
    }

    [Fact]
    public void Seed_contains_every_event_parameter()
    {
        var seed = Seed();
        Assert.Equal(73, seed.Count);
        Assert.Equal(18, EventParameters.All.Count);
        Assert.All(EventParameters.All, p => Assert.True(seed.ContainsKey(p), p));
    }

    [Fact]
    public void Reading_has_exactly_the_seed_keys()
    {
        var seed = Seed();
        var reading = new PumpDevice(seed, randomSeed: 1).Read(EventParameters.Initial());
        Assert.True(reading.Keys.ToHashSet().SetEquals(seed.Keys));
    }

    [Fact]
    public void Same_seed_is_deterministic()
    {
        var a = new PumpDevice(Seed(), randomSeed: 7);
        var b = new PumpDevice(Seed(), randomSeed: 7);
        for (var i = 0; i < 100; i++)
            Assert.Equal(JsonSerializer.Serialize(a.Read(EventParameters.Initial())),
                JsonSerializer.Serialize(b.Read(EventParameters.Initial())));
    }

    [Fact]
    public void Generated_values_stay_in_range()
    {
        var device = new PumpDevice(Seed(), randomSeed: 3);
        for (var i = 0; i < 1000; i++)
        {
            var r = device.Read(EventParameters.Initial());
            Assert.InRange(r["OperatingPressurePSI"].GetInt32(), 50850, 57150);
            Assert.InRange(r["StrokeRateLeft"].GetInt32(), 80, 100);
            Assert.InRange(r["StrokeTimeRight"].GetInt32(), 5200, 5400);
            Assert.InRange(r["HydraulicOilTemperatureF"].GetInt32(), 100, 115);
            Assert.InRange(r["ProportionalOutputValue"].GetInt32(), 70, 90);
        }
    }

    [Fact]
    public void Event_parameters_mirror_event_values_keeping_seed_types()
    {
        var events = EventParameters.Initial();
        events["OperatingAlarmActive"] = 5;
        events["OilFilterWarning"] = 1;
        events["MotorRunning"] = 0;

        var r = new PumpDevice(Seed(), randomSeed: 1).Read(events);

        Assert.Equal(5, r["OperatingAlarmActive"].GetInt32());   // int in the seed
        Assert.True(r["OilFilterWarning"].GetBoolean());          // bool in the seed
        Assert.False(r["MotorRunning"].GetBoolean());
        Assert.False(r["MaintenanceAlarmActive"].GetBoolean());
    }
}
```

`tests/Shape.Gateway.Simulator.Tests/EventStateTests.cs`:
```csharp
using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class EventStateTests
{
    [Fact]
    public void Starts_with_motor_running_and_everything_else_zero()
    {
        var s = new EventState().Snapshot();
        Assert.Equal(1, s["MotorRunning"]);
        Assert.All(s.Where(kv => kv.Key != "MotorRunning"), kv => Assert.Equal(0, kv.Value));
    }

    [Fact]
    public void TrySet_applies_known_parameters()
    {
        var state = new EventState();
        Assert.True(state.TrySet(new Dictionary<string, int> { ["OperatingAlarmActive"] = 6 }, out var unknown));
        Assert.Empty(unknown);
        Assert.Equal(6, state.Snapshot()["OperatingAlarmActive"]);
    }

    [Fact]
    public void TrySet_with_unknown_parameter_applies_nothing()
    {
        var state = new EventState();
        Assert.False(state.TrySet(new Dictionary<string, int> { ["OperatingAlarmActive"] = 6, ["Nope"] = 1 }, out var unknown));
        Assert.Equal(new[] { "Nope" }, unknown);
        Assert.Equal(0, state.Snapshot()["OperatingAlarmActive"]);
    }

    [Fact]
    public void Reset_restores_initial_values()
    {
        var state = new EventState();
        state.TrySet(new Dictionary<string, int> { ["MotorRunning"] = 0, ["OilChangeAlarm"] = 1 }, out _);
        state.Reset();
        Assert.Equal(1, state.Snapshot()["MotorRunning"]);
        Assert.Equal(0, state.Snapshot()["OilChangeAlarm"]);
    }
}
```

- [ ] **Step 5: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Gateway.Simulator.Tests`
Expected: FAIL — `The type or namespace name 'DeviceConfig' could not be found`.

- [ ] **Step 6: Implement**

`src/Shape.Gateway.Simulator/DeviceConfig.cs`:
```csharp
using System.Text.Json;

namespace Shape.Gateway.Simulator;

public sealed record DeviceConfig(string SerialNumber, Guid DeviceGuid, string Name, string GatewayId, string ModuleId)
{
    public static readonly string DefaultPath = Path.Combine(AppContext.BaseDirectory, "config", "device.json");

    public static DeviceConfig Load(string path) =>
        JsonSerializer.Deserialize<DeviceConfig>(File.ReadAllText(path), new JsonSerializerOptions(JsonSerializerDefaults.Web))
        ?? throw new InvalidDataException($"Device config '{path}' is empty.");
}
```

`src/Shape.Gateway.Simulator/PumpSeed.cs`:
```csharp
using System.Text.Json;

namespace Shape.Gateway.Simulator;

public static class PumpSeed
{
    public static readonly string DefaultPath = Path.Combine(AppContext.BaseDirectory, "config", "pump-seed.json");

    public static Dictionary<string, JsonElement> Load(string path) =>
        JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(File.ReadAllText(path))
        ?? throw new InvalidDataException($"Seed '{path}' is empty.");
}
```

`src/Shape.Gateway.Simulator/EventParameters.cs`:
```csharp
namespace Shape.Gateway.Simulator;

/// <summary>The parameters the gateway's EventModeModule edge-detects. Values are raw register values.</summary>
public static class EventParameters
{
    public static readonly IReadOnlyList<string> All =
    [
        "MotorRunning", "OperatingAlarmActive", "OperatingWarningActive", "MaintenanceAlarmActive",
        "MaintenanceWarningActive", "OilChangeWarning", "OilChangeAlarm", "OilFilterWarning", "OilFilterAlarm",
        "WaterFilterAssyWarning", "WaterFilterAssyAlarm", "TemperatureCheckValveAlarmA", "TemperatureCheckValveAlarmB",
        "TemperatureCheckValveWarningA", "TemperatureCheckValveWarningB", "BoosterPressureFault",
        "HydraulicFilterAssyWarning", "HydraulicFilterAssyAlarm",
    ];

    public static Dictionary<string, int> Initial() => All.ToDictionary(p => p, p => p == "MotorRunning" ? 1 : 0);
}
```

`src/Shape.Gateway.Simulator/EventState.cs`:
```csharp
namespace Shape.Gateway.Simulator;

/// <summary>Current event register values, settable through the scenario API. Thread-safe.</summary>
public sealed class EventState
{
    private readonly object _gate = new();
    private Dictionary<string, int> _values = EventParameters.Initial();

    public IReadOnlyDictionary<string, int> Snapshot()
    {
        lock (_gate) return new Dictionary<string, int>(_values);
    }

    public bool TrySet(IReadOnlyDictionary<string, int> updates, out IReadOnlyList<string> unknown)
    {
        unknown = updates.Keys.Where(k => !EventParameters.All.Contains(k)).ToList();
        if (unknown.Count > 0) return false;
        lock (_gate)
            foreach (var (k, v) in updates) _values[k] = v;
        return true;
    }

    public void Reset()
    {
        lock (_gate) _values = EventParameters.Initial();
    }
}
```

`src/Shape.Gateway.Simulator/PumpDevice.cs`:
```csharp
using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>Modbus stand-in: one Read per poll. Event parameters mirror the supplied event values (spec §4.1).</summary>
public sealed class PumpDevice
{
    private static readonly string[] HourKeys =
        ["OperatingHoursTW1", "OperatingHoursCheckValveAssy_A", "OperatingHoursCheckValveAssy_B", "OperatingHoursMotor"];

    private readonly Dictionary<string, JsonElement> _seed;
    private readonly Random _rng;
    private long _tick;
    private int _rateL, _rateR, _timeL, _timeR, _oilF;
    private double _strokes;

    public PumpDevice(IReadOnlyDictionary<string, JsonElement> seed, int? randomSeed = null)
    {
        _seed = new Dictionary<string, JsonElement>(seed);
        _rng = randomSeed is { } s ? new Random(s) : new Random();
        _rateL = Math.Clamp(Int("StrokeRateLeft"), 80, 100);
        _rateR = Math.Clamp(Int("StrokeRateRight"), 80, 100);
        _timeL = Math.Clamp(Int("StrokeTimeLeft"), 5200, 5400);
        _timeR = Math.Clamp(Int("StrokeTimeRight"), 5200, 5400);
        _oilF = Math.Clamp(Int("HydraulicOilTemperatureF"), 100, 115);
        _strokes = Int("StrokeCounterTW1");
    }

    public Dictionary<string, JsonElement> Read(IReadOnlyDictionary<string, int> events)
    {
        var t = _tick++;
        var g = new Dictionary<string, object>();

        var psi = (int)Math.Round(54000 + 3000 * Math.Sin(2 * Math.PI * t / 30.0) + (_rng.NextDouble() * 300 - 150));
        g["OperatingPressurePSI"] = psi;
        g["OperatingPressureBar"] = (int)Math.Round(psi / 14.5038);
        g["StrokeRateLeft"] = _rateL = Walk(_rateL, 2, 80, 100);
        g["StrokeRateRight"] = _rateR = Walk(_rateR, 2, 80, 100);
        g["StrokeTimeLeft"] = _timeL = Walk(_timeL, 10, 5200, 5400);
        g["StrokeTimeRight"] = _timeR = Walk(_timeR, 10, 5200, 5400);
        _strokes += (_rateL + _rateR) / 2.0 / 60.0;
        g["StrokeCounterTW1"] = (long)_strokes;
        if (t > 0 && t % 10 == 0) _oilF = Walk(_oilF, 1, 100, 115);
        g["HydraulicOilTemperatureF"] = _oilF;
        g["HydraulicOilTemperatureC"] = (int)Math.Round((_oilF - 32) * 5 / 9.0);
        g["ProportionalOutputValue"] = (int)Math.Round(70 + Math.Clamp((psi - 51000) / 6000.0, 0, 1) * 20);
        foreach (var key in HourKeys)
            if (_seed.ContainsKey(key)) g[key] = Int(key) + t / 3600;

        foreach (var (name, value) in events)
        {
            if (!_seed.TryGetValue(name, out var seedValue)) continue;
            g[name] = seedValue.ValueKind is JsonValueKind.True or JsonValueKind.False ? value != 0 : value;
        }

        var reading = new Dictionary<string, JsonElement>(_seed);
        foreach (var (k, v) in g)
            if (reading.ContainsKey(k)) reading[k] = JsonSerializer.SerializeToElement(v);
        return reading;
    }

    private int Walk(int v, int step, int min, int max) => Math.Clamp(v + _rng.Next(-step, step + 1), min, max);

    private int Int(string key) => _seed[key].GetInt32();
}
```

- [ ] **Step 7: Run tests — expect pass**

Run: `dotnet test tests/Shape.Gateway.Simulator.Tests`
Expected: `Passed!  - Failed: 0, Passed: 10`.

If `Seed_contains_every_event_parameter` fails, the seed and the catalog names have drifted: fix the seed file, never the test.

- [ ] **Step 8: Commit**

```powershell
git add -A
git commit -m "chore: replace prototype with live polling harness scaffold and simulator device model"
```

---

### Task 2: FastPollingController — module direct-method semantics

**Files:**
- Create: `src/Shape.Gateway.Simulator/{MethodResult,FastPollingSession,FastPollingController}.cs`
- Test: `tests/Shape.Gateway.Simulator.Tests/FastPollingControllerTests.cs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `MethodResult(int Status, object Payload)`
  - `FastPollingSession(Guid sessionId, int pollIntervalSeconds, int durationMinutes, TimeProvider time)`: `SessionId`, `PollIntervalSeconds`, `StartedUtc`, `TotalDurationMinutes`, `CancellationToken Expired`, `CancellationToken Stopped`, `Stop()`, `TryExtend(int minutes, int capMinutes) → string?` (null = success, else cap reason)
  - `FastPollingController(TimeProvider time)`: `Invoke(string methodName, JsonElement? payload) → MethodResult`; `Active → FastPollingSession?`; `Clear(FastPollingSession)`; `event Action<FastPollingSession>? SessionStarted`; const `MaxTotalMinutes = 30`

- [ ] **Step 1: Write failing tests** `tests/Shape.Gateway.Simulator.Tests/FastPollingControllerTests.cs`

```csharp
using System.Text.Json;
using Microsoft.Extensions.Time.Testing;
using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class FastPollingControllerTests
{
    private readonly FakeTimeProvider _time = new(DateTimeOffset.Parse("2026-10-01T10:00:00Z"));

    private static JsonElement? P(string? json) => json is null ? null : JsonDocument.Parse(json).RootElement.Clone();
    private static JsonElement B(MethodResult r) => JsonSerializer.SerializeToElement(r.Payload);
    private static List<string?> Errors(MethodResult r) =>
        B(r).GetProperty("errors").EnumerateArray().Select(e => e.GetString()).ToList();

    [Theory]
    [InlineData(null)]
    [InlineData("{}")]
    [InlineData("\"not an object\"")]
    public void Enable_without_parameters_uses_defaults(string? payload)
    {
        var r = new FastPollingController(_time).Invoke("FastPollingEnable", P(payload));

        Assert.Equal(200, r.Status);
        var b = B(r);
        Assert.Equal("accepted", b.GetProperty("result").GetString());
        Assert.Equal(10, b.GetProperty("durationMinutes").GetInt32());
        Assert.Equal(1, b.GetProperty("intervalSeconds").GetInt32());
        Assert.True(Guid.TryParse(b.GetProperty("sessionId").GetString(), out _));
        Assert.Equal(new[] { "sessionDurationMinutes", "pollIntervalSeconds" },
            b.GetProperty("defaultsApplied").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void Enable_uses_supplied_values_case_insensitively()
    {
        var c = new FastPollingController(_time);
        var r = c.Invoke("FastPollingEnable", P("""{"SESSIONDURATIONMINUTES":15,"pollintervalseconds":5}"""));

        Assert.Equal(200, r.Status);
        Assert.Empty(B(r).GetProperty("defaultsApplied").EnumerateArray());
        Assert.Equal(15, c.Active!.TotalDurationMinutes);
        Assert.Equal(5, c.Active.PollIntervalSeconds);
    }

    [Fact]
    public void Enable_rejects_every_out_of_range_or_non_integer_value()
    {
        var r = new FastPollingController(_time).Invoke("FastPollingEnable",
            P("""{"sessionDurationMinutes":45,"pollIntervalSeconds":"x"}"""));

        Assert.Equal(400, r.Status);
        Assert.Equal("Invalid request.", B(r).GetProperty("reason").GetString());
        Assert.Equal(new[]
        {
            "'sessionDurationMinutes'=45 is outside the allowed range (10-30).",
            "'pollIntervalSeconds' must be an integer.",
        }, Errors(r));
    }

    [Fact]
    public void Enable_validates_before_reporting_already_active()
    {
        var c = new FastPollingController(_time);
        c.Invoke("FastPollingEnable", null);

        Assert.Equal(400, c.Invoke("FastPollingEnable", P("""{"pollIntervalSeconds":0}""")).Status);
        var conflict = c.Invoke("FastPollingEnable", null);
        Assert.Equal(409, conflict.Status);
        Assert.Equal("A fast polling session is already active.", B(conflict).GetProperty("reason").GetString());
    }

    [Fact]
    public void Disable_stops_and_clears_or_409s_when_idle()
    {
        var c = new FastPollingController(_time);
        Assert.Equal(409, c.Invoke("FastPollingDisable", null).Status);

        c.Invoke("FastPollingEnable", null);
        var s = c.Active!;
        var r = c.Invoke("FastPollingDisable", null);

        Assert.Equal(200, r.Status);
        Assert.Equal(s.SessionId.ToString(), B(r).GetProperty("sessionId").GetString());
        Assert.True(s.Stopped.IsCancellationRequested);
        Assert.Null(c.Active);
    }

    [Fact]
    public void Session_expires_after_its_duration()
    {
        var c = new FastPollingController(_time);
        c.Invoke("FastPollingEnable", P("""{"sessionDurationMinutes":10}"""));
        var s = c.Active!;

        _time.Advance(TimeSpan.FromMinutes(10) - TimeSpan.FromSeconds(1));
        Assert.False(s.Expired.IsCancellationRequested);
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.True(s.Expired.IsCancellationRequested);
    }

    [Fact]
    public void Extend_measures_total_from_original_start()
    {
        var c = new FastPollingController(_time);
        c.Invoke("FastPollingEnable", P("""{"sessionDurationMinutes":10}"""));
        var s = c.Active!;
        _time.Advance(TimeSpan.FromMinutes(5));

        var r = c.Invoke("Extend-FastPolling",
            P($$"""{"sessionId":"{{s.SessionId}}","extendSessionDurationMinutes":10}"""));

        Assert.Equal(200, r.Status);
        Assert.Equal(10, B(r).GetProperty("extendedByMinutes").GetInt32());
        Assert.Equal(20, B(r).GetProperty("totalDurationMinutes").GetInt32());
        _time.Advance(TimeSpan.FromMinutes(15) - TimeSpan.FromSeconds(1)); // T = 19:59
        Assert.False(s.Expired.IsCancellationRequested);
        _time.Advance(TimeSpan.FromSeconds(1));                              // T = 20:00
        Assert.True(s.Expired.IsCancellationRequested);
    }

    [Fact]
    public void Extend_checks_fields_then_session_then_cap()
    {
        var c = new FastPollingController(_time);
        Assert.Equal(400, c.Invoke("Extend-FastPolling", P("""{"sessionId":"nope"}""")).Status);
        Assert.Equal(409, c.Invoke("Extend-FastPolling",
            P($$"""{"sessionId":"{{Guid.NewGuid()}}","extendSessionDurationMinutes":5}""")).Status);

        c.Invoke("FastPollingEnable", P("""{"sessionDurationMinutes":25}"""));
        var id = c.Active!.SessionId;
        var mismatch = c.Invoke("Extend-FastPolling",
            P($$"""{"sessionId":"{{Guid.NewGuid()}}","extendSessionDurationMinutes":5}"""));
        Assert.Equal(409, mismatch.Status);
        Assert.EndsWith("does not match the currently active session.", B(mismatch).GetProperty("reason").GetString());

        var cap = c.Invoke("Extend-FastPolling", P($$"""{"sessionId":"{{id}}","extendSessionDurationMinutes":10}"""));
        Assert.Equal(400, cap.Status);
        Assert.Equal(
            "Extending by 10 min would bring the session's total duration to 35 min, exceeding the 30 min cap (currently 25 min).",
            B(cap).GetProperty("reason").GetString());
        Assert.Equal(25, c.Active!.TotalDurationMinutes);
    }

    [Fact]
    public void Extend_field_errors_list_both_fields()
    {
        var r = new FastPollingController(_time).Invoke("Extend-FastPolling", P("{}"));
        Assert.Equal(new[]
        {
            "'sessionId' is required and must be a GUID.",
            "'extendSessionDurationMinutes' is required and must be a positive integer.",
        }, Errors(r));
    }

    [Fact]
    public void Unknown_method_is_501()
    {
        Assert.Equal(501, new FastPollingController(_time).Invoke("Reboot", null).Status);
    }

    [Fact]
    public void SessionStarted_fires_on_successful_enable_only()
    {
        var c = new FastPollingController(_time);
        var started = new List<Guid>();
        c.SessionStarted += s => started.Add(s.SessionId);

        c.Invoke("FastPollingEnable", null);
        c.Invoke("FastPollingEnable", null);

        Assert.Single(started);
    }

    [Fact]
    public void Clear_only_clears_the_matching_session()
    {
        var c = new FastPollingController(_time);
        c.Invoke("FastPollingEnable", null);
        var first = c.Active!;
        c.Invoke("FastPollingDisable", null);
        c.Invoke("FastPollingEnable", null);
        var second = c.Active!;

        c.Clear(first);

        Assert.Same(second, c.Active);
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Gateway.Simulator.Tests --filter FullyQualifiedName~FastPollingController`
Expected: FAIL — `The type or namespace name 'FastPollingController' could not be found`.

- [ ] **Step 3: Implement**

`src/Shape.Gateway.Simulator/MethodResult.cs`:
```csharp
namespace Shape.Gateway.Simulator;

/// <summary>A direct-method response: the module's status code and JSON payload.</summary>
public sealed record MethodResult(int Status, object Payload);
```

`src/Shape.Gateway.Simulator/FastPollingSession.cs`:
```csharp
namespace Shape.Gateway.Simulator;

/// <summary>One fast-polling session. One expiry timer, rescheduled on extend (module doc v3).</summary>
public sealed class FastPollingSession
{
    private readonly object _gate = new();
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _expiry;
    private readonly CancellationTokenSource _stop = new();
    private int _totalMinutes;

    public FastPollingSession(Guid sessionId, int pollIntervalSeconds, int durationMinutes, TimeProvider time)
    {
        SessionId = sessionId;
        PollIntervalSeconds = pollIntervalSeconds;
        _time = time;
        StartedUtc = time.GetUtcNow();
        _totalMinutes = durationMinutes;
        _expiry = new CancellationTokenSource(TimeSpan.FromMinutes(durationMinutes), time);
    }

    public Guid SessionId { get; }
    public int PollIntervalSeconds { get; }
    public DateTimeOffset StartedUtc { get; }
    public int TotalDurationMinutes { get { lock (_gate) return _totalMinutes; } }
    public CancellationToken Expired => _expiry.Token;
    public CancellationToken Stopped => _stop.Token;

    public void Stop() => _stop.Cancel();

    /// <returns>null on success; otherwise the cap-exceeded reason.</returns>
    public string? TryExtend(int minutes, int capMinutes)
    {
        lock (_gate)
        {
            var proposed = _totalMinutes + minutes;
            if (proposed > capMinutes)
                return $"Extending by {minutes} min would bring the session's total duration to {proposed} min, " +
                       $"exceeding the {capMinutes} min cap (currently {_totalMinutes} min).";
            _totalMinutes = proposed;
            var remaining = StartedUtc.AddMinutes(proposed) - _time.GetUtcNow();
            _expiry.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
            return null;
        }
    }
}
```

`src/Shape.Gateway.Simulator/FastPollingController.cs`:
```csharp
using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>FastPollingEnable / FastPollingDisable / Extend-FastPolling exactly as the module doc (v3).</summary>
public sealed class FastPollingController(TimeProvider time)
{
    public const int MinDuration = 10, MaxDuration = 30, DefaultDuration = 10;
    public const int MinInterval = 1, MaxInterval = 60, DefaultInterval = 1;
    public const int MaxTotalMinutes = 30;

    private FastPollingSession? _active;

    public FastPollingSession? Active => Volatile.Read(ref _active);

    public event Action<FastPollingSession>? SessionStarted;

    public MethodResult Invoke(string methodName, JsonElement? payload) => methodName switch
    {
        "FastPollingEnable" => Enable(payload),
        "FastPollingDisable" => Disable(),
        "Extend-FastPolling" => Extend(payload),
        _ => new MethodResult(501, new { result = "rejected", reason = $"Method '{methodName}' is not implemented." }),
    };

    public void Clear(FastPollingSession session) => Interlocked.CompareExchange(ref _active, null, session);

    private MethodResult Enable(JsonElement? payload)
    {
        var errors = new List<string>();
        var duration = ReadOptionalInt(payload, "sessionDurationMinutes", MinDuration, MaxDuration, errors, out var durationSupplied);
        var interval = ReadOptionalInt(payload, "pollIntervalSeconds", MinInterval, MaxInterval, errors, out var intervalSupplied);
        if (errors.Count > 0) return Rejected400(errors);

        var session = new FastPollingSession(Guid.NewGuid(), interval ?? DefaultInterval, duration ?? DefaultDuration, time);
        if (Interlocked.CompareExchange(ref _active, session, null) is not null)
            return new MethodResult(409, new { result = "rejected", reason = "A fast polling session is already active." });

        var defaults = new List<string>();
        if (!durationSupplied) defaults.Add("sessionDurationMinutes");
        if (!intervalSupplied) defaults.Add("pollIntervalSeconds");
        SessionStarted?.Invoke(session);
        return new MethodResult(200, new
        {
            result = "accepted",
            durationMinutes = session.TotalDurationMinutes,
            intervalSeconds = session.PollIntervalSeconds,
            sessionId = session.SessionId,
            defaultsApplied = defaults,
        });
    }

    private MethodResult Disable()
    {
        var session = Volatile.Read(ref _active);
        if (session is null || Interlocked.CompareExchange(ref _active, null, session) != session)
            return new MethodResult(409, new { result = "rejected", reason = "No fast polling session is active." });
        session.Stop();
        return new MethodResult(200, new { result = "accepted", sessionId = session.SessionId });
    }

    private MethodResult Extend(JsonElement? payload)
    {
        Guid? sessionId = null;
        int? minutes = null;
        if (payload is { ValueKind: JsonValueKind.Object } obj)
        {
            foreach (var p in obj.EnumerateObject())
            {
                if (p.NameEquals("sessionId") && p.Value.ValueKind == JsonValueKind.String &&
                    Guid.TryParse(p.Value.GetString(), out var g))
                    sessionId = g;
                if (p.NameEquals("extendSessionDurationMinutes") && p.Value.ValueKind == JsonValueKind.Number &&
                    p.Value.TryGetInt32(out var m) && m > 0)
                    minutes = m;
            }
        }
        var errors = new List<string>();
        if (sessionId is null) errors.Add("'sessionId' is required and must be a GUID.");
        if (minutes is null) errors.Add("'extendSessionDurationMinutes' is required and must be a positive integer.");
        if (errors.Count > 0) return Rejected400(errors);

        var session = Active;
        if (session is null)
            return new MethodResult(409, new { result = "rejected", reason = "No fast polling session is active." });
        if (session.SessionId != sessionId)
            return new MethodResult(409, new
            {
                result = "rejected",
                reason = $"Session '{sessionId}' does not match the currently active session.",
            });

        var capReason = session.TryExtend(minutes!.Value, MaxTotalMinutes);
        if (capReason is not null)
            return new MethodResult(400, new { result = "rejected", reason = capReason });

        return new MethodResult(200, new
        {
            result = "accepted",
            sessionId = session.SessionId,
            extendedByMinutes = minutes,
            totalDurationMinutes = session.TotalDurationMinutes,
        });
    }

    private static MethodResult Rejected400(List<string> errors) =>
        new(400, new { result = "rejected", reason = "Invalid request.", errors });

    private static int? ReadOptionalInt(JsonElement? payload, string name, int min, int max, List<string> errors, out bool supplied)
    {
        supplied = false;
        if (payload is not { ValueKind: JsonValueKind.Object } obj) return null;
        foreach (var p in obj.EnumerateObject())
        {
            if (!p.Name.Equals(name, StringComparison.OrdinalIgnoreCase)) continue;
            supplied = true;
            if (p.Value.ValueKind != JsonValueKind.Number || !p.Value.TryGetInt32(out var v))
            {
                errors.Add($"'{name}' must be an integer.");
                return null;
            }
            if (v < min || v > max)
            {
                errors.Add($"'{name}'={v} is outside the allowed range ({min}-{max}).");
                return null;
            }
            return v;
        }
        return null;
    }
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.Gateway.Simulator.Tests --filter FullyQualifiedName~FastPollingController`
Expected: `Passed!  - Failed: 0, Passed: 14`.

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.Gateway.Simulator tests/Shape.Gateway.Simulator.Tests
git commit -m "feat(simulator): add fast-polling enable, disable and extend semantics"
```

---


### Task 3: Simulator batch pipeline — entries, gzip, Event Grid envelope, faults, publisher

**Files:**
- Create: `src/Shape.Gateway.Simulator/{GatewayEntry,EntryEncoder,BatchBuffer,GzipBatchCodec,EventGridEnvelope,FaultInjector,IBatchPublisher,EventGridPublisher}.cs`
- Test: `tests/Shape.Gateway.Simulator.Tests/{EntryEncoderTests,BatchBufferTests,EnvelopeTests,FaultInjectorTests,EventGridPublisherTests}.cs`

**Interfaces:**
- Consumes: `DeviceConfig` (Task 1).
- Produces:
  - `GatewayEntry { DateTime TimestampUtc; Dictionary<string,JsonElement>? Body; Dictionary<string,JsonElement>? Changes; Dictionary<string,int>? Event }` serialised as `TimestampUtc`/`Body`/`Changes`/`Event`, nulls omitted
  - `EntryEncoder.Encode(IReadOnlyDictionary<string,JsonElement> reading, IReadOnlyDictionary<string,int> events, DateTimeOffset ts) → GatewayEntry` (one instance per session)
  - `OutgoingBatch(Guid SessionId, int Seq, DateTime SentUtc, IReadOnlyList<GatewayEntry> Entries)`
  - `BatchBuffer(Guid sessionId, int size = 5)`: `Add(GatewayEntry, DateTimeOffset now) → OutgoingBatch?`, `Flush(DateTimeOffset now) → OutgoingBatch?`
  - `GzipBatchCodec.Encode(IReadOnlyList<GatewayEntry>) → byte[]`, `GzipBatchCodec.DecodeJson(byte[]) → string`
  - `EventGridEnvelope.Build(DeviceConfig device, OutgoingBatch batch, byte[] gzipBody) → string` (JSON array of one event)
  - `FaultInjector(int dropEvery, bool reorder, bool duplicate)`: `Process(OutgoingBatch) → IReadOnlyList<OutgoingBatch>`, `Drain() → IReadOnlyList<OutgoingBatch>`
  - `IBatchPublisher.PublishAsync(OutgoingBatch batch, CancellationToken ct) → Task<bool>`
  - `EventGridPublisher(HttpClient http, Uri endpoint, DeviceConfig device, IReadOnlyList<TimeSpan> retryDelays, ILogger<EventGridPublisher> logger)`; `DefaultRetryDelays` = 1 s, 2 s, 4 s; `DefaultEndpoint` = `http://localhost:7071/runtime/webhooks/EventGrid?functionName=LiveTelemetryProcessorEventGrid`

- [ ] **Step 1: Write failing tests**

`tests/Shape.Gateway.Simulator.Tests/EntryEncoderTests.cs`:
```csharp
using System.Text.Json;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class EntryEncoderTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");

    private static Dictionary<string, JsonElement> R(params (string K, object V)[] kv) => kv.ToDictionary(x => x.K, x => J(x.V));
    private static Dictionary<string, int> E(params (string K, int V)[] kv) => kv.ToDictionary(x => x.K, x => x.V);

    [Fact]
    public void First_entry_is_Body_with_full_Event_including_zeros()
    {
        var entry = new EntryEncoder().Encode(R(("A", 1)), E(("OperatingAlarmActive", 0), ("MotorRunning", 1)), T0);

        Assert.Equal(T0.UtcDateTime, entry.TimestampUtc);
        Assert.NotNull(entry.Body);
        Assert.Null(entry.Changes);
        Assert.Equal(2, entry.Event!.Count);
        Assert.Equal(0, entry.Event["OperatingAlarmActive"]);
    }

    [Fact]
    public void Delta_carries_only_changed_fields_and_no_Event_when_events_unchanged()
    {
        var enc = new EntryEncoder();
        enc.Encode(R(("A", 1), ("B", 2)), E(("X", 0)), T0);

        var entry = enc.Encode(R(("A", 1), ("B", 3)), E(("X", 0)), T0.AddSeconds(1));

        Assert.Null(entry.Body);
        Assert.Equal(new[] { "B" }, entry.Changes!.Keys);
        Assert.Null(entry.Event);
    }

    [Fact]
    public void Heartbeat_is_empty_Changes()
    {
        var enc = new EntryEncoder();
        enc.Encode(R(("A", 1)), E(("X", 0)), T0);

        var entry = enc.Encode(R(("A", 1)), E(("X", 0)), T0.AddSeconds(1));

        Assert.Empty(entry.Changes!);
        Assert.Null(entry.Event);
    }

    [Fact]
    public void Event_carries_only_changed_parameters()
    {
        var enc = new EntryEncoder();
        enc.Encode(R(("A", 1)), E(("X", 0), ("Y", 0)), T0);

        var entry = enc.Encode(R(("A", 1)), E(("X", 5), ("Y", 0)), T0.AddSeconds(1));

        Assert.Equal(new Dictionary<string, int> { ["X"] = 5 }, entry.Event);
    }

    [Fact]
    public void Serialises_with_gateway_names_and_omits_nulls()
    {
        var entry = new EntryEncoder().Encode(R(("A", 1)), E(("X", 0)), T0);
        var json = JsonSerializer.Serialize(entry);

        Assert.Contains("\"TimestampUtc\":\"2026-10-01T10:00:00Z\"", json);
        Assert.Contains("\"Body\":{\"A\":1}", json);
        Assert.Contains("\"Event\":{\"X\":0}", json);
        Assert.DoesNotContain("Changes", json);
    }
}
```

`tests/Shape.Gateway.Simulator.Tests/BatchBufferTests.cs`:
```csharp
using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class BatchBufferTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    private static GatewayEntry Entry(int s) => new() { TimestampUtc = T0.AddSeconds(s).UtcDateTime, Changes = new() };

    [Fact]
    public void Emits_on_fifth_entry_with_increasing_seq()
    {
        var id = Guid.NewGuid();
        var buffer = new BatchBuffer(id);
        var batches = new List<OutgoingBatch>();
        for (var i = 0; i < 10; i++)
            if (buffer.Add(Entry(i), T0.AddSeconds(i)) is { } b) batches.Add(b);

        Assert.Equal(2, batches.Count);
        Assert.Equal(new[] { 0, 1 }, batches.Select(b => b.Seq));
        Assert.All(batches, b => Assert.Equal(5, b.Entries.Count));
        Assert.All(batches, b => Assert.Equal(id, b.SessionId));
        Assert.Equal(T0.AddSeconds(4).UtcDateTime, batches[0].SentUtc);
    }

    [Fact]
    public void Flush_emits_partial_then_nothing()
    {
        var buffer = new BatchBuffer(Guid.NewGuid());
        buffer.Add(Entry(0), T0);
        buffer.Add(Entry(1), T0);

        Assert.Equal(2, buffer.Flush(T0)!.Entries.Count);
        Assert.Null(buffer.Flush(T0));
    }
}
```

`tests/Shape.Gateway.Simulator.Tests/EnvelopeTests.cs`:
```csharp
using System.IO.Compression;
using System.Text.Json;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class EnvelopeTests
{
    private static readonly DateTime T0 = DateTime.Parse("2026-10-01T10:00:00Z").ToUniversalTime();

    [Fact]
    public void Gzip_round_trips_Entries()
    {
        var entries = new[] { new GatewayEntry { TimestampUtc = T0, Changes = new() { ["A"] = J(2) } } };

        var json = GzipBatchCodec.DecodeJson(GzipBatchCodec.Encode(entries));

        using var doc = JsonDocument.Parse(json);
        var e = Assert.Single(doc.RootElement.GetProperty("Entries").EnumerateArray());
        Assert.Equal(2, e.GetProperty("Changes").GetProperty("A").GetInt32());
    }

    [Fact]
    public void Event_grid_envelope_matches_iot_hub_device_telemetry_shape()
    {
        var device = Device();
        var batch = new OutgoingBatch(Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8"), 0, T0,
            [new GatewayEntry { TimestampUtc = T0, Changes = new() }]);
        var gz = GzipBatchCodec.Encode(batch.Entries);

        using var doc = JsonDocument.Parse(EventGridEnvelope.Build(device, batch, gz));

        var ev = Assert.Single(doc.RootElement.EnumerateArray());
        Assert.Equal("Microsoft.Devices.DeviceTelemetry", ev.GetProperty("eventType").GetString());
        Assert.Equal("devices/gw-001", ev.GetProperty("subject").GetString());
        var data = ev.GetProperty("data");
        var props = data.GetProperty("properties");
        Assert.Equal("fast-polling", props.GetProperty("telemetry-type").GetString());
        Assert.Equal("cc27db6b-508b-488e-a816-faae684491d8", props.GetProperty("session-id").GetString());
        Assert.Equal("2026-10-01T10:00:00.0000000Z", props.GetProperty("batchSentUtc").GetString());
        var sys = data.GetProperty("systemProperties");
        Assert.Equal("gw-001", sys.GetProperty("iothub-connection-device-id").GetString());
        Assert.Equal("gzip", sys.GetProperty("iothub-content-encoding").GetString());
        var body = Convert.FromBase64String(data.GetProperty("body").GetString()!);
        Assert.Equal(gz, body);
        using var gzip = new GZipStream(new MemoryStream(body), CompressionMode.Decompress);
        Assert.Contains("Entries", new StreamReader(gzip).ReadToEnd());
    }
}
```

`tests/Shape.Gateway.Simulator.Tests/FaultInjectorTests.cs`:
```csharp
using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class FaultInjectorTests
{
    private static OutgoingBatch B(int seq) => new(Guid.Empty, seq, DateTime.UtcNow, []);

    private static List<int> Run(FaultInjector f, int n)
    {
        var sent = new List<int>();
        for (var i = 0; i < n; i++) sent.AddRange(f.Process(B(i)).Select(b => b.Seq));
        sent.AddRange(f.Drain().Select(b => b.Seq));
        return sent;
    }

    [Fact] public void No_faults_passes_through() => Assert.Equal(new[] { 0, 1, 2 }, Run(new FaultInjector(0, false, false), 3));
    [Fact] public void Drop_every_2_keeps_baseline() => Assert.Equal(new[] { 0, 2, 4 }, Run(new FaultInjector(2, false, false), 6));
    [Fact] public void Reorder_keeps_baseline_first_then_swaps_pairs() => Assert.Equal(new[] { 0, 2, 1, 4, 3 }, Run(new FaultInjector(0, true, false), 5));
    [Fact] public void Reorder_drains_leftover() => Assert.Equal(new[] { 0, 2, 1, 3 }, Run(new FaultInjector(0, true, false), 4));
    [Fact] public void Duplicate_sends_every_batch_twice() => Assert.Equal(new[] { 0, 0, 1, 1 }, Run(new FaultInjector(0, false, true), 2));
}
```

`tests/Shape.Gateway.Simulator.Tests/EventGridPublisherTests.cs`:
```csharp
using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class EventGridPublisherTests
{
    public sealed record Captured(string? AegEventType, string? MediaType, string Body);

    private sealed class Stub(params object[] outcomes) : HttpMessageHandler
    {
        private int _n;
        public List<Captured> Requests { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage r, CancellationToken ct)
        {
            Requests.Add(new Captured(
                r.Headers.TryGetValues("aeg-event-type", out var v) ? v.Single() : null,
                r.Content?.Headers.ContentType?.MediaType,
                await r.Content!.ReadAsStringAsync(ct)));
            var o = outcomes[Math.Min(_n++, outcomes.Length - 1)];
            if (o is Exception ex) throw ex;
            return new HttpResponseMessage((HttpStatusCode)o);
        }
    }

    private static EventGridPublisher P(Stub s) => new(new HttpClient(s), EventGridPublisher.DefaultEndpoint, Device(),
        [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero], NullLogger<EventGridPublisher>.Instance);

    private static OutgoingBatch Batch() => new(Guid.NewGuid(), 0, DateTime.UtcNow,
        [new GatewayEntry { TimestampUtc = DateTime.UtcNow, Changes = new() }]);

    [Fact]
    public async Task Posts_notification_with_json_body()
    {
        var s = new Stub(HttpStatusCode.Accepted);
        Assert.True(await P(s).PublishAsync(Batch(), CancellationToken.None));
        var r = Assert.Single(s.Requests);
        Assert.Equal("Notification", r.AegEventType);
        Assert.Equal("application/json", r.MediaType);
        Assert.Contains("Microsoft.Devices.DeviceTelemetry", r.Body);
    }

    [Fact]
    public async Task Retries_failure_status_connection_error_and_timeout()
    {
        var s = new Stub(HttpStatusCode.InternalServerError, new HttpRequestException("refused"),
            new TaskCanceledException("timeout"), HttpStatusCode.OK);
        Assert.True(await P(s).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(4, s.Requests.Count);
    }

    [Fact]
    public async Task Gives_up_after_three_retries()
    {
        var s = new Stub(HttpStatusCode.ServiceUnavailable);
        Assert.False(await P(s).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(4, s.Requests.Count);
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Gateway.Simulator.Tests`
Expected: FAIL — `The type or namespace name 'EntryEncoder' could not be found`.

- [ ] **Step 3: Implement**

`src/Shape.Gateway.Simulator/GatewayEntry.cs`:
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.Gateway.Simulator;

/// <summary>One tick as the module sends it (module doc v3 "Sample Telemetry Payloads").</summary>
public sealed record GatewayEntry
{
    [JsonPropertyName("TimestampUtc")] public DateTime TimestampUtc { get; init; }

    [JsonPropertyName("Body"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, JsonElement>? Body { get; init; }

    [JsonPropertyName("Changes"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, JsonElement>? Changes { get; init; }

    [JsonPropertyName("Event"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, int>? Event { get; init; }
}

public sealed record OutgoingBatch(Guid SessionId, int Seq, DateTime SentUtc, IReadOnlyList<GatewayEntry> Entries);
```

`src/Shape.Gateway.Simulator/EntryEncoder.cs`:
```csharp
using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>Body + full Event on a session's first tick; Changes + changed-only Event afterwards. One per session.</summary>
public sealed class EntryEncoder
{
    private Dictionary<string, JsonElement>? _lastSent;
    private Dictionary<string, int>? _lastEvents;

    public GatewayEntry Encode(IReadOnlyDictionary<string, JsonElement> reading, IReadOnlyDictionary<string, int> events, DateTimeOffset ts)
    {
        var current = new Dictionary<string, JsonElement>(reading);
        var currentEvents = new Dictionary<string, int>(events);
        GatewayEntry entry;
        if (_lastSent is null || _lastEvents is null)
        {
            entry = new GatewayEntry { TimestampUtc = ts.UtcDateTime, Body = current, Event = new Dictionary<string, int>(currentEvents) };
        }
        else
        {
            var changes = new Dictionary<string, JsonElement>();
            foreach (var (k, v) in current)
                if (!_lastSent.TryGetValue(k, out var prev) || prev.GetRawText() != v.GetRawText())
                    changes[k] = v;
            var eventChanges = currentEvents
                .Where(kv => !_lastEvents.TryGetValue(kv.Key, out var prev) || prev != kv.Value)
                .ToDictionary(kv => kv.Key, kv => kv.Value);
            entry = new GatewayEntry
            {
                TimestampUtc = ts.UtcDateTime,
                Changes = changes,
                Event = eventChanges.Count > 0 ? eventChanges : null,
            };
        }
        _lastSent = current;
        _lastEvents = currentEvents;
        return entry;
    }
}
```

`src/Shape.Gateway.Simulator/BatchBuffer.cs`:
```csharp
namespace Shape.Gateway.Simulator;

/// <summary>Buffers entries into batches of 5 (module BatchSize). One per session.</summary>
public sealed class BatchBuffer(Guid sessionId, int size = 5)
{
    private readonly List<GatewayEntry> _pending = new();
    private int _seq;

    public OutgoingBatch? Add(GatewayEntry entry, DateTimeOffset now)
    {
        _pending.Add(entry);
        return _pending.Count >= size ? Emit(now) : null;
    }

    public OutgoingBatch? Flush(DateTimeOffset now) => _pending.Count == 0 ? null : Emit(now);

    private OutgoingBatch Emit(DateTimeOffset now)
    {
        var batch = new OutgoingBatch(sessionId, _seq++, now.UtcDateTime, _pending.ToList());
        _pending.Clear();
        return batch;
    }
}
```

`src/Shape.Gateway.Simulator/GzipBatchCodec.cs`:
```csharp
using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>{ "Entries": [...] } → gzip (CompressionLevel.Optimal), as GzipCompressor does in the module.</summary>
public static class GzipBatchCodec
{
    public static byte[] Encode(IReadOnlyList<GatewayEntry> entries)
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(new { Entries = entries });
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gzip.Write(json);
        return output.ToArray();
    }

    public static string DecodeJson(byte[] gzipBytes)
    {
        using var gzip = new GZipStream(new MemoryStream(gzipBytes), CompressionMode.Decompress);
        using var reader = new StreamReader(gzip, Encoding.UTF8);
        return reader.ReadToEnd();
    }
}
```

`src/Shape.Gateway.Simulator/EventGridEnvelope.cs`:
```csharp
using System.Globalization;
using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>
/// IoT Hub → Event Grid system topic shape (Microsoft.Devices.DeviceTelemetry). This is the harness's assumption
/// for open item 5 of the 09-20 design: a gzip body arrives base64 in data.body, encoding in systemProperties.
/// </summary>
public static class EventGridEnvelope
{
    public static string Build(DeviceConfig device, OutgoingBatch batch, byte[] gzipBody)
    {
        var sent = batch.SentUtc.ToString("O", CultureInfo.InvariantCulture);
        return JsonSerializer.Serialize(new[]
        {
            new
            {
                id = Guid.NewGuid().ToString(),
                topic = "/subscriptions/local/resourceGroups/local/providers/Microsoft.Devices/IotHubs/local-hub",
                subject = $"devices/{device.GatewayId}",
                eventType = "Microsoft.Devices.DeviceTelemetry",
                eventTime = sent,
                dataVersion = "",
                metadataVersion = "1",
                data = new
                {
                    properties = new Dictionary<string, string>
                    {
                        ["batchSentUtc"] = sent,
                        ["session-id"] = batch.SessionId.ToString(),
                        ["telemetry-type"] = "fast-polling",
                    },
                    systemProperties = new Dictionary<string, string>
                    {
                        ["iothub-connection-device-id"] = device.GatewayId,
                        ["iothub-connection-module-id"] = device.ModuleId,
                        ["iothub-content-encoding"] = "gzip",
                        ["iothub-content-type"] = "application/json",
                        ["iothub-enqueuedtime"] = sent,
                    },
                    body = Convert.ToBase64String(gzipBody),
                },
            },
        });
    }
}
```

`src/Shape.Gateway.Simulator/FaultInjector.cs`:
```csharp
namespace Shape.Gateway.Simulator;

/// <summary>Delivery faults for validating the ordering and redelivery guards. One per session.</summary>
public sealed class FaultInjector(int dropEvery, bool reorder, bool duplicate)
{
    private long _count;
    private OutgoingBatch? _held;

    public IReadOnlyList<OutgoingBatch> Process(OutgoingBatch batch)
    {
        _count++;
        if (dropEvery >= 2 && _count % dropEvery == 0) return [];
        IReadOnlyList<OutgoingBatch> result;
        if (!reorder || batch.Seq == 0) result = [batch];   // never hold the baseline batch
        else if (_held is null) { _held = batch; result = []; }
        else { result = [batch, _held]; _held = null; }
        return duplicate ? result.SelectMany(b => new[] { b, b }).ToList() : result;
    }

    public IReadOnlyList<OutgoingBatch> Drain()
    {
        if (_held is null) return [];
        var held = _held;
        _held = null;
        return duplicate ? [held, held] : [held];
    }
}
```

`src/Shape.Gateway.Simulator/IBatchPublisher.cs`:
```csharp
namespace Shape.Gateway.Simulator;

public interface IBatchPublisher
{
    /// <returns>true when delivered; false when dropped after all retries.</returns>
    Task<bool> PublishAsync(OutgoingBatch batch, CancellationToken ct);
}
```

`src/Shape.Gateway.Simulator/EventGridPublisher.cs`:
```csharp
using System.Text;

namespace Shape.Gateway.Simulator;

/// <summary>Delivers a batch as Event Grid would: POST to the function's local EventGrid webhook.</summary>
public sealed class EventGridPublisher(
    HttpClient http, Uri endpoint, DeviceConfig device, IReadOnlyList<TimeSpan> retryDelays, ILogger<EventGridPublisher> logger)
    : IBatchPublisher
{
    public static readonly Uri DefaultEndpoint =
        new("http://localhost:7071/runtime/webhooks/EventGrid?functionName=LiveTelemetryProcessorEventGrid");

    public static readonly TimeSpan[] DefaultRetryDelays = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4)];

    public async Task<bool> PublishAsync(OutgoingBatch batch, CancellationToken ct)
    {
        var gz = GzipBatchCodec.Encode(batch.Entries);
        var json = EventGridEnvelope.Build(device, batch, gz);
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Post, endpoint)
                {
                    Content = new StringContent(json, Encoding.UTF8, "application/json"),
                };
                req.Headers.Add("aeg-event-type", "Notification");
                using var res = await http.SendAsync(req, ct);
                if (res.IsSuccessStatusCode)
                {
                    logger.LogInformation("Delivered batch {Seq} ({Entries} entries, {Bytes} gzip bytes)", batch.Seq, batch.Entries.Count, gz.Length);
                    return true;
                }
                logger.LogWarning("Batch {Seq} attempt {Attempt} got HTTP {Status}", batch.Seq, attempt + 1, (int)res.StatusCode);
            }
            catch (HttpRequestException ex)
            {
                logger.LogWarning("Batch {Seq} attempt {Attempt} failed: {Error}", batch.Seq, attempt + 1, ex.Message);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning("Batch {Seq} attempt {Attempt} timed out", batch.Seq, attempt + 1);
            }

            if (attempt >= retryDelays.Count)
            {
                logger.LogError("Dropping batch {Seq} after {Attempts} attempts", batch.Seq, attempt + 1);
                return false;
            }
            await Task.Delay(retryDelays[attempt], ct);
        }
    }
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.Gateway.Simulator.Tests`
Expected: `Passed!  - Failed: 0` with 41 tests (10 + 14 + 17 new).

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.Gateway.Simulator tests/Shape.Gateway.Simulator.Tests
git commit -m "feat(simulator): add gzip batch encoding, Event Grid envelope, faults and publisher"
```

---


### Task 4: Simulator session runner and host (IoT Hub facade, scenario API)

**Files:**
- Create: `src/Shape.Gateway.Simulator/{SimulatorOptions,SessionRunner}.cs`
- Modify: `src/Shape.Gateway.Simulator/Program.cs` (replace placeholder), `src/Shape.Gateway.Simulator/appsettings.json`
- Test: `tests/Shape.Gateway.Simulator.Tests/{SimulatorOptionsTests,SessionRunnerTests,HostTests}.cs`

**Interfaces:**
- Consumes: everything from Tasks 1–3.
- Produces:
  - `SimulatorOptions { int? RandomSeed; int DropEvery; bool Reorder; bool Duplicate; bool AlarmAtStart; bool Unreachable; Uri FunctionsUrl; string Urls }`; `static SimulatorOptions Parse(string[] args)`
  - `SessionRunner(Func<PumpDevice>, EventState, IBatchPublisher, SimulatorOptions, FastPollingController, TimeProvider, ILogger<SessionRunner>)`; `RunAsync(FastPollingSession session, CancellationToken hostStopping) → Task`
  - `MethodInvocation(string MethodName, JsonElement? Payload, int? ResponseTimeoutInSeconds)`
  - HTTP on `:5100`: `POST /twins/{deviceId}/modules/{moduleId}/methods` → `200 { status, payload }` | `404` | `504`; `POST /sim/events`; `POST /sim/events/clear`; `GET /sim/state` → `{ active, sessionId, totalDurationMinutes, pollIntervalSeconds, events }`

- [ ] **Step 1: Write failing tests**

`tests/Shape.Gateway.Simulator.Tests/SimulatorOptionsTests.cs`:
```csharp
using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class SimulatorOptionsTests
{
    [Fact]
    public void Defaults()
    {
        var o = SimulatorOptions.Parse([]);
        Assert.Null(o.RandomSeed);
        Assert.Equal(0, o.DropEvery);
        Assert.False(o.Reorder || o.Duplicate || o.AlarmAtStart || o.Unreachable);
        Assert.Equal(EventGridPublisher.DefaultEndpoint, o.FunctionsUrl);
        Assert.Equal("http://localhost:5100", o.Urls);
    }

    [Fact]
    public void Parses_every_flag()
    {
        var o = SimulatorOptions.Parse(["--seed", "42", "--drop-every", "3", "--reorder", "--duplicate",
            "--alarm-at-start", "--unreachable", "--functions-url", "http://x/hook", "--urls", "http://localhost:6000"]);
        Assert.Equal(42, o.RandomSeed);
        Assert.Equal(3, o.DropEvery);
        Assert.True(o.Reorder && o.Duplicate && o.AlarmAtStart && o.Unreachable);
        Assert.Equal(new Uri("http://x/hook"), o.FunctionsUrl);
        Assert.Equal("http://localhost:6000", o.Urls);
    }

    [Theory]
    [InlineData("--drop-every", "1")]
    [InlineData("--seed", "x")]
    [InlineData("--bogus", "1")]
    public void Rejects_bad_args(string flag, string value) =>
        Assert.Throws<ArgumentException>(() => SimulatorOptions.Parse([flag, value]));
}
```

`tests/Shape.Gateway.Simulator.Tests/SessionRunnerTests.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class SessionRunnerTests
{
    private sealed class Recorder : IBatchPublisher
    {
        private readonly List<OutgoingBatch> _batches = new();
        public List<OutgoingBatch> Batches { get { lock (_batches) return _batches.ToList(); } }
        public Task<bool> PublishAsync(OutgoingBatch b, CancellationToken ct) { lock (_batches) _batches.Add(b); return Task.FromResult(true); }
    }

    private sealed class Harness
    {
        public FakeTimeProvider Time { get; } = new(DateTimeOffset.Parse("2026-10-01T10:00:00Z"));
        public EventState Events { get; } = new();
        public Recorder Publisher { get; } = new();
        public FastPollingController Controller { get; }
        public SessionRunner Runner { get; }

        public Harness(SimulatorOptions? options = null)
        {
            Controller = new FastPollingController(Time);
            var seed = Seed();
            Runner = new SessionRunner(() => new PumpDevice(seed, 1), Events, Publisher, options ?? new SimulatorOptions(),
                Controller, Time, NullLogger<SessionRunner>.Instance);
        }

        public async Task<(FastPollingSession Session, Task Run)> StartAsync(string payload = "{}")
        {
            Controller.Invoke("FastPollingEnable", JsonDocument.Parse(payload).RootElement.Clone());
            var s = Controller.Active!;
            var run = Runner.RunAsync(s, CancellationToken.None);
            await Task.Delay(20);
            return (s, run);
        }

        public async Task TickAsync(int n)
        {
            for (var i = 0; i < n; i++) { Time.Advance(TimeSpan.FromSeconds(1)); await Task.Delay(20); }
        }

        public async Task UntilAsync(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!condition()) { if (DateTime.UtcNow > deadline) throw new TimeoutException(); await Task.Delay(10); }
        }
    }

    [Fact]
    public async Task First_batch_has_Body_with_full_Event_then_deltas()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync();
        await h.TickAsync(9);
        await h.UntilAsync(() => h.Publisher.Batches.Count == 2);

        var b = h.Publisher.Batches;
        Assert.NotNull(b[0].Entries[0].Body);
        Assert.Equal(18, b[0].Entries[0].Event!.Count);
        Assert.All(b[0].Entries.Skip(1), e => Assert.NotNull(e.Changes));
        Assert.All(b[1].Entries, e => Assert.Null(e.Body));

        h.Controller.Invoke("FastPollingDisable", null);
        await run;
    }

    [Fact]
    public async Task Event_change_appears_on_the_next_entry_only()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync();
        await h.TickAsync(1);
        h.Events.TrySet(new Dictionary<string, int> { ["OperatingAlarmActive"] = 6 }, out _);
        await h.TickAsync(3);
        await h.UntilAsync(() => h.Publisher.Batches.Count == 1);

        var entries = h.Publisher.Batches[0].Entries;
        Assert.Null(entries[1].Event);
        Assert.Equal(new Dictionary<string, int> { ["OperatingAlarmActive"] = 6 }, entries[2].Event);
        Assert.Equal(6, entries[2].Changes!["OperatingAlarmActive"].GetInt32());
        Assert.Null(entries[3].Event);

        h.Controller.Invoke("FastPollingDisable", null);
        await run;
    }

    [Fact]
    public async Task Disable_flushes_partial_batch_and_clears_controller()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync();
        await h.TickAsync(6);
        await h.UntilAsync(() => h.Publisher.Batches.Count == 1);
        h.Controller.Invoke("FastPollingDisable", null);
        await run;

        Assert.Equal(2, h.Publisher.Batches[^1].Entries.Count);
        Assert.Null(h.Controller.Active);
    }

    [Fact]
    public async Task Poll_interval_is_honoured()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync("""{"pollIntervalSeconds":5}""");
        await h.TickAsync(20); // 20 s at 5 s/tick → ticks at 0,5,10,15,20 = 5 entries
        await h.UntilAsync(() => h.Publisher.Batches.Count == 1);

        var ts = h.Publisher.Batches[0].Entries.Select(e => e.TimestampUtc).ToList();
        Assert.Equal(TimeSpan.FromSeconds(5), ts[1] - ts[0]);

        h.Controller.Invoke("FastPollingDisable", null);
        await run;
    }

    [Fact]
    public async Task Alarm_at_start_opens_with_the_alarm_in_the_baseline()
    {
        var h = new Harness(new SimulatorOptions { AlarmAtStart = true });
        var (_, run) = await h.StartAsync();
        await h.TickAsync(4);
        await h.UntilAsync(() => h.Publisher.Batches.Count == 1);

        Assert.Equal(5, h.Publisher.Batches[0].Entries[0].Event!["OperatingAlarmActive"]);

        h.Controller.Invoke("FastPollingDisable", null);
        await run;
    }

    [Fact]
    public async Task Expiry_ends_the_run()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync("""{"sessionDurationMinutes":10}""");
        h.Time.Advance(TimeSpan.FromMinutes(10));
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Null(h.Controller.Active);
    }
}
```

`tests/Shape.Gateway.Simulator.Tests/HostTests.cs`:
```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Shape.Gateway.Simulator.Tests;

public class HostTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _client;

    public HostTests(WebApplicationFactory<Program> factory) => _client = factory.CreateClient();

    private Task<HttpResponseMessage> Invoke(string device, string module, string method, object? payload = null) =>
        _client.PostAsJsonAsync($"/twins/{device}/modules/{module}/methods",
            new { methodName = method, payload, responseTimeoutInSeconds = 5 });

    [Fact]
    public async Task Facade_404s_unknown_device_or_module()
    {
        Assert.Equal(HttpStatusCode.NotFound, (await Invoke("nope", "DefaultModeModule", "FastPollingEnable")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await Invoke("gw-001", "nope", "FastPollingEnable")).StatusCode);
    }

    [Fact]
    public async Task Facade_wraps_module_status_and_payload_and_scenario_api_works()
    {
        var res = await Invoke("gw-001", "DefaultModeModule", "FastPollingEnable", new { sessionDurationMinutes = 10 });
        Assert.Equal(HttpStatusCode.OK, res.StatusCode);
        var body = await res.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(200, body.GetProperty("status").GetInt32());
        Assert.Equal("accepted", body.GetProperty("payload").GetProperty("result").GetString());

        var state = await _client.GetFromJsonAsync<JsonElement>("/sim/state");
        Assert.True(state.GetProperty("active").GetBoolean());

        var bad = await _client.PostAsJsonAsync("/sim/events", new Dictionary<string, int> { ["Nope"] = 1 });
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        var ok = await _client.PostAsJsonAsync("/sim/events", new Dictionary<string, int> { ["OperatingAlarmActive"] = 5 });
        Assert.Equal(5, (await ok.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("OperatingAlarmActive").GetInt32());
        await _client.PostAsync("/sim/events/clear", null);

        var disable = await (await Invoke("gw-001", "DefaultModeModule", "FastPollingDisable")).Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(200, disable.GetProperty("status").GetInt32());
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Gateway.Simulator.Tests`
Expected: FAIL — `The type or namespace name 'SimulatorOptions' could not be found`.

- [ ] **Step 3: Implement**

`src/Shape.Gateway.Simulator/SimulatorOptions.cs`:
```csharp
namespace Shape.Gateway.Simulator;

public sealed record SimulatorOptions
{
    public int? RandomSeed { get; init; }
    public int DropEvery { get; init; }
    public bool Reorder { get; init; }
    public bool Duplicate { get; init; }
    public bool AlarmAtStart { get; init; }
    public bool Unreachable { get; init; }
    public Uri FunctionsUrl { get; init; } = EventGridPublisher.DefaultEndpoint;
    public string Urls { get; init; } = "http://localhost:5100";

    public static SimulatorOptions Parse(string[] args)
    {
        var o = new SimulatorOptions();
        for (var i = 0; i < args.Length; i++)
        {
            o = args[i] switch
            {
                "--seed" => o with { RandomSeed = Int(args, ++i, "--seed") },
                "--drop-every" => o with { DropEvery = Int(args, ++i, "--drop-every") is >= 2 and var n ? n : throw new ArgumentException("--drop-every must be >= 2.") },
                "--reorder" => o with { Reorder = true },
                "--duplicate" => o with { Duplicate = true },
                "--alarm-at-start" => o with { AlarmAtStart = true },
                "--unreachable" => o with { Unreachable = true },
                "--functions-url" => o with { FunctionsUrl = new Uri(Value(args, ++i, "--functions-url")) },
                "--urls" => o with { Urls = Value(args, ++i, "--urls") },
                _ => throw new ArgumentException($"Unknown argument '{args[i]}'."),
            };
        }
        return o;
    }

    private static string Value(string[] a, int i, string flag) => i < a.Length ? a[i] : throw new ArgumentException($"{flag} needs a value.");
    private static int Int(string[] a, int i, string flag) => int.TryParse(Value(a, i, flag), out var v) ? v : throw new ArgumentException($"{flag} needs an integer.");
}
```

`src/Shape.Gateway.Simulator/SessionRunner.cs`:
```csharp
using System.Threading.Channels;

namespace Shape.Gateway.Simulator;

/// <summary>
/// Runs one session: poll every PollIntervalSeconds, encode, batch 5, publish. Publishing runs on its own consumer so
/// retries never delay sampling, and one failed delivery never ends the session.
/// </summary>
public sealed class SessionRunner(
    Func<PumpDevice> deviceFactory, EventState events, IBatchPublisher publisher, SimulatorOptions options,
    FastPollingController controller, TimeProvider time, ILogger<SessionRunner> logger)
{
    public async Task RunAsync(FastPollingSession session, CancellationToken hostStopping)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(session.Expired, session.Stopped, hostStopping);
        if (options.AlarmAtStart) events.TrySet(new Dictionary<string, int> { ["OperatingAlarmActive"] = 5 }, out _);

        var device = deviceFactory();
        var encoder = new EntryEncoder();
        var buffer = new BatchBuffer(session.SessionId);
        var faults = new FaultInjector(options.DropEvery, options.Reorder, options.Duplicate);
        var outbox = Channel.CreateUnbounded<OutgoingBatch>(new UnboundedChannelOptions { SingleReader = true });
        var consumer = Task.Run(async () =>
        {
            await foreach (var batch in outbox.Reader.ReadAllAsync())
            {
                try { await publisher.PublishAsync(batch, CancellationToken.None); }
                catch (Exception ex) { logger.LogError(ex, "Publishing batch {Seq} threw; continuing", batch.Seq); }
            }
        });

        logger.LogInformation("Twin: FastPolling Active=true SessionId={SessionId} DurationMinutes={Minutes} IntervalSeconds={Interval}",
            session.SessionId, session.TotalDurationMinutes, session.PollIntervalSeconds);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(session.PollIntervalSeconds), time);
        try
        {
            do
            {
                var now = time.GetUtcNow();
                var ev = events.Snapshot();
                var entry = encoder.Encode(device.Read(ev), ev, now);
                if (buffer.Add(entry, now) is { } batch)
                    foreach (var b in faults.Process(batch)) outbox.Writer.TryWrite(b);
            }
            while (await timer.WaitForNextTickAsync(stop.Token));
        }
        catch (OperationCanceledException)
        {
            // disable, expiry or host shutdown
        }
        finally
        {
            controller.Clear(session);
            if (buffer.Flush(time.GetUtcNow()) is { } last)
                foreach (var b in faults.Process(last)) outbox.Writer.TryWrite(b);
            foreach (var b in faults.Drain()) outbox.Writer.TryWrite(b);
            outbox.Writer.Complete();
            await consumer;
            var reason = session.Stopped.IsCancellationRequested ? "ManualDisable"
                : session.Expired.IsCancellationRequested ? "AutoExpiry" : "HostShutdown";
            logger.LogInformation("Twin: FastPolling Active=false SessionId={SessionId} StopReason={Reason}", session.SessionId, reason);
        }
    }
}
```

`src/Shape.Gateway.Simulator/Program.cs`:
```csharp
using System.Text.Json;
using Shape.Gateway.Simulator;

var options = SimulatorOptions.Parse(args);

var builder = WebApplication.CreateBuilder(new WebApplicationOptions { ContentRootPath = AppContext.BaseDirectory });
builder.WebHost.UseUrls(options.Urls);
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.WithOrigins("http://localhost:5173").AllowAnyHeader().AllowAnyMethod()));
builder.Services.AddHttpClient(Microsoft.Extensions.Options.Options.DefaultName, c => c.Timeout = TimeSpan.FromSeconds(30));
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton(DeviceConfig.Load(DeviceConfig.DefaultPath));
builder.Services.AddSingleton<EventState>();
builder.Services.AddSingleton<FastPollingController>();
builder.Services.AddSingleton<IBatchPublisher>(sp => new EventGridPublisher(
    sp.GetRequiredService<IHttpClientFactory>().CreateClient(), options.FunctionsUrl, sp.GetRequiredService<DeviceConfig>(),
    EventGridPublisher.DefaultRetryDelays, sp.GetRequiredService<ILogger<EventGridPublisher>>()));
var seed = PumpSeed.Load(PumpSeed.DefaultPath);
builder.Services.AddSingleton<Func<PumpDevice>>(() => new PumpDevice(seed, options.RandomSeed));
builder.Services.AddSingleton<SessionRunner>();

var app = builder.Build();
app.UseCors();

var controller = app.Services.GetRequiredService<FastPollingController>();
var runner = app.Services.GetRequiredService<SessionRunner>();
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
controller.SessionStarted += session => _ = Task.Run(() => runner.RunAsync(session, lifetime.ApplicationStopping));

// IoT Hub REST "invoke module method" facade: the backend's direct-method client talks to this.
app.MapPost("/twins/{deviceId}/modules/{moduleId}/methods",
    async (string deviceId, string moduleId, MethodInvocation body, DeviceConfig device, CancellationToken ct) =>
    {
        if (deviceId != device.GatewayId || moduleId != device.ModuleId)
            return Results.Json(new { message = $"Device '{deviceId}' module '{moduleId}' not found." }, statusCode: 404);
        if (options.Unreachable)
        {
            await Task.Delay(TimeSpan.FromSeconds((body.ResponseTimeoutInSeconds ?? 30) + 1), ct);
            return Results.Json(new { message = "Timed out waiting for the device to respond." }, statusCode: 504);
        }
        var result = controller.Invoke(body.MethodName, body.Payload);
        return Results.Json(new { status = result.Status, payload = result.Payload });
    });

app.MapPost("/sim/events", (Dictionary<string, int> updates, EventState events) =>
    events.TrySet(updates, out var unknown)
        ? Results.Json(events.Snapshot())
        : Results.Json(new { message = "Unknown event parameters.", unknown }, statusCode: 400));

app.MapPost("/sim/events/clear", (EventState events) =>
{
    events.Reset();
    return Results.Json(events.Snapshot());
});

app.MapGet("/sim/state", (EventState events) =>
{
    var s = controller.Active;
    return Results.Json(new
    {
        active = s is not null,
        sessionId = s?.SessionId,
        totalDurationMinutes = s?.TotalDurationMinutes,
        pollIntervalSeconds = s?.PollIntervalSeconds,
        events = events.Snapshot(),
    });
});

app.Run();

public sealed record MethodInvocation(string MethodName, JsonElement? Payload, int? ResponseTimeoutInSeconds);

public partial class Program;
```

Replace `src/Shape.Gateway.Simulator/appsettings.json`:
```json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "Microsoft.AspNetCore": "Warning",
      "System.Net.Http.HttpClient": "Warning"
    }
  },
  "AllowedHosts": "*"
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.Gateway.Simulator.Tests`
Expected: `Passed!  - Failed: 0` (41 earlier + 5 options + 6 runner + 2 host = 54). Run the `SessionRunner` filter three times to confirm the fake-time tests are stable.

- [ ] **Step 5: Manual check — facade, scenario API, delivery attempts**

```powershell
dotnet build src/Shape.Gateway.Simulator -v q
$sim = Start-Process dotnet -ArgumentList 'src/Shape.Gateway.Simulator/bin/Debug/net8.0/Shape.Gateway.Simulator.dll','--seed','42' -PassThru -RedirectStandardOutput $env:TEMP\sim.log
Start-Sleep 3
Invoke-RestMethod -Method Post http://localhost:5100/twins/gw-001/modules/DefaultModeModule/methods -ContentType application/json -Body '{"methodName":"FastPollingEnable","payload":{},"responseTimeoutInSeconds":10}' | ConvertTo-Json -Compress
Invoke-RestMethod http://localhost:5100/sim/state | ConvertTo-Json -Compress -Depth 3
Start-Sleep 9
Invoke-RestMethod -Method Post http://localhost:5100/twins/gw-001/modules/DefaultModeModule/methods -ContentType application/json -Body '{"methodName":"FastPollingDisable","responseTimeoutInSeconds":10}' | ConvertTo-Json -Compress
Start-Sleep 1; Stop-Process $sim.Id
Select-String -Path $env:TEMP\sim.log -Pattern 'Twin:|attempt' | Select-Object -First 4 | ForEach-Object Line
```
Expected: enable `{"status":200,"payload":{"result":"accepted",...}}`; state `active: true`; disable status 200; log shows `Twin: FastPolling Active=true`, `Batch 0 attempt 1 failed` (no function host yet), `Twin: ... StopReason=ManualDisable`.

- [ ] **Step 6: Commit**

```powershell
git add src/Shape.Gateway.Simulator tests/Shape.Gateway.Simulator.Tests
git commit -m "feat(simulator): add session runner, IoT Hub direct-method facade and scenario API"
```

---


### Task 5: Ingestion scaffold — `LiveSessionKeys` and the Event Grid decoder

**Files:**
- Create: `src/Shape.LiveIngestion/{Shape.LiveIngestion.csproj,Program.cs,host.json,local.settings.json}`
- Create: `src/Shape.LiveIngestion/LivePolling/{LiveSessionKeys,LiveTelemetryBatch,LiveEventGridDecoder}.cs`
- Test: `tests/Shape.LiveIngestion.Tests/{TestEvents,LiveSessionKeysTests,LiveEventGridDecoderTests}.cs`

**Interfaces:**
- Consumes: nothing (the wire shape from Task 3 is restated in the test helper).
- Produces:
  - `LiveSessionKeys`: `HeadRowKey = "!current"`, `Timestamp(DateTimeOffset) → string`, `ChangeRowKey(string parameter, DateTimeOffset ts)`, `ParamRange(string parameter, DateTimeOffset? since = null) → (string From, string To)`, `EventRowKey(string parameter, DateTimeOffset ts)`, `EventRange(DateTimeOffset? since = null) → (string From, string To)`, `ValidateParameter(string)` (throws `ArgumentException`)
  - `LiveEntry(DateTimeOffset TimestampUtc, Dictionary<string,JsonElement>? Body, Dictionary<string,JsonElement>? Changes, Dictionary<string,JsonElement>? Event)`
  - `LiveTelemetryBatch(Guid SessionId, DateTimeOffset BatchSentUtc, string GatewayId, long CompressedBytes, IReadOnlyList<LiveEntry> Entries)`
  - `DecodeStatus { Ok, NotFastPolling, Invalid }`; `DecodeResult(DecodeStatus Status, LiveTelemetryBatch? Batch, string? Reason)`; `LiveEventGridDecoder.Decode(string eventJson) → DecodeResult`

- [ ] **Step 1: Create the function project**

`src/Shape.LiveIngestion/Shape.LiveIngestion.csproj`:
```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <AzureFunctionsVersion>v4</AzureFunctionsVersion>
    <OutputType>Exe</OutputType>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.Azure.Functions.Worker" Version="2.52.0" />
    <PackageReference Include="Microsoft.Azure.Functions.Worker.Sdk" Version="2.1.0" />
    <PackageReference Include="Microsoft.Azure.Functions.Worker.Extensions.EventGrid" Version="3.6.0" />
    <PackageReference Include="Microsoft.Azure.Functions.Worker.Extensions.SignalRService" Version="1.15.0" />
    <PackageReference Include="Microsoft.Azure.Functions.Worker.Extensions.Http" Version="3.3.0" />
    <PackageReference Include="Azure.Data.Tables" Version="12.12.0" />
    <PackageReference Include="Microsoft.Data.Sqlite" Version="8.0.11" />
  </ItemGroup>
  <ItemGroup>
    <None Update="host.json" CopyToOutputDirectory="PreserveNewest" />
    <None Update="local.settings.json" CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="Never" />
    <None Include="..\..\config\*.json" Link="config\%(Filename)%(Extension)" CopyToOutputDirectory="PreserveNewest" />
  </ItemGroup>
</Project>
```

`src/Shape.LiveIngestion/host.json`:
```json
{
  "version": "2.0",
  "logging": { "logLevel": { "default": "Information", "Host.Results": "Error", "Function": "Information" } }
}
```

`src/Shape.LiveIngestion/local.settings.json` (committed: only local emulator values):
```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
    "AzureSignalRConnectionString": "Endpoint=http://localhost;Port=8888;AccessKey=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGH;Version=1.0;",
    "LiveTableStorage": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;",
    "LivePolling__SessionDbPath": "data/live-polling.db",
    "LivePolling__CleanupSchedule": "*/15 * * * * *"
  },
  "Host": {
    "LocalHttpPort": 7071,
    "CORS": "http://localhost:5173",
    "CORSCredentials": true
  }
}
```

`src/Shape.LiveIngestion/Program.cs` (DI is added in Task 9):
```csharp
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .Build();

host.Run();
```

```powershell
dotnet sln add src/Shape.LiveIngestion
dotnet new xunit -n Shape.LiveIngestion.Tests -o tests/Shape.LiveIngestion.Tests -f net8.0
Remove-Item tests/Shape.LiveIngestion.Tests/UnitTest1.cs
dotnet sln add tests/Shape.LiveIngestion.Tests
dotnet add tests/Shape.LiveIngestion.Tests reference src/Shape.LiveIngestion
dotnet add tests/Shape.LiveIngestion.Tests package Microsoft.Extensions.TimeProvider.Testing
```
Add inside `<Project>` of `tests/Shape.LiveIngestion.Tests/Shape.LiveIngestion.Tests.csproj`:
```xml
  <ItemGroup>
    <None Include="..\..\config\*.json" Link="config\%(Filename)%(Extension)" CopyToOutputDirectory="PreserveNewest" />
  </ItemGroup>
```

- [ ] **Step 2: Write failing tests**

`tests/Shape.LiveIngestion.Tests/TestEvents.cs`:
```csharp
using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace Shape.LiveIngestion.Tests;

/// <summary>Builds Event Grid Microsoft.Devices.DeviceTelemetry events in the simulator's shape (spec §4.4).</summary>
public static class TestEvents
{
    public static readonly Guid Session = Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8");

    public static byte[] Gzip(string json)
    {
        using var output = new MemoryStream();
        using (var gz = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gz.Write(Encoding.UTF8.GetBytes(json));
        return output.ToArray();
    }

    public static string Build(string entriesJson, string telemetryType = "fast-polling", Guid? session = null,
        string gateway = "gw-001", string? rawBody = null, string batchSentUtc = "2026-10-01T10:00:05.0000000Z")
    {
        var body = rawBody ?? Convert.ToBase64String(Gzip($$"""{"Entries":{{entriesJson}}}"""));
        return JsonSerializer.Serialize(new
        {
            id = "1",
            subject = $"devices/{gateway}",
            eventType = "Microsoft.Devices.DeviceTelemetry",
            eventTime = batchSentUtc,
            dataVersion = "",
            data = new
            {
                properties = new Dictionary<string, string>
                {
                    ["batchSentUtc"] = batchSentUtc,
                    ["session-id"] = (session ?? Session).ToString(),
                    ["telemetry-type"] = telemetryType,
                },
                systemProperties = new Dictionary<string, string>
                {
                    ["iothub-connection-device-id"] = gateway,
                    ["iothub-content-encoding"] = "gzip",
                },
                body,
            },
        });
    }
}
```

`tests/Shape.LiveIngestion.Tests/LiveSessionKeysTests.cs`:
```csharp
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class LiveSessionKeysTests
{
    private static readonly DateTimeOffset T = DateTimeOffset.Parse("2026-09-28T10:27:02.218Z");

    private static bool InRange(string key, (string From, string To) r) =>
        string.CompareOrdinal(key, r.From) >= 0 && string.CompareOrdinal(key, r.To) < 0;

    [Fact]
    public void Row_keys_use_O_format_in_utc()
    {
        Assert.Equal("OperatingPressurePSI|2026-09-28T10:27:02.2180000Z", LiveSessionKeys.ChangeRowKey("OperatingPressurePSI", T));
        Assert.Equal("evt|2026-09-28T10:27:02.2180000Z|OperatingAlarmActive", LiveSessionKeys.EventRowKey("OperatingAlarmActive", T));
        Assert.Equal("2026-09-28T10:27:02.2180000Z", LiveSessionKeys.Timestamp(T.ToOffset(TimeSpan.FromHours(5.5))));
    }

    [Fact]
    public void Param_range_excludes_longer_names_with_the_same_prefix()
    {
        var range = LiveSessionKeys.ParamRange("StrokeRate");
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("StrokeRate", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("StrokeRateLeft", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("StrokeRateRight", T), range));
    }

    [Fact]
    public void Event_range_excludes_parameter_rows_and_the_head_row()
    {
        var range = LiveSessionKeys.EventRange();
        Assert.True(InRange(LiveSessionKeys.EventRowKey("OilFilterAlarm", T), range));
        Assert.False(InRange(LiveSessionKeys.HeadRowKey, range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("OperatingPressurePSI", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("evtX", T), range));
    }

    [Fact]
    public void Since_lower_bounds_are_inclusive_and_chronological()
    {
        var range = LiveSessionKeys.ParamRange("A", T);
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("A", T.AddTicks(-1)), range));
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("A", T), range));
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("A", T.AddDays(1)), range));
    }

    [Fact]
    public void Two_events_on_one_tick_do_not_collide()
    {
        Assert.NotEqual(LiveSessionKeys.EventRowKey("OperatingAlarmActive", T), LiveSessionKeys.EventRowKey("MaintenanceAlarmActive", T));
    }

    [Theory]
    [InlineData("evt")]
    [InlineData("a|b")]
    [InlineData("")]
    public void Reserved_or_malformed_parameter_names_are_rejected(string name)
    {
        Assert.Throws<ArgumentException>(() => LiveSessionKeys.ChangeRowKey(name, T));
        Assert.Throws<ArgumentException>(() => LiveSessionKeys.ParamRange(name));
    }
}
```

`tests/Shape.LiveIngestion.Tests/LiveEventGridDecoderTests.cs`:
```csharp
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class LiveEventGridDecoderTests
{
    private const string Entries = """
    [ { "TimestampUtc": "2026-10-01T10:00:01Z", "Body": { "A": 1, "B": true }, "Event": { "OperatingAlarmActive": 0 } },
      { "TimestampUtc": "2026-10-01T10:00:02Z", "Changes": { "A": 2 } },
      { "TimestampUtc": "2026-10-01T10:00:03Z", "Changes": {}, "Event": { "OperatingAlarmActive": 5 } } ]
    """;

    [Fact]
    public void Decodes_a_fast_polling_batch()
    {
        var r = LiveEventGridDecoder.Decode(TestEvents.Build(Entries));

        Assert.Equal(DecodeStatus.Ok, r.Status);
        var b = r.Batch!;
        Assert.Equal(TestEvents.Session, b.SessionId);
        Assert.Equal("gw-001", b.GatewayId);
        Assert.Equal(DateTimeOffset.Parse("2026-10-01T10:00:05Z"), b.BatchSentUtc);
        Assert.True(b.CompressedBytes > 0);
        Assert.Equal(3, b.Entries.Count);
        Assert.Equal(1, b.Entries[0].Body!["A"].GetInt32());
        Assert.Equal(0, b.Entries[0].Event!["OperatingAlarmActive"].GetInt32());
        Assert.Null(b.Entries[1].Event);
        Assert.Empty(b.Entries[2].Changes!);
        Assert.Equal(5, b.Entries[2].Event!["OperatingAlarmActive"].GetInt32());
    }

    [Fact]
    public void Accepts_an_event_array_with_one_event()
    {
        Assert.Equal(DecodeStatus.Ok, LiveEventGridDecoder.Decode($"[{TestEvents.Build(Entries)}]").Status);
    }

    [Fact]
    public void Other_telemetry_types_are_not_fast_polling()
    {
        Assert.Equal(DecodeStatus.NotFastPolling, LiveEventGridDecoder.Decode(TestEvents.Build(Entries, telemetryType: "regular")).Status);
    }

    [Fact]
    public void Event_that_is_not_an_object_is_ignored_but_entry_kept()
    {
        var r = LiveEventGridDecoder.Decode(TestEvents.Build("""[{ "TimestampUtc": "2026-10-01T10:00:01Z", "Changes": { "A": 1 }, "Event": [1,2] }]"""));
        Assert.Equal(DecodeStatus.Ok, r.Status);
        Assert.Null(r.Batch!.Entries[0].Event);
    }

    [Theory]
    [InlineData("not base64!!")]
    [InlineData("aGVsbG8=")] // valid base64, not gzip
    public void Corrupt_body_is_invalid(string body)
    {
        var r = LiveEventGridDecoder.Decode(TestEvents.Build("[]", rawBody: body));
        Assert.Equal(DecodeStatus.Invalid, r.Status);
        Assert.False(string.IsNullOrEmpty(r.Reason));
    }

    [Theory]
    [InlineData("""[{ "TimestampUtc": "2026-10-01T10:00:01Z" }]""")]                                         // neither
    [InlineData("""[{ "TimestampUtc": "2026-10-01T10:00:01Z", "Body": {}, "Changes": {} }]""")]             // both
    [InlineData("""[{ "Changes": {} }]""")]                                                                  // no timestamp
    [InlineData("""[]""")]                                                                                   // empty
    public void Malformed_entries_are_invalid(string entries)
    {
        Assert.Equal(DecodeStatus.Invalid, LiveEventGridDecoder.Decode(TestEvents.Build(entries)).Status);
    }

    [Fact]
    public void Garbage_event_json_is_invalid()
    {
        Assert.Equal(DecodeStatus.Invalid, LiveEventGridDecoder.Decode("{not json").Status);
    }
}
```

- [ ] **Step 3: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveIngestion.Tests`
Expected: FAIL — `The type or namespace name 'LivePolling' does not exist in the namespace 'Shape.LiveIngestion'`.

- [ ] **Step 4: Implement**

`src/Shape.LiveIngestion/LivePolling/LiveSessionKeys.cs` (**the backend gets a byte-identical copy in Task 12, namespace aside**):
```csharp
using System.Globalization;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Row key scheme for one live session partition (09-20 §5.2, 09-30 §6). Duplicated verbatim in the backend; both
/// copies carry the same tests. Timestamps are UTC "O" (7 fractional digits), so lexical order is chronological.
/// </summary>
public static class LiveSessionKeys
{
    public const string HeadRowKey = "!current";
    public const string EventPrefix = "evt";

    public static string Timestamp(DateTimeOffset ts) => ts.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);

    public static string ChangeRowKey(string parameter, DateTimeOffset ts)
    {
        ValidateParameter(parameter);
        return $"{parameter}|{Timestamp(ts)}";
    }

    /// <summary>[From, To) covering every change point of one parameter; '}' is '|' + 1.</summary>
    public static (string From, string To) ParamRange(string parameter, DateTimeOffset? since = null)
    {
        ValidateParameter(parameter);
        return ($"{parameter}|{(since is { } s ? Timestamp(s) : "")}", $"{parameter}}}");
    }

    public static string EventRowKey(string parameter, DateTimeOffset ts)
    {
        ValidateParameter(parameter);
        return $"{EventPrefix}|{Timestamp(ts)}|{parameter}";
    }

    public static (string From, string To) EventRange(DateTimeOffset? since = null) =>
        ($"{EventPrefix}|{(since is { } s ? Timestamp(s) : "")}", $"{EventPrefix}}}");

    public static void ValidateParameter(string parameter)
    {
        if (string.IsNullOrEmpty(parameter) || parameter.Contains('|') || parameter == EventPrefix)
            throw new ArgumentException($"'{parameter}' cannot be used as a live session parameter name.", nameof(parameter));
    }
}
```

`src/Shape.LiveIngestion/LivePolling/LiveTelemetryBatch.cs`:
```csharp
using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>One gateway tick. Exactly one of Body/Changes; Event is a sibling, never nested.</summary>
public sealed record LiveEntry(
    DateTimeOffset TimestampUtc,
    Dictionary<string, JsonElement>? Body,
    Dictionary<string, JsonElement>? Changes,
    Dictionary<string, JsonElement>? Event);

public sealed record LiveTelemetryBatch(
    Guid SessionId, DateTimeOffset BatchSentUtc, string GatewayId, long CompressedBytes, IReadOnlyList<LiveEntry> Entries);

public enum DecodeStatus { Ok, NotFastPolling, Invalid }

public sealed record DecodeResult(DecodeStatus Status, LiveTelemetryBatch? Batch, string? Reason)
{
    public static DecodeResult Invalid(string reason) => new(DecodeStatus.Invalid, null, reason);
}
```

`src/Shape.LiveIngestion/LivePolling/LiveEventGridDecoder.cs`:
```csharp
using System.IO.Compression;
using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// The only class that knows how Event Grid presents an IoT Hub message: data.properties (app properties),
/// data.systemProperties, data.body = base64(gzip(JSON)). Assumption for 09-20 §16 item 5 — change here once confirmed.
/// </summary>
public static class LiveEventGridDecoder
{
    public static DecodeResult Decode(string eventJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(eventJson);
            var ev = doc.RootElement.ValueKind == JsonValueKind.Array ? doc.RootElement[0] : doc.RootElement;
            var data = ev.GetProperty("data");
            var props = data.GetProperty("properties");

            if (!props.TryGetProperty("telemetry-type", out var type) || type.GetString() != "fast-polling")
                return new DecodeResult(DecodeStatus.NotFastPolling, null, $"telemetry-type is '{(type.ValueKind == JsonValueKind.String ? type.GetString() : "<absent>")}'");

            var sessionId = Guid.Parse(props.GetProperty("session-id").GetString()!);
            var sentUtc = DateTimeOffset.Parse(props.GetProperty("batchSentUtc").GetString()!, null,
                System.Globalization.DateTimeStyles.RoundtripKind);
            var sys = data.GetProperty("systemProperties");
            var gatewayId = sys.GetProperty("iothub-connection-device-id").GetString()!;
            var gzip = sys.TryGetProperty("iothub-content-encoding", out var enc) && enc.GetString() == "gzip";

            byte[] raw;
            try { raw = Convert.FromBase64String(data.GetProperty("body").GetString()!); }
            catch (FormatException) { return DecodeResult.Invalid("body is not base64"); }

            string json;
            try { json = gzip ? Gunzip(raw) : System.Text.Encoding.UTF8.GetString(raw); }
            catch (InvalidDataException) { return DecodeResult.Invalid("body is not valid gzip"); }

            using var batchDoc = JsonDocument.Parse(json);
            var entries = new List<LiveEntry>();
            foreach (var e in batchDoc.RootElement.GetProperty("Entries").EnumerateArray())
            {
                if (!e.TryGetProperty("TimestampUtc", out var ts) || ts.ValueKind != JsonValueKind.String)
                    return DecodeResult.Invalid("entry without TimestampUtc");
                var body = ObjectOrNull(e, "Body");
                var changes = ObjectOrNull(e, "Changes");
                if ((body is null) == (changes is null))
                    return DecodeResult.Invalid("entry must carry exactly one of Body or Changes");
                entries.Add(new LiveEntry(ts.GetDateTimeOffset(), body, changes, ObjectOrNull(e, "Event")));
            }
            if (entries.Count == 0) return DecodeResult.Invalid("batch has no entries");

            return new DecodeResult(DecodeStatus.Ok, new LiveTelemetryBatch(sessionId, sentUtc, gatewayId, raw.LongLength, entries), null);
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException or FormatException
                                       or IndexOutOfRangeException or ArgumentNullException)
        {
            return DecodeResult.Invalid(ex.Message);
        }
    }

    private static Dictionary<string, JsonElement>? ObjectOrNull(JsonElement entry, string name) =>
        entry.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Object
            ? v.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone())
            : null;

    private static string Gunzip(byte[] bytes)
    {
        using var gz = new GZipStream(new MemoryStream(bytes), CompressionMode.Decompress);
        using var reader = new StreamReader(gz);
        return reader.ReadToEnd();
    }
}
```

- [ ] **Step 5: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveIngestion.Tests`
Expected: `Passed!  - Failed: 0, Passed: 19` (keys 8 + decoder 11; theory cases count individually).

- [ ] **Step 6: Commit**

```powershell
git add src/Shape.LiveIngestion tests/Shape.LiveIngestion.Tests ShapeLive.sln
git commit -m "feat(ingestion): add live session key scheme and Event Grid batch decoder"
```

---


### Task 6: Alert catalog, event resolution and device status

**Files:**
- Create: `src/Shape.LiveIngestion/LivePolling/{IAlertDefinitionCatalog,AlertDefinitionCatalog,LiveEvent,LiveEventResolver,DeviceStatusDeriver}.cs`
- Test: `tests/Shape.LiveIngestion.Tests/{TestCatalog,AlertDefinitionCatalogTests,LiveEventResolverTests,DeviceStatusDeriverTests}.cs`

**Interfaces:**
- Consumes: `config/alert-catalog.json` (Task 1).
- Produces:
  - `AlertDefinition(string ParameterName, int AlertCode, string EventType, string Domain, string Severity, string Description, IReadOnlyDictionary<int,string>? ValueLabels)`
  - `IAlertDefinitionCatalog.GetAsync(CancellationToken ct = default) → Task<IReadOnlyDictionary<string, AlertDefinition>>`
  - `AlertDefinitionCatalog(string path, ILogger<AlertDefinitionCatalog> logger)`; `AlertDefinitionCatalog.DefaultPath`
  - `LiveEvent(string Parameter, long Value, string State, string EventType, string Domain, string Severity, string? Label)`; `LiveEventStates.Triggered/Cleared/Active`; `LiveEvent.Unknown = "unknown"`
  - `LiveEventResolver.Resolve(IReadOnlyDictionary<string,JsonElement>? eventBlock, bool isBaseline, IReadOnlyDictionary<string,AlertDefinition> catalog, ILogger logger) → IReadOnlyList<LiveEvent>`
  - `DeviceStatusDeriver.Derive(IReadOnlyDictionary<string,JsonElement> snapshot) → int` (0/1/2)

- [ ] **Step 1: Write failing tests**

`tests/Shape.LiveIngestion.Tests/TestCatalog.cs`:
```csharp
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public static class TestCatalog
{
    public static IReadOnlyDictionary<string, AlertDefinition> Load() =>
        new AlertDefinitionCatalog(AlertDefinitionCatalog.DefaultPath, NullLogger<AlertDefinitionCatalog>.Instance).GetAsync().GetAwaiter().GetResult();
}
```

`tests/Shape.LiveIngestion.Tests/AlertDefinitionCatalogTests.cs`:
```csharp
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class AlertDefinitionCatalogTests
{
    [Fact]
    public async Task Loads_eighteen_definitions_with_labels_on_the_two_operational_codes_only()
    {
        var defs = await new AlertDefinitionCatalog(AlertDefinitionCatalog.DefaultPath, NullLogger<AlertDefinitionCatalog>.Instance).GetAsync();

        Assert.Equal(18, defs.Count);
        Assert.Equal("Emergency Stop Active", defs["OperatingAlarmActive"].ValueLabels![5]);
        Assert.Equal("Left Overstroke Warning", defs["OperatingWarningActive"].ValueLabels![21]);
        Assert.Equal(2, defs.Values.Count(d => d.ValueLabels is { Count: > 0 }));
        Assert.Equal("status", defs["MotorRunning"].EventType);
        Assert.Equal(40306, defs["WaterFilterAssyAlarm"].AlertCode);
    }

    [Fact]
    public async Task Loads_once()
    {
        var path = Path.GetTempFileName();
        File.Copy(AlertDefinitionCatalog.DefaultPath, path, overwrite: true);
        var catalog = new AlertDefinitionCatalog(path, NullLogger<AlertDefinitionCatalog>.Instance);
        var first = await catalog.GetAsync();
        File.WriteAllText(path, "{ broken");

        Assert.Same(first, await catalog.GetAsync());
    }

    [Fact]
    public async Task Failed_load_is_not_cached()
    {
        var path = Path.Combine(Path.GetTempPath(), $"catalog-{Guid.NewGuid():N}.json");
        var catalog = new AlertDefinitionCatalog(path, NullLogger<AlertDefinitionCatalog>.Instance);
        await Assert.ThrowsAnyAsync<Exception>(() => catalog.GetAsync());

        File.Copy(AlertDefinitionCatalog.DefaultPath, path);
        Assert.Equal(18, (await catalog.GetAsync()).Count);
    }
}
```

`tests/Shape.LiveIngestion.Tests/LiveEventResolverTests.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class LiveEventResolverTests
{
    private static readonly IReadOnlyDictionary<string, AlertDefinition> Catalog = TestCatalog.Load();

    private static Dictionary<string, JsonElement> Block(string json) =>
        JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());

    private static IReadOnlyList<LiveEvent> Resolve(string json, bool baseline) =>
        LiveEventResolver.Resolve(Block(json), baseline, Catalog, NullLogger.Instance);

    [Fact]
    public void Delta_non_zero_is_triggered_with_metadata_and_label()
    {
        var e = Assert.Single(Resolve("""{"OperatingAlarmActive":5}""", baseline: false));
        Assert.Equal(new LiveEvent("OperatingAlarmActive", 5, "triggered", "alarm", "operational", "critical", "Emergency Stop Active"), e);
    }

    [Fact]
    public void Delta_zero_is_cleared_with_null_label()
    {
        var e = Assert.Single(Resolve("""{"OperatingAlarmActive":0}""", baseline: false));
        Assert.Equal("cleared", e.State);
        Assert.Null(e.Label);
    }

    [Fact]
    public void Baseline_non_zero_is_active_and_zeros_are_suppressed()
    {
        var events = Resolve("""{"OperatingAlarmActive":6,"OilFilterAlarm":0,"MotorRunning":1}""", baseline: true);
        Assert.Equal(new[] { "OperatingAlarmActive", "MotorRunning" }, events.Select(e => e.Parameter));
        Assert.All(events, e => Assert.Equal("active", e.State));
        Assert.Equal("Overstroke Alarm", events[0].Label);
    }

    [Fact]
    public void Boolean_parameters_carry_metadata_without_label()
    {
        var e = Assert.Single(Resolve("""{"MaintenanceAlarmActive":1}""", baseline: false));
        Assert.Equal(("alarm", "maintenance", "critical"), (e.EventType, e.Domain, e.Severity));
        Assert.Null(e.Label);
    }

    [Fact]
    public void Value_code_without_a_label_has_null_label()
    {
        Assert.Null(Assert.Single(Resolve("""{"OperatingAlarmActive":99}""", baseline: false)).Label);
    }

    [Theory]
    [InlineData("\"5\"")]
    [InlineData("5.0")]
    public void String_and_double_values_are_coerced(string raw)
    {
        var e = Assert.Single(Resolve($$"""{"OperatingAlarmActive":{{raw}}}""", baseline: false));
        Assert.Equal(5, e.Value);
        Assert.Equal("Emergency Stop Active", e.Label);
    }

    [Fact]
    public void True_and_false_coerce_to_one_and_zero()
    {
        var events = Resolve("""{"OilFilterAlarm":true,"OilChangeAlarm":false}""", baseline: false);
        Assert.Equal(new long[] { 1, 0 }, events.Select(e => e.Value));
        Assert.Equal(new[] { "triggered", "cleared" }, events.Select(e => e.State));
    }

    [Theory]
    [InlineData("\"abc\"")]
    [InlineData("5.5")]
    [InlineData("null")]
    [InlineData("{}")]
    public void Uncoercible_values_are_skipped(string raw)
    {
        Assert.Empty(Resolve($$"""{"OperatingAlarmActive":{{raw}}}""", baseline: false));
    }

    [Fact]
    public void Unknown_parameter_is_emitted_with_unknown_metadata()
    {
        var e = Assert.Single(Resolve("""{"BrandNewAlarm":1}""", baseline: false));
        Assert.Equal(("unknown", "unknown", "unknown"), (e.EventType, e.Domain, e.Severity));
        Assert.Equal("triggered", e.State);
    }

    [Fact]
    public void Empty_or_missing_block_yields_nothing()
    {
        Assert.Empty(LiveEventResolver.Resolve(null, false, Catalog, NullLogger.Instance));
        Assert.Empty(Resolve("{}", baseline: true));
    }

    [Fact]
    public void Empty_catalog_resolves_everything_as_unknown()
    {
        var e = Assert.Single(LiveEventResolver.Resolve(Block("""{"OperatingAlarmActive":5}"""), false,
            new Dictionary<string, AlertDefinition>(), NullLogger.Instance));
        Assert.Equal("unknown", e.EventType);
        Assert.Null(e.Label);
    }
}
```

`tests/Shape.LiveIngestion.Tests/DeviceStatusDeriverTests.cs`:
```csharp
using System.Text.Json;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class DeviceStatusDeriverTests
{
    private static Dictionary<string, JsonElement> S(string json) =>
        JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());

    [Theory]
    [InlineData("""{"OperatingAlarmActive":0,"OperatingWarningActive":0,"MaintenanceAlarmActive":false,"MaintenanceWarningActive":false}""", 0)]
    [InlineData("""{"OperatingAlarmActive":0,"OperatingWarningActive":21,"MaintenanceAlarmActive":false}""", 1)]
    [InlineData("""{"MaintenanceWarningActive":true}""", 1)]
    [InlineData("""{"OperatingAlarmActive":5,"OperatingWarningActive":21}""", 2)]
    [InlineData("""{"MaintenanceAlarmActive":true,"OperatingWarningActive":21}""", 2)]
    [InlineData("""{"MaintenanceAlarmActive":1}""", 2)]
    [InlineData("""{}""", 0)]
    public void Alarm_outranks_warning(string json, int expected) => Assert.Equal(expected, DeviceStatusDeriver.Derive(S(json)));

    [Fact]
    public void MotorRunning_never_affects_status()
    {
        Assert.Equal(0, DeviceStatusDeriver.Derive(S("""{"MotorRunning":false,"OilFilterAlarm":true}""")));
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveIngestion.Tests`
Expected: FAIL — `The type or namespace name 'AlertDefinitionCatalog' could not be found`.

- [ ] **Step 3: Implement**

`src/Shape.LiveIngestion/LivePolling/IAlertDefinitionCatalog.cs`:
```csharp
namespace Shape.LiveIngestion.LivePolling;

public sealed record AlertDefinition(
    string ParameterName, int AlertCode, string EventType, string Domain, string Severity, string Description,
    IReadOnlyDictionary<int, string>? ValueLabels);

/// <summary>Alert definitions keyed by ParameterName (09-30 §5.1). Static file here instead of SQL (spec §6).</summary>
public interface IAlertDefinitionCatalog
{
    Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default);
}
```

`src/Shape.LiveIngestion/LivePolling/AlertDefinitionCatalog.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>Loads once, holds for the life of the process, never caches a failed load.</summary>
public sealed class AlertDefinitionCatalog(string path, ILogger<AlertDefinitionCatalog> logger) : IAlertDefinitionCatalog
{
    public static readonly string DefaultPath = Path.Combine(AppContext.BaseDirectory, "config", "alert-catalog.json");

    private readonly SemaphoreSlim _gate = new(1, 1);
    private IReadOnlyDictionary<string, AlertDefinition>? _loaded;

    public async Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default)
    {
        if (Volatile.Read(ref _loaded) is { } cached) return cached;
        await _gate.WaitAsync(ct);
        try
        {
            if (_loaded is not null) return _loaded;
            await using var stream = File.OpenRead(path);
            var file = await JsonSerializer.DeserializeAsync<CatalogFile>(stream, new JsonSerializerOptions(JsonSerializerDefaults.Web), ct)
                       ?? throw new InvalidDataException($"Alert catalog '{path}' is empty.");
            var map = file.Definitions.ToDictionary(
                d => d.ParameterName,
                d => new AlertDefinition(d.ParameterName, d.AlertCode, d.EventType, d.Domain, d.Severity, d.Description,
                    d.ValueLabels?.ToDictionary(kv => int.Parse(kv.Key), kv => kv.Value)),
                StringComparer.Ordinal);
            logger.LogInformation("Loaded {Count} alert definitions from {Path}", map.Count, path);
            Volatile.Write(ref _loaded, map);
            return map;
        }
        finally
        {
            _gate.Release();
        }
    }

    private sealed record CatalogFile(List<CatalogEntry> Definitions);

    private sealed record CatalogEntry(
        string ParameterName, int AlertCode, string EventType, string Domain, string Severity, string Description,
        string? Action, Dictionary<string, string>? ValueLabels);
}
```

`src/Shape.LiveIngestion/LivePolling/LiveEvent.cs`:
```csharp
namespace Shape.LiveIngestion.LivePolling;

/// <summary>A resolved event occurrence (09-30 §7.1). Label is display text, null for 16 of 18 parameters.</summary>
public sealed record LiveEvent(string Parameter, long Value, string State, string EventType, string Domain, string Severity, string? Label)
{
    public const string Unknown = "unknown";
}

public static class LiveEventStates
{
    public const string Triggered = "triggered";
    public const string Cleared = "cleared";
    public const string Active = "active";
}
```

`src/Shape.LiveIngestion/LivePolling/LiveEventResolver.cs`:
```csharp
using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Event block → resolved events (09-30 §5.2, §5.3). Baseline: non-zero → active, zero → nothing.
/// Delta: non-zero → triggered, zero → cleared. Label is null on cleared (mirrors the real implementation, 09-30 §12).
/// </summary>
public static class LiveEventResolver
{
    public static IReadOnlyList<LiveEvent> Resolve(
        IReadOnlyDictionary<string, JsonElement>? eventBlock, bool isBaseline,
        IReadOnlyDictionary<string, AlertDefinition> catalog, ILogger logger)
    {
        if (eventBlock is null || eventBlock.Count == 0) return [];
        var events = new List<LiveEvent>();
        foreach (var (parameter, raw) in eventBlock)
        {
            if (!TryCoerce(raw, out var value))
            {
                logger.LogWarning("Event value for {Parameter} is not an integer ({Raw}); skipped", parameter, raw.GetRawText());
                continue;
            }
            if (isBaseline && value == 0) continue;

            var state = isBaseline ? LiveEventStates.Active : value == 0 ? LiveEventStates.Cleared : LiveEventStates.Triggered;
            catalog.TryGetValue(parameter, out var def);
            if (def is null)
                logger.LogWarning("Event parameter {Parameter} has no alert definition; emitting with unknown metadata", parameter);
            string? label = null;
            if (state != LiveEventStates.Cleared && def?.ValueLabels is { } labels && value is >= int.MinValue and <= int.MaxValue)
                labels.TryGetValue((int)value, out label);

            events.Add(new LiveEvent(parameter, value, state,
                def?.EventType ?? LiveEvent.Unknown, def?.Domain ?? LiveEvent.Unknown, def?.Severity ?? LiveEvent.Unknown, label));
        }
        return events;
    }

    private static bool TryCoerce(JsonElement raw, out long value)
    {
        value = 0;
        switch (raw.ValueKind)
        {
            case JsonValueKind.True: value = 1; return true;
            case JsonValueKind.False: value = 0; return true;
            case JsonValueKind.Number:
                if (raw.TryGetInt64(out value)) return true;
                var d = raw.GetDouble();
                if (d == Math.Floor(d) && d is >= long.MinValue and <= long.MaxValue) { value = (long)d; return true; }
                return false;
            case JsonValueKind.String:
                return long.TryParse(raw.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
            default:
                return false;
        }
    }
}
```

`src/Shape.LiveIngestion/LivePolling/DeviceStatusDeriver.cs`:
```csharp
using System.Globalization;
using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Device-level status from the folded snapshot (09-20 §7.4, 09-30 §4): 2 Alarm, 1 Warning, 0 Normal.
/// Needs no catalog; MotorRunning never affects it.
/// </summary>
public static class DeviceStatusDeriver
{
    public static int Derive(IReadOnlyDictionary<string, JsonElement> snapshot)
    {
        if (IsSet(snapshot, "OperatingAlarmActive") || IsSet(snapshot, "MaintenanceAlarmActive")) return 2;
        if (IsSet(snapshot, "OperatingWarningActive") || IsSet(snapshot, "MaintenanceWarningActive")) return 1;
        return 0;
    }

    private static bool IsSet(IReadOnlyDictionary<string, JsonElement> s, string key) =>
        s.TryGetValue(key, out var v) && v.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.Number => v.GetDouble() != 0,
            JsonValueKind.String => double.TryParse(v.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var d) && d != 0,
            _ => false,
        };
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveIngestion.Tests`
Expected: `Passed!  - Failed: 0, Passed: 45` (19 + 3 catalog + 15 resolver + 8 status).

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.LiveIngestion tests/Shape.LiveIngestion.Tests
git commit -m "feat(ingestion): add alert catalog, event resolution and device status derivation"
```

---


### Task 7: SnapshotFolder — pure fold, change points, status rows, events, broadcast entries

**Files:**
- Create: `src/Shape.LiveIngestion/LivePolling/{LiveHead,FoldModels,SnapshotFolder}.cs`
- Test: `tests/Shape.LiveIngestion.Tests/{FoldBuilders,SnapshotFolderTests}.cs`

**Interfaces:**
- Consumes: `LiveTelemetryBatch`, `LiveEntry` (Task 5); `LiveEventResolver`, `DeviceStatusDeriver`, `AlertDefinition`, `LiveEvent` (Task 6).
- Produces:
  - `LiveHead` record: `Guid DeviceGuid`, `string DeviceId`, `Dictionary<string,JsonElement> Snapshot`, `int? DeviceStatus`, `DateTimeOffset? LastSnapshotTimestampUtc`, `bool HasBaseline`, `long TickCount`, `long BytesTotal`, `DateTimeOffset? FirstTickUtc`, `DateTimeOffset? LastTickUtc`, `string? ETag`
  - `ResolvedDevice(Guid DeviceGuid, string SerialNumber)`
  - `ChangePointWrite(string Parameter, DateTimeOffset TimestampUtc, JsonElement Value)`
  - `LiveEventWrite(DateTimeOffset TimestampUtc, LiveEvent Event)`
  - `BroadcastEntry(DateTimeOffset TimestampUtc, int DeviceStatus, Dictionary<string,JsonElement>? Snapshot, Dictionary<string,JsonElement>? Changes, IReadOnlyList<LiveEvent>? Events)`
  - `FoldResult(LiveHead Head, IReadOnlyList<ChangePointWrite> ChangePoints, IReadOnlyList<LiveEventWrite> Events, IReadOnlyList<BroadcastEntry> Broadcast, int Applied, int Backfilled)` with `bool HasWrites`, `int EntityCount` (head + rows)
  - `SnapshotFolder.StatusParameter = "DeviceStatus"`; `SnapshotFolder.Fold(LiveHead? head, LiveTelemetryBatch batch, ResolvedDevice device, IReadOnlyDictionary<string,AlertDefinition> catalog, ILogger logger) → FoldResult`

- [ ] **Step 1: Write failing tests**

`tests/Shape.LiveIngestion.Tests/FoldBuilders.cs`:
```csharp
using System.Text.Json;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public static class FoldBuilders
{
    public static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    public static readonly ResolvedDevice Device = new(Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), "PUMP-00417");

    public static Dictionary<string, JsonElement> M(string json) =>
        JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());

    public static LiveEntry Base(int sec, string body, string? ev = null) => new(T0.AddSeconds(sec), M(body), null, ev is null ? null : M(ev));
    public static LiveEntry Delta(int sec, string changes = "{}", string? ev = null) => new(T0.AddSeconds(sec), null, M(changes), ev is null ? null : M(ev));

    public static LiveTelemetryBatch Batch(params LiveEntry[] entries) =>
        new(TestEvents.Session, entries[^1].TimestampUtc, "gw-001", 700, entries);

    public static Dictionary<string, JsonElement> Seed() =>
        JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "config", "pump-seed.json")))!;
}
```

`tests/Shape.LiveIngestion.Tests/SnapshotFolderTests.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;
using static Shape.LiveIngestion.Tests.FoldBuilders;

namespace Shape.LiveIngestion.Tests;

public class SnapshotFolderTests
{
    private static readonly IReadOnlyDictionary<string, AlertDefinition> Catalog = TestCatalog.Load();

    private static FoldResult Fold(LiveHead? head, LiveTelemetryBatch batch) =>
        SnapshotFolder.Fold(head, batch, Device, Catalog, NullLogger.Instance);

    private const string BaseBody = """{"P":100,"OperatingAlarmActive":0,"OperatingWarningActive":0,"MotorRunning":true}""";
    private const string BaseEvents = """{"OperatingAlarmActive":0,"OperatingWarningActive":0,"MotorRunning":1}""";

    private static FoldResult FirstBatch() => Fold(null, Batch(
        Base(0, BaseBody, BaseEvents),
        Delta(1, """{"P":101}"""),
        Delta(2),
        Delta(3, """{"OperatingWarningActive":21}""", """{"OperatingWarningActive":21}"""),
        Delta(4, """{"P":102}""")));

    [Fact]
    public void First_batch_folds_baseline_and_deltas_into_the_head()
    {
        var r = FirstBatch();

        Assert.Equal(5, r.Applied);
        Assert.True(r.Head.HasBaseline);
        Assert.Equal(102, r.Head.Snapshot["P"].GetInt32());
        Assert.Equal(1, r.Head.DeviceStatus);
        Assert.Equal(5, r.Head.TickCount);
        Assert.Equal(T0, r.Head.FirstTickUtc);
        Assert.Equal(T0.AddSeconds(4), r.Head.LastTickUtc);
        Assert.Equal(T0.AddSeconds(4), r.Head.LastSnapshotTimestampUtc);
        Assert.Equal(700, r.Head.BytesTotal);
        Assert.Equal(Device.DeviceGuid, r.Head.DeviceGuid);
        Assert.Equal("PUMP-00417", r.Head.DeviceId);
    }

    [Fact]
    public void Change_points_are_every_baseline_field_plus_each_changed_field()
    {
        var cps = FirstBatch().ChangePoints.Where(c => c.Parameter != SnapshotFolder.StatusParameter).ToList();

        Assert.Equal(4 + 1 + 1 + 1, cps.Count);   // baseline 4 fields, P@1, warning@3, P@4
        Assert.Contains(cps, c => c.Parameter == "P" && c.TimestampUtc == T0.AddSeconds(1) && c.Value.GetInt32() == 101);
        Assert.DoesNotContain(cps, c => c.TimestampUtc == T0.AddSeconds(2)); // heartbeat writes nothing
    }

    [Fact]
    public void Status_rows_are_written_only_where_status_moves()
    {
        var status = FirstBatch().ChangePoints.Where(c => c.Parameter == SnapshotFolder.StatusParameter).ToList();

        Assert.Equal(new[] { (T0, 0L), (T0.AddSeconds(3), 1L) }, status.Select(s => (s.TimestampUtc, s.Value.GetInt64())));
    }

    [Fact]
    public void Baseline_zeros_write_no_events_and_delta_writes_triggered()
    {
        var events = FirstBatch().Events;

        Assert.Equal(new[] { ("MotorRunning", "active"), ("OperatingWarningActive", "triggered") },
            events.Select(e => (e.Event.Parameter, e.Event.State)));
        Assert.Equal(T0.AddSeconds(3), events[1].TimestampUtc);
        Assert.Equal("Left Overstroke Warning", events[1].Event.Label);
    }

    [Fact]
    public void Broadcast_leads_with_snapshot_then_changes_with_events_only_where_present()
    {
        var b = FirstBatch().Broadcast;

        Assert.Equal(5, b.Count);
        Assert.NotNull(b[0].Snapshot);
        Assert.Null(b[0].Changes);
        Assert.All(b.Skip(1), e => { Assert.Null(e.Snapshot); Assert.NotNull(e.Changes); });
        Assert.Empty(b[2].Changes!);
        Assert.Null(b[1].Events);
        Assert.Equal("triggered", Assert.Single(b[3].Events!).State);
        Assert.Equal(new[] { 0, 0, 0, 1, 1 }, b.Select(e => e.DeviceStatus));
        Assert.Equal("active", Assert.Single(b[0].Events!).State);
    }

    [Fact]
    public void Broadcast_snapshot_is_an_independent_copy()
    {
        var r = FirstBatch();
        r.Broadcast[0].Snapshot!["P"] = JsonSerializer.SerializeToElement(999);
        Assert.Equal(102, r.Head.Snapshot["P"].GetInt32());
    }

    [Fact]
    public void Next_batch_folds_onto_the_stored_head_and_re_materialises_the_snapshot()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Delta(5, """{"P":103}"""), Delta(6), Delta(7), Delta(8), Delta(9)));

        Assert.Equal(5, r.Applied);
        Assert.Equal(10, r.Head.TickCount);
        Assert.Equal(1400, r.Head.BytesTotal);
        Assert.Equal(103, r.Broadcast[0].Snapshot!["P"].GetInt32());
        Assert.Equal(21, r.Broadcast[0].Snapshot!["OperatingWarningActive"].GetInt32());
        Assert.DoesNotContain(r.ChangePoints, c => c.Parameter == SnapshotFolder.StatusParameter);
    }

    [Fact]
    public void Delta_clear_writes_cleared_event_and_status_row()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Delta(5, """{"OperatingWarningActive":0}""", """{"OperatingWarningActive":0}""")));

        var e = Assert.Single(r.Events);
        Assert.Equal("cleared", e.Event.State);
        Assert.Contains(r.ChangePoints, c => c.Parameter == SnapshotFolder.StatusParameter && c.Value.GetInt64() == 0);
    }

    [Fact]
    public void Fully_duplicate_batch_has_no_writes_and_no_broadcast()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Delta(1, """{"P":101}"""), Delta(2), Delta(3), Delta(4)));

        Assert.False(r.HasWrites);
        Assert.Empty(r.Broadcast);
        Assert.Equal(700, r.Head.BytesTotal);
    }

    [Fact]
    public void Overlapping_redelivery_applies_only_new_entries()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Delta(3), Delta(4), Delta(5, """{"P":200}"""), Delta(6)));

        Assert.Equal(2, r.Applied);
        Assert.Equal(T0.AddSeconds(5), r.Broadcast[0].TimestampUtc);
        Assert.Equal(200, r.Broadcast[0].Snapshot!["P"].GetInt32());
    }

    [Fact]
    public void Second_entry_with_same_timestamp_is_skipped()
    {
        var r = Fold(null, Batch(Base(0, BaseBody, BaseEvents), Delta(1, """{"P":1}"""), Delta(1, """{"P":2}""")));

        Assert.Equal(2, r.Applied);
        Assert.Equal(1, r.Head.Snapshot["P"].GetInt32());
        Assert.Single(r.ChangePoints, c => c.Parameter == "P" && c.TimestampUtc == T0.AddSeconds(1));
    }

    [Fact]
    public void Out_of_order_baseline_back_fills_without_overwriting_newer_values()
    {
        // Batch 1 (deltas) arrives before batch 0 (baseline).
        var early = Fold(null, Batch(Delta(5, """{"P":150}"""), Delta(6)));
        Assert.False(early.Head.HasBaseline);

        var r = Fold(early.Head, Batch(Base(0, """{"P":100,"Q":7,"OperatingAlarmActive":5}""", """{"OperatingAlarmActive":5}"""), Delta(1)));

        Assert.True(r.HasWrites);
        Assert.Equal(1, r.Backfilled);
        Assert.Equal(0, r.Applied);
        Assert.True(r.Head.HasBaseline);
        Assert.Equal(150, r.Head.Snapshot["P"].GetInt32());        // newer value kept
        Assert.Equal(7, r.Head.Snapshot["Q"].GetInt32());          // missing field filled
        Assert.Equal(T0.AddSeconds(6), r.Head.LastSnapshotTimestampUtc);
        Assert.Equal(T0, r.Head.FirstTickUtc);
        Assert.Contains(r.ChangePoints, c => c.Parameter == "Q" && c.TimestampUtc == T0);
        Assert.Equal("active", Assert.Single(r.Events).Event.State);
        Assert.Empty(r.Broadcast);
    }

    [Fact]
    public void Second_baseline_mid_session_rebaselines_and_records_active_events()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Base(5, """{"P":500,"OperatingAlarmActive":6,"OperatingWarningActive":0,"MotorRunning":true}""",
            """{"OperatingAlarmActive":6,"MotorRunning":1}""")));

        Assert.Equal(500, r.Head.Snapshot["P"].GetInt32());
        Assert.Equal(2, r.Head.DeviceStatus);
        Assert.All(r.Events, e => Assert.Equal("active", e.Event.State));
        Assert.Equal(T0.AddSeconds(5), r.Events[0].TimestampUtc);
    }

    [Fact]
    public void Real_seed_first_batch_exceeds_the_transaction_limit_but_later_batches_fit()
    {
        var seed = Seed();
        var body = JsonSerializer.Serialize(seed);
        var deltaKeys = new[] { "OperatingPressurePSI", "OperatingPressureBar", "StrokeRateLeft", "StrokeRateRight",
            "StrokeTimeLeft", "StrokeTimeRight", "StrokeCounterTW1", "ProportionalOutputValue" };
        string D(int i) => JsonSerializer.Serialize(deltaKeys.ToDictionary(k => k, _ => i));

        var first = Fold(null, Batch(Base(0, body, """{"MotorRunning":1}"""), Delta(1, D(1)), Delta(2, D(2)), Delta(3, D(3)), Delta(4, D(4))));
        var later = Fold(first.Head, Batch(Delta(5, D(5)), Delta(6, D(6)), Delta(7, D(7)), Delta(8, D(8)), Delta(9, D(9))));

        Assert.Equal(73, seed.Count);
        Assert.True(first.EntityCount > 100, $"first batch = {first.EntityCount} entities");   // F2
        Assert.True(later.EntityCount <= 100, $"later batch = {later.EntityCount} entities");
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveIngestion.Tests --filter FullyQualifiedName~SnapshotFolder`
Expected: FAIL — `The type or namespace name 'FoldResult' could not be found`.

- [ ] **Step 3: Implement**

`src/Shape.LiveIngestion/LivePolling/LiveHead.cs`:
```csharp
using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>The "!current" head row (spec §5.6). ETag null means "not yet stored".</summary>
public sealed record LiveHead
{
    public Guid DeviceGuid { get; init; }
    public string DeviceId { get; init; } = "";
    public Dictionary<string, JsonElement> Snapshot { get; init; } = new();
    public int? DeviceStatus { get; init; }
    public DateTimeOffset? LastSnapshotTimestampUtc { get; init; }
    public bool HasBaseline { get; init; }
    public long TickCount { get; init; }
    public long BytesTotal { get; init; }
    public DateTimeOffset? FirstTickUtc { get; init; }
    public DateTimeOffset? LastTickUtc { get; init; }
    public string? ETag { get; init; }
}
```

`src/Shape.LiveIngestion/LivePolling/FoldModels.cs`:
```csharp
using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

public sealed record ResolvedDevice(Guid DeviceGuid, string SerialNumber);

public sealed record ChangePointWrite(string Parameter, DateTimeOffset TimestampUtc, JsonElement Value);

public sealed record LiveEventWrite(DateTimeOffset TimestampUtc, LiveEvent Event);

/// <summary>One entry of the outgoing SignalR batch. Exactly one of Snapshot/Changes; Events null when none.</summary>
public sealed record BroadcastEntry(
    DateTimeOffset TimestampUtc, int DeviceStatus,
    Dictionary<string, JsonElement>? Snapshot, Dictionary<string, JsonElement>? Changes, IReadOnlyList<LiveEvent>? Events);

public sealed record FoldResult(
    LiveHead Head, IReadOnlyList<ChangePointWrite> ChangePoints, IReadOnlyList<LiveEventWrite> Events,
    IReadOnlyList<BroadcastEntry> Broadcast, int Applied, int Backfilled)
{
    public bool HasWrites => Applied + Backfilled > 0;

    /// <summary>Entities one commit writes: the head row plus every change-point and event row.</summary>
    public int EntityCount => 1 + ChangePoints.Count + Events.Count;
}
```

`src/Shape.LiveIngestion/LivePolling/SnapshotFolder.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Pure fold of one gateway batch onto the stored head (spec §5.1 step 5–6, 09-20 §5.2, 09-30 §5).
/// Entries not newer than LastSnapshotTimestampUtc are skipped, except an older baseline when none has been folded
/// yet, which is back-filled. Never mutates its inputs.
/// </summary>
public static class SnapshotFolder
{
    public const string StatusParameter = "DeviceStatus";

    public static FoldResult Fold(
        LiveHead? head, LiveTelemetryBatch batch, ResolvedDevice device,
        IReadOnlyDictionary<string, AlertDefinition> catalog, ILogger logger)
    {
        var snapshot = head is null ? new Dictionary<string, JsonElement>() : new Dictionary<string, JsonElement>(head.Snapshot);
        var hasBaseline = head?.HasBaseline ?? false;
        var last = head?.LastSnapshotTimestampUtc;
        var status = head?.DeviceStatus;
        var ticks = head?.TickCount ?? 0;
        var firstTick = head?.FirstTickUtc;
        var lastTick = head?.LastTickUtc;

        var changePoints = new List<ChangePointWrite>();
        var events = new List<LiveEventWrite>();
        var broadcast = new List<BroadcastEntry>();
        int applied = 0, backfilled = 0;

        foreach (var entry in batch.Entries.OrderBy(e => e.TimestampUtc))
        {
            var ts = entry.TimestampUtc;
            if (last is not null && ts <= last)
            {
                if (entry.Body is null || hasBaseline) continue;   // duplicate / overlapping redelivery
                // Out-of-order baseline: record it at its own timestamp without moving the head backwards.
                foreach (var (k, v) in entry.Body)
                {
                    changePoints.Add(new ChangePointWrite(k, ts, v));
                    snapshot.TryAdd(k, v);
                }
                changePoints.Add(new ChangePointWrite(StatusParameter, ts, StatusValue(DeviceStatusDeriver.Derive(entry.Body))));
                foreach (var e in LiveEventResolver.Resolve(entry.Event, isBaseline: true, catalog, logger))
                    events.Add(new LiveEventWrite(ts, e));
                hasBaseline = true;
                ticks++;
                if (firstTick is null || ts < firstTick) firstTick = ts;
                backfilled++;
                continue;
            }

            if (entry.Body is not null)
            {
                snapshot = new Dictionary<string, JsonElement>(entry.Body);
                hasBaseline = true;
                foreach (var (k, v) in entry.Body) changePoints.Add(new ChangePointWrite(k, ts, v));
            }
            else
            {
                foreach (var (k, v) in entry.Changes!)
                {
                    snapshot[k] = v;
                    changePoints.Add(new ChangePointWrite(k, ts, v));
                }
            }

            var entryStatus = DeviceStatusDeriver.Derive(snapshot);
            if (entryStatus != status)
            {
                changePoints.Add(new ChangePointWrite(StatusParameter, ts, StatusValue(entryStatus)));
                status = entryStatus;
            }

            var resolved = LiveEventResolver.Resolve(entry.Event, isBaseline: entry.Body is not null, catalog, logger);
            foreach (var e in resolved) events.Add(new LiveEventWrite(ts, e));

            var isFirstOut = broadcast.Count == 0;
            broadcast.Add(new BroadcastEntry(
                ts, entryStatus,
                isFirstOut ? new Dictionary<string, JsonElement>(snapshot) : null,
                isFirstOut ? null : new Dictionary<string, JsonElement>(entry.Changes ?? entry.Body!),
                resolved.Count > 0 ? resolved : null));

            ticks++;
            firstTick ??= ts;
            lastTick = ts;
            last = ts;
            applied++;
        }

        var newHead = new LiveHead
        {
            DeviceGuid = device.DeviceGuid,
            DeviceId = device.SerialNumber,
            Snapshot = snapshot,
            DeviceStatus = status,
            LastSnapshotTimestampUtc = last,
            HasBaseline = hasBaseline,
            TickCount = ticks,
            BytesTotal = (head?.BytesTotal ?? 0) + (applied + backfilled > 0 ? batch.CompressedBytes : 0),
            FirstTickUtc = firstTick,
            LastTickUtc = lastTick,
            ETag = head?.ETag,
        };
        return new FoldResult(newHead, changePoints, events, broadcast, applied, backfilled);
    }

    private static JsonElement StatusValue(int status) => JsonSerializer.SerializeToElement((long)status);
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveIngestion.Tests --filter FullyQualifiedName~SnapshotFolder`
Expected: `Passed!  - Failed: 0, Passed: 14`. Note the entity count printed by the transaction-limit test's message if it ever fails; record the measured first-batch count in the ledger for `docs/findings.md` F2.

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.LiveIngestion tests/Shape.LiveIngestion.Tests
git commit -m "feat(ingestion): add snapshot folder with ordering guard, back-fill and event resolution"
```

---


### Task 8: `TableLiveSessionStore` — head load, single or chunked commit, partition delete

**Files:**
- Create: `src/Shape.LiveIngestion/LivePolling/{ILiveSessionStore,LiveRowMapper,TableLiveSessionStore}.cs`
- Test: `tests/Shape.LiveIngestion.Tests/{DevStorage,LiveRowMapperTests,TableLiveSessionStoreTests}.cs`

**Interfaces:**
- Consumes: `LiveSessionKeys` (Task 5); `LiveHead`, `ChangePointWrite`, `LiveEventWrite`, `SnapshotFolder`, `FoldResult` (Task 7).
- Produces:
  - `ILiveSessionStore`: `Task<LiveHead?> LoadHeadAsync(Guid sessionId, CancellationToken ct = default)`; `Task WriteBatchAsync(Guid sessionId, LiveHead head, IReadOnlyList<ChangePointWrite> changePoints, IReadOnlyList<LiveEventWrite> events, CancellationToken ct = default)`; `Task<int> DeletePartitionAsync(Guid sessionId, CancellationToken ct = default)`
  - `LiveHeadConflictException : Exception` — thrown on `409`/`412` for the head row
  - `TableLiveSessionStore(TableServiceClient service, string tableName = TableLiveSessionStore.DefaultTable)`; `DefaultTable = "livesessions"`; `MaxBatch = 100`
  - `LiveRowMapper` (public static): `PartitionKey(Guid)`, `Head(string pk, LiveHead)`, `ReadHead(TableEntity)`, `ChangePoint(string pk, ChangePointWrite)`, `Event(string pk, LiveEventWrite)`. Column contract (spec §5.6): change point `V` native + `T` ∈ `b`/`i`/`d`/`s` (`n` with no `V` for a JSON null) + `TimestampUtc`; event row `Parameter`, `Value` Int64, `State`, `EventType`, `Domain`, `Severity`, `Label` (absent when null), `TimestampUtc`
  - Test helper `DevStorage.ConnectionString`, `DevStorage.Service()`, `DevStorage.UniqueTable()`

**Azurite is required for the `Integration` tests from this task on.** Start it in its own window and leave it running:
```powershell
npx --yes azurite --silent --location .azurite --blobPort 10100 --queuePort 10101 --tablePort 10102
```

- [ ] **Step 1: Write failing tests**

```powershell
dotnet add tests/Shape.LiveIngestion.Tests package Azure.Data.Tables --version 12.12.0
```

`tests/Shape.LiveIngestion.Tests/DevStorage.cs`:
```csharp
using Azure.Data.Tables;

namespace Shape.LiveIngestion.Tests;

/// <summary>Azurite on 10100–10102 (Global Constraints). Each test uses its own table.</summary>
public static class DevStorage
{
    public const string ConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;";

    public static TableServiceClient Service() => new(ConnectionString);

    public static string UniqueTable() => "t" + Guid.NewGuid().ToString("N")[..20];
}
```

`tests/Shape.LiveIngestion.Tests/LiveRowMapperTests.cs`:
```csharp
using System.Text.Json;
using Shape.LiveIngestion.LivePolling;
using static Shape.LiveIngestion.Tests.FoldBuilders;

namespace Shape.LiveIngestion.Tests;

public class LiveRowMapperTests
{
    private const string Pk = "cc27db6b-508b-488e-a816-faae684491d8";

    [Theory]
    [InlineData("true", "b", typeof(bool))]
    [InlineData("54372", "i", typeof(long))]
    [InlineData("12.5", "d", typeof(double))]
    [InlineData("\"OK\"", "s", typeof(string))]
    public void Change_point_stores_native_value_and_type_code(string json, string code, Type clr)
    {
        var e = LiveRowMapper.ChangePoint(Pk, new ChangePointWrite("P", T0, JsonDocument.Parse(json).RootElement.Clone()));

        Assert.Equal("P|2026-10-01T10:00:00.0000000Z", e.RowKey);
        Assert.Equal(code, e.GetString("T"));
        Assert.IsType(clr, e["V"]);
        Assert.Equal(T0, e.GetDateTimeOffset("TimestampUtc"));
    }

    [Fact]
    public void Json_null_is_stored_as_type_n_without_value()
    {
        var e = LiveRowMapper.ChangePoint(Pk, new ChangePointWrite("P", T0, JsonDocument.Parse("null").RootElement.Clone()));
        Assert.Equal("n", e.GetString("T"));
        Assert.False(e.ContainsKey("V"));
    }

    [Fact]
    public void Event_row_omits_label_when_null_and_keeps_it_when_present()
    {
        var unlabelled = LiveRowMapper.Event(Pk, new LiveEventWrite(T0,
            new LiveEvent("MaintenanceAlarmActive", 1, "triggered", "alarm", "maintenance", "critical", null)));
        var labelled = LiveRowMapper.Event(Pk, new LiveEventWrite(T0,
            new LiveEvent("OperatingAlarmActive", 5, "active", "alarm", "operational", "critical", "Emergency Stop Active")));

        Assert.Equal("evt|2026-10-01T10:00:00.0000000Z|MaintenanceAlarmActive", unlabelled.RowKey);
        Assert.False(unlabelled.ContainsKey("Label"));
        Assert.Equal(1L, unlabelled["Value"]);
        Assert.Equal("Emergency Stop Active", labelled.GetString("Label"));
        Assert.Equal("active", labelled.GetString("State"));
    }

    [Fact]
    public void Head_round_trips_through_the_entity()
    {
        var head = new LiveHead
        {
            DeviceGuid = Device.DeviceGuid, DeviceId = "PUMP-00417", Snapshot = M("""{"P":1,"OK":true,"S":"x"}"""),
            DeviceStatus = 2, LastSnapshotTimestampUtc = T0.AddSeconds(4), HasBaseline = true, TickCount = 5,
            BytesTotal = 700, FirstTickUtc = T0, LastTickUtc = T0.AddSeconds(4),
        };

        var back = LiveRowMapper.ReadHead(LiveRowMapper.Head(Pk, head));

        Assert.Equal(head with { Snapshot = back.Snapshot, ETag = back.ETag }, back);
        Assert.Equal("""{"P":1,"OK":true,"S":"x"}""", JsonSerializer.Serialize(back.Snapshot));
    }

    [Fact]
    public void Head_before_first_tick_has_no_status_or_tick_columns()
    {
        var e = LiveRowMapper.Head(Pk, new LiveHead { DeviceGuid = Device.DeviceGuid, DeviceId = "PUMP-00417" });

        Assert.Equal("!current", e.RowKey);
        Assert.False(e.ContainsKey("DeviceStatus"));
        Assert.False(e.ContainsKey("FirstTickUtc"));
        Assert.Null(LiveRowMapper.ReadHead(e).DeviceStatus);
    }
}
```

`tests/Shape.LiveIngestion.Tests/TableLiveSessionStoreTests.cs`:
```csharp
using System.Text.Json;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;
using static Shape.LiveIngestion.Tests.FoldBuilders;

namespace Shape.LiveIngestion.Tests;

[Trait("Category", "Integration")]
public class TableLiveSessionStoreTests
{
    private static readonly IReadOnlyDictionary<string, AlertDefinition> Catalog = TestCatalog.Load();
    private readonly string _table = DevStorage.UniqueTable();
    private readonly TableLiveSessionStore _store;
    private readonly Guid _session = Guid.NewGuid();

    public TableLiveSessionStoreTests() => _store = new TableLiveSessionStore(DevStorage.Service(), _table);

    private static FoldResult Fold(LiveHead? head, params LiveEntry[] entries) =>
        SnapshotFolder.Fold(head, Batch(entries), Device, Catalog, NullLogger.Instance);

    private static FoldResult SmallFirst() => Fold(null,
        Base(0, """{"P":100,"OperatingAlarmActive":0,"OK":true,"S":"a"}""", """{"OperatingAlarmActive":0,"MotorRunning":1}"""),
        Delta(1, """{"P":101}"""), Delta(2), Delta(3, """{"OperatingAlarmActive":6}""", """{"OperatingAlarmActive":6}"""), Delta(4));

    private static FoldResult LargeFirst()
    {
        var keys = new[] { "OperatingPressurePSI", "OperatingPressureBar", "StrokeRateLeft", "StrokeRateRight",
            "StrokeTimeLeft", "StrokeTimeRight", "StrokeCounterTW1", "ProportionalOutputValue" };
        string D(int i) => JsonSerializer.Serialize(keys.ToDictionary(k => k, _ => i));
        return Fold(null, Base(0, JsonSerializer.Serialize(Seed()), """{"MotorRunning":1}"""),
            Delta(1, D(1)), Delta(2, D(2)), Delta(3, D(3)), Delta(4, D(4)));
    }

    private Task Write(FoldResult r) => _store.WriteBatchAsync(_session, r.Head, r.ChangePoints, r.Events);

    private async Task<List<TableEntity>> Rows()
    {
        var client = DevStorage.Service().GetTableClient(_table);
        var rows = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>(TableClient.CreateQueryFilter($"PartitionKey eq {_session.ToString()}")))
            rows.Add(e);
        return rows;
    }

    [Fact]
    public async Task Load_head_returns_null_for_a_new_session() =>
        Assert.Null(await _store.LoadHeadAsync(_session));

    [Fact]
    public async Task Small_batch_commits_head_and_rows_together()
    {
        var r = SmallFirst();
        Assert.True(r.EntityCount <= 100);

        await Write(r);

        var head = await _store.LoadHeadAsync(_session);
        Assert.NotNull(head);
        Assert.NotNull(head!.ETag);
        Assert.Equal(2, head.DeviceStatus);
        Assert.Equal(101, head.Snapshot["P"].GetInt32());
        Assert.Equal(r.EntityCount, (await Rows()).Count);
    }

    [Fact]
    public async Task Next_batch_with_the_loaded_etag_commits()
    {
        await Write(SmallFirst());
        var head = await _store.LoadHeadAsync(_session);

        var next = Fold(head, Delta(5, """{"P":102}"""), Delta(6), Delta(7), Delta(8), Delta(9));
        await Write(next);

        var after = await _store.LoadHeadAsync(_session);
        Assert.Equal(10, after!.TickCount);
        Assert.NotEqual(head!.ETag, after.ETag);
    }

    [Fact]
    public async Task Stale_etag_conflicts_and_the_transaction_writes_no_rows()
    {
        await Write(SmallFirst());
        var head = await _store.LoadHeadAsync(_session);
        await Write(Fold(head, Delta(5, """{"P":102}""")));               // moves the ETag on
        var before = (await Rows()).Count;

        var stale = Fold(head, Delta(5, """{"P":999}"""), Delta(6, """{"Q":1}"""));
        await Assert.ThrowsAsync<LiveHeadConflictException>(() => Write(stale));

        Assert.Equal(before, (await Rows()).Count);
        Assert.DoesNotContain(await Rows(), e => e.RowKey.StartsWith("Q|"));
    }

    [Fact]
    public async Task Two_writers_adding_the_head_conflict()
    {
        await Write(SmallFirst());
        await Assert.ThrowsAsync<LiveHeadConflictException>(() => Write(SmallFirst()));
    }

    [Fact]
    public async Task Large_first_batch_takes_the_chunked_path_and_commits()
    {
        var r = LargeFirst();
        Assert.True(r.EntityCount > 100);

        await Write(r);

        Assert.Equal(r.EntityCount, (await Rows()).Count);
        Assert.Equal(5, (await _store.LoadHeadAsync(_session))!.TickCount);
    }

    [Fact]
    public async Task Chunked_commit_retried_after_head_conflict_produces_no_duplicate_rows()
    {
        var r = LargeFirst();
        await Write(r);
        var stored = await _store.LoadHeadAsync(_session);

        // A redelivered copy with a stale view of the head: rows are upserted, then the head add conflicts.
        await Assert.ThrowsAsync<LiveHeadConflictException>(() => Write(r));
        // The retry re-folds on the stored head: every entry is not-newer, so nothing is written.
        var retry = Fold(stored, Delta(4));
        Assert.False(retry.HasWrites);

        var rows = await Rows();
        Assert.Equal(r.EntityCount, rows.Count);
        Assert.Equal(rows.Count, rows.Select(e => e.RowKey).Distinct().Count());
    }

    [Fact]
    public async Task Delete_partition_removes_more_than_100_rows()
    {
        var r = LargeFirst();
        await Write(r);

        var deleted = await _store.DeletePartitionAsync(_session);

        Assert.Equal(r.EntityCount, deleted);
        Assert.Empty(await Rows());
        Assert.Null(await _store.LoadHeadAsync(_session));
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveIngestion.Tests --filter "FullyQualifiedName~LiveRowMapper|FullyQualifiedName~TableLiveSessionStore"`
Expected: FAIL — `The type or namespace name 'LiveRowMapper' could not be found`.

- [ ] **Step 3: Implement**

`src/Shape.LiveIngestion/LivePolling/ILiveSessionStore.cs`:
```csharp
namespace Shape.LiveIngestion.LivePolling;

public interface ILiveSessionStore
{
    Task<LiveHead?> LoadHeadAsync(Guid sessionId, CancellationToken ct = default);

    /// <summary>Commits one folded batch. Throws <see cref="LiveHeadConflictException"/> when the head moved.</summary>
    Task WriteBatchAsync(Guid sessionId, LiveHead head, IReadOnlyList<ChangePointWrite> changePoints,
        IReadOnlyList<LiveEventWrite> events, CancellationToken ct = default);

    /// <summary>Deletes every row of the session's partition. Returns the number deleted.</summary>
    Task<int> DeletePartitionAsync(Guid sessionId, CancellationToken ct = default);
}

public sealed class LiveHeadConflictException(string message, Exception inner) : Exception(message, inner);
```

`src/Shape.LiveIngestion/LivePolling/LiveRowMapper.cs`:
```csharp
using System.Text.Json;
using Azure;
using Azure.Data.Tables;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>Entity shapes of spec §5.6. The backend reader (Task 12) decodes the same columns.</summary>
public static class LiveRowMapper
{
    public static string PartitionKey(Guid sessionId) => sessionId.ToString();

    public static TableEntity Head(string pk, LiveHead h)
    {
        var e = new TableEntity(pk, LiveSessionKeys.HeadRowKey)
        {
            ["DeviceGuid"] = h.DeviceGuid,
            ["DeviceId"] = h.DeviceId,
            ["Snapshot"] = JsonSerializer.Serialize(h.Snapshot),
            ["HasBaseline"] = h.HasBaseline,
            ["TickCount"] = h.TickCount,
            ["BytesTotal"] = h.BytesTotal,
        };
        if (h.DeviceStatus is { } s) e["DeviceStatus"] = s;
        if (h.LastSnapshotTimestampUtc is { } l) e["LastSnapshotTimestampUtc"] = l;
        if (h.FirstTickUtc is { } f) e["FirstTickUtc"] = f;
        if (h.LastTickUtc is { } t) e["LastTickUtc"] = t;
        return e;
    }

    public static LiveHead ReadHead(TableEntity e) => new()
    {
        DeviceGuid = e.GetGuid("DeviceGuid") ?? Guid.Empty,
        DeviceId = e.GetString("DeviceId") ?? "",
        Snapshot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(e.GetString("Snapshot") ?? "{}")!,
        DeviceStatus = e.GetInt32("DeviceStatus"),
        LastSnapshotTimestampUtc = e.GetDateTimeOffset("LastSnapshotTimestampUtc"),
        HasBaseline = e.GetBoolean("HasBaseline") ?? false,
        TickCount = e.GetInt64("TickCount") ?? 0,
        BytesTotal = e.GetInt64("BytesTotal") ?? 0,
        FirstTickUtc = e.GetDateTimeOffset("FirstTickUtc"),
        LastTickUtc = e.GetDateTimeOffset("LastTickUtc"),
        ETag = e.ETag == default ? null : e.ETag.ToString(),
    };

    public static TableEntity ChangePoint(string pk, ChangePointWrite c)
    {
        var e = new TableEntity(pk, LiveSessionKeys.ChangeRowKey(c.Parameter, c.TimestampUtc)) { ["TimestampUtc"] = c.TimestampUtc };
        var v = c.Value;
        switch (v.ValueKind)
        {
            case JsonValueKind.True or JsonValueKind.False: e["V"] = v.GetBoolean(); e["T"] = "b"; break;
            case JsonValueKind.Number when v.TryGetInt64(out var i): e["V"] = i; e["T"] = "i"; break;
            case JsonValueKind.Number: e["V"] = v.GetDouble(); e["T"] = "d"; break;
            case JsonValueKind.String: e["V"] = v.GetString(); e["T"] = "s"; break;
            case JsonValueKind.Null: e["T"] = "n"; break;
            default: e["V"] = v.GetRawText(); e["T"] = "s"; break;
        }
        return e;
    }

    public static TableEntity Event(string pk, LiveEventWrite w)
    {
        var ev = w.Event;
        var e = new TableEntity(pk, LiveSessionKeys.EventRowKey(ev.Parameter, w.TimestampUtc))
        {
            ["Parameter"] = ev.Parameter,
            ["Value"] = ev.Value,
            ["State"] = ev.State,
            ["EventType"] = ev.EventType,
            ["Domain"] = ev.Domain,
            ["Severity"] = ev.Severity,
            ["TimestampUtc"] = w.TimestampUtc,
        };
        if (ev.Label is not null) e["Label"] = ev.Label;
        return e;
    }

    internal static ETag ToETag(string etag) => new(etag);
}
```

`src/Shape.LiveIngestion/LivePolling/TableLiveSessionStore.cs`:
```csharp
using Azure;
using Azure.Data.Tables;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Spec §5.1 step 7. ≤ 100 entities: one entity-group transaction, head first. > 100: rows upserted in chunks of
/// ≤ 100 (idempotent — deterministic keys and values), then the head alone with If-Match as the commit point.
/// </summary>
public sealed class TableLiveSessionStore(TableServiceClient service, string tableName = TableLiveSessionStore.DefaultTable)
    : ILiveSessionStore
{
    public const string DefaultTable = "livesessions";
    public const int MaxBatch = 100;

    private readonly TableClient _table = service.GetTableClient(tableName);
    private int _ensured;

    public async Task<LiveHead?> LoadHeadAsync(Guid sessionId, CancellationToken ct = default)
    {
        await EnsureTableAsync(ct);
        var r = await _table.GetEntityIfExistsAsync<TableEntity>(LiveRowMapper.PartitionKey(sessionId), LiveSessionKeys.HeadRowKey,
            cancellationToken: ct);
        return r.HasValue ? LiveRowMapper.ReadHead(r.Value!) : null;
    }

    public async Task WriteBatchAsync(Guid sessionId, LiveHead head, IReadOnlyList<ChangePointWrite> changePoints,
        IReadOnlyList<LiveEventWrite> events, CancellationToken ct = default)
    {
        await EnsureTableAsync(ct);
        var pk = LiveRowMapper.PartitionKey(sessionId);
        var rows = changePoints.Select(c => LiveRowMapper.ChangePoint(pk, c))
            .Concat(events.Select(e => LiveRowMapper.Event(pk, e)))
            .GroupBy(e => e.RowKey).Select(g => g.Last())         // a transaction rejects a repeated RowKey
            .Select(e => new TableTransactionAction(TableTransactionActionType.UpsertReplace, e))
            .ToList();
        var headEntity = LiveRowMapper.Head(pk, head);
        var headAction = head.ETag is null
            ? new TableTransactionAction(TableTransactionActionType.Add, headEntity)
            : new TableTransactionAction(TableTransactionActionType.UpdateReplace, headEntity, LiveRowMapper.ToETag(head.ETag));

        try
        {
            if (rows.Count + 1 <= MaxBatch)
            {
                await _table.SubmitTransactionAsync(rows.Prepend(headAction), ct);
                return;
            }
            foreach (var chunk in rows.Chunk(MaxBatch))
                await _table.SubmitTransactionAsync(chunk, ct);
            await _table.SubmitTransactionAsync([headAction], ct);
        }
        catch (RequestFailedException ex) when (IsHeadConflict(ex))
        {
            throw new LiveHeadConflictException($"Head row of session {sessionId} changed concurrently ({ex.Status} {ex.ErrorCode}).", ex);
        }
    }

    public async Task<int> DeletePartitionAsync(Guid sessionId, CancellationToken ct = default)
    {
        await EnsureTableAsync(ct);
        var pk = LiveRowMapper.PartitionKey(sessionId);
        var keys = new List<string>();
        await foreach (var e in _table.QueryAsync<TableEntity>(TableClient.CreateQueryFilter($"PartitionKey eq {pk}"),
                           select: ["RowKey"], cancellationToken: ct))
            keys.Add(e.RowKey);

        foreach (var chunk in keys.Chunk(MaxBatch))
            await _table.SubmitTransactionAsync(
                chunk.Select(rk => new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity(pk, rk), ETag.All)), ct);
        return keys.Count;
    }

    private static bool IsHeadConflict(RequestFailedException ex) =>
        ex.Status is 409 or 412 || ex.ErrorCode is "EntityAlreadyExists" or "UpdateConditionNotSatisfied";

    private async Task EnsureTableAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _ensured) == 1) return;
        await _table.CreateIfNotExistsAsync(ct);
        Volatile.Write(ref _ensured, 1);
    }
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveIngestion.Tests --filter "FullyQualifiedName~LiveRowMapper|FullyQualifiedName~TableLiveSessionStore"`
Expected: `Passed!  - Failed: 0, Passed: 16` (mapper 8 — the theory counts 4 — plus store 8). If a conflict test fails with a `TableTransactionFailedException` whose `Status` is `400`, log `ex.Status`/`ex.ErrorCode` once and extend `IsHeadConflict` with the code Azurite reports; do not widen it to all `400`s (an over-limit transaction is also a `400` and must not be retried as a conflict).

Run the whole project to make sure nothing else moved: `dotnet test tests/Shape.LiveIngestion.Tests`
Expected: `Passed!  - Failed: 0, Passed: 75` (45 + 14 fold + 16 here).

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.LiveIngestion tests/Shape.LiveIngestion.Tests
git commit -m "feat(ingestion): add table live session store with chunked commit and partition delete"
```

---


### Task 9: Broadcast encoder, `LiveTelemetryProcessor`, Event Grid function and negotiator

**Files:**
- Create: `src/Shape.LiveIngestion/Infrastructure/UtcDateTimeOffsetConverter.cs`
- Create: `src/Shape.LiveIngestion/LivePolling/{DeviceConfig,DeviceResolver,SignalRNames,BroadcastEncoder,LiveTelemetryProcessor}.cs`
- Create: `src/Shape.LiveIngestion/Functions/{LiveTelemetryProcessorFunction,NegotiatorFunctions}.cs`
- Modify: `src/Shape.LiveIngestion/Program.cs`
- Test: `tests/Shape.LiveIngestion.Tests/{FakeLiveSessionStore,BroadcastEncoderTests,LiveTelemetryProcessorTests}.cs`

**Interfaces:**
- Consumes: `LiveEventGridDecoder`, `DecodeStatus` (Task 5); `IAlertDefinitionCatalog`, `AlertDefinition` (Task 6); `SnapshotFolder`, `BroadcastEntry`, `ResolvedDevice`, `LiveHead` (Task 7); `ILiveSessionStore`, `LiveHeadConflictException`, `TableLiveSessionStore` (Task 8).
- Produces:
  - `UtcDateTimeOffsetConverter : JsonConverter<DateTimeOffset>` — writes `yyyy-MM-ddTHH:mm:ss.fffffffZ` (UTC)
  - `DeviceConfig(string SerialNumber, Guid DeviceGuid, string Name, string? GatewayId, string ModuleId)`; `DeviceConfig.Load(string path)`; `DeviceConfig.DefaultPath`
  - `DeviceResolver(DeviceConfig)`; `bool TryResolve(string gatewayId, out ResolvedDevice device)`
  - `SignalRNames.Hub = "iot_telemetry"`, `Target = "liveTelemetryUpdate"`, `ConnectionSetting = "AzureSignalRConnectionString"`, `DeviceGroup(Guid) → "telemetry:device:{guid}"`, `DeviceGroup(string)`
  - `LiveEnvelope(string Encoding, string Payload)` serialised as `{"encoding","payload"}`; `BroadcastEncoder.Encoding = "gzip+base64"`, `BroadcastEncoder.Json` (options), `BroadcastEncoder.BatchJson(Guid sessionId, Guid deviceGuid, DateTimeOffset batchSentUtc, IReadOnlyList<BroadcastEntry>) → string`, `BroadcastEncoder.Encode(...same...) → LiveEnvelope`
  - `LiveBroadcast(string GroupName, LiveEnvelope Envelope)`; `LiveTelemetryProcessor(DeviceResolver, IAlertDefinitionCatalog, ILiveSessionStore, ILogger<LiveTelemetryProcessor>)`; `Task<LiveBroadcast?> ProcessAsync(string eventJson, CancellationToken ct = default)`; `MaxRetries = 3`
  - Functions: `LiveTelemetryProcessorEventGrid`, `negotiate` (`POST /api/negotiate`), `join-device`, `leave-device` (`POST /api/{name}?deviceId=`)

- [ ] **Step 1: Write failing tests**

`tests/Shape.LiveIngestion.Tests/FakeLiveSessionStore.cs`:
```csharp
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

/// <summary>In-memory store with ETag semantics and injectable conflicts.</summary>
public sealed class FakeLiveSessionStore : ILiveSessionStore
{
    public LiveHead? Head { get; set; }
    public int Loads { get; private set; }
    public int Writes { get; private set; }
    public int ConflictsToThrow { get; set; }
    public List<ChangePointWrite> ChangePoints { get; } = [];
    public List<LiveEventWrite> Events { get; } = [];

    public Task<LiveHead?> LoadHeadAsync(Guid sessionId, CancellationToken ct = default)
    {
        Loads++;
        return Task.FromResult(Head);
    }

    public Task WriteBatchAsync(Guid sessionId, LiveHead head, IReadOnlyList<ChangePointWrite> changePoints,
        IReadOnlyList<LiveEventWrite> events, CancellationToken ct = default)
    {
        if (ConflictsToThrow > 0)
        {
            ConflictsToThrow--;
            throw new LiveHeadConflictException("injected", new InvalidOperationException());
        }
        if (head.ETag != Head?.ETag) throw new LiveHeadConflictException("etag", new InvalidOperationException());
        Writes++;
        Head = head with { ETag = Guid.NewGuid().ToString() };
        ChangePoints.AddRange(changePoints);
        Events.AddRange(events);
        return Task.CompletedTask;
    }

    public Task<int> DeletePartitionAsync(Guid sessionId, CancellationToken ct = default)
    {
        var n = ChangePoints.Count + Events.Count + (Head is null ? 0 : 1);
        Head = null;
        ChangePoints.Clear();
        Events.Clear();
        return Task.FromResult(n);
    }
}
```

`tests/Shape.LiveIngestion.Tests/BroadcastEncoderTests.cs`:
```csharp
using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Shape.LiveIngestion.LivePolling;
using static Shape.LiveIngestion.Tests.FoldBuilders;

namespace Shape.LiveIngestion.Tests;

public class BroadcastEncoderTests
{
    private static readonly Guid Session = TestEvents.Session;

    public static string Decode(LiveEnvelope envelope)
    {
        using var gz = new GZipStream(new MemoryStream(Convert.FromBase64String(envelope.Payload)), CompressionMode.Decompress);
        using var reader = new StreamReader(gz, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private static IReadOnlyList<BroadcastEntry> Entries() =>
    [
        new(T0, 0, M("""{"MotorRunning":true,"OperatingPressurePSI":54351}"""), null, null),
        new(T0.AddSeconds(1), 0, null, M("""{"OperatingPressurePSI":54360}"""), null),
        new(T0.AddSeconds(2), 0, null, M("{}"), null),
        new(T0.AddSeconds(3), 1, null, M("""{"OperatingWarningActive":21}"""),
            [new LiveEvent("OperatingWarningActive", 21, "triggered", "warning", "operational", "major", "Left Overstroke Warning")]),
        new(T0.AddSeconds(4), 1, null, M("{}"),
            [new LiveEvent("MaintenanceAlarmActive", 1, "triggered", "alarm", "maintenance", "critical", null)]),
    ];

    private static JsonElement Batch() =>
        JsonDocument.Parse(Decode(BroadcastEncoder.Encode(Session, Device.DeviceGuid, T0.AddSeconds(5), Entries()))).RootElement;

    [Fact]
    public void Envelope_is_gzip_base64_with_lowercase_property_names()
    {
        var envelope = BroadcastEncoder.Encode(Session, Device.DeviceGuid, T0.AddSeconds(5), Entries());
        var json = JsonSerializer.Serialize(envelope);

        Assert.Equal("gzip+base64", envelope.Encoding);
        Assert.StartsWith("""{"encoding":"gzip+base64","payload":"H4sI""", json);
    }

    [Fact]
    public void Batch_has_session_device_and_sent_time()
    {
        var b = Batch();
        Assert.Equal(Session.ToString(), b.GetProperty("sessionId").GetString());
        Assert.Equal(Device.DeviceGuid.ToString(), b.GetProperty("deviceGuid").GetString());
        Assert.Equal("2026-10-01T10:00:05.0000000Z", b.GetProperty("batchSentUtc").GetString());
    }

    [Fact]
    public void Every_entry_carries_all_five_keys_with_explicit_nulls()
    {
        foreach (var e in Batch().GetProperty("entries").EnumerateArray())
            Assert.Equal(new[] { "timestampUtc", "deviceStatus", "snapshot", "changes", "events" },
                e.EnumerateObject().Select(p => p.Name));

        var entries = Batch().GetProperty("entries");
        Assert.Equal(JsonValueKind.Null, entries[0].GetProperty("changes").ValueKind);
        Assert.Equal(JsonValueKind.Null, entries[1].GetProperty("snapshot").ValueKind);
        Assert.Equal(JsonValueKind.Null, entries[1].GetProperty("events").ValueKind);
        Assert.Equal(JsonValueKind.Object, entries[2].GetProperty("changes").ValueKind);
        Assert.Equal(JsonValueKind.Number, entries[3].GetProperty("deviceStatus").ValueKind);
    }

    [Fact]
    public void Field_names_keep_telemetry_casing_and_events_are_camel_case_with_null_label()
    {
        var entries = Batch().GetProperty("entries");
        Assert.True(entries[0].GetProperty("snapshot").TryGetProperty("OperatingPressurePSI", out _));

        var labelled = entries[3].GetProperty("events")[0];
        Assert.Equal(new[] { "parameter", "value", "state", "eventType", "domain", "severity", "label" },
            labelled.EnumerateObject().Select(p => p.Name));
        Assert.Equal("Left Overstroke Warning", labelled.GetProperty("label").GetString());
        Assert.Equal(JsonValueKind.Null, entries[4].GetProperty("events")[0].GetProperty("label").ValueKind);
        Assert.Equal("2026-10-01T10:00:03.0000000Z", entries[3].GetProperty("timestampUtc").GetString());
    }

    [Fact]
    public void Real_seed_batch_stays_inside_the_message_budget()
    {
        var seed = Seed();
        var keys = new[] { "OperatingPressurePSI", "OperatingPressureBar", "StrokeRateLeft", "StrokeRateRight",
            "StrokeTimeLeft", "StrokeTimeRight", "StrokeCounterTW1", "ProportionalOutputValue" };
        var entries = new List<BroadcastEntry> { new(T0, 0, seed, null, null) };
        for (var i = 1; i < 5; i++)
            entries.Add(new(T0.AddSeconds(i), 0, null,
                keys.ToDictionary(k => k, _ => JsonSerializer.SerializeToElement(54000 + i)), null));

        var decoded = Encoding.UTF8.GetByteCount(BroadcastEncoder.BatchJson(Session, Device.DeviceGuid, T0.AddSeconds(5), entries));
        var envelope = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(
            BroadcastEncoder.Encode(Session, Device.DeviceGuid, T0.AddSeconds(5), entries)));

        Assert.True(decoded < 4096, $"decoded batch = {decoded} bytes");
        Assert.True(envelope < 2048, $"envelope = {envelope} bytes");
    }
}
```

If `Real_seed_batch_stays_inside_the_message_budget` fails, do **not** loosen the bound: note the measured sizes from the assertion message — they become evidence for a new finding in Task 14 (`docs/findings.md`, "broadcast size with the 73-field seed") — and mark the test `[Fact(Skip = "F-size: measured N bytes, see docs/findings.md")]`.

`tests/Shape.LiveIngestion.Tests/LiveTelemetryProcessorTests.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class LiveTelemetryProcessorTests
{
    private static readonly DeviceResolver Devices = new(new DeviceConfig(
        "PUMP-00417", Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), "PREC-NEO30-MARYL-07", "gw-001", "DefaultModeModule"));

    private const string FirstEntries = """
        [{"TimestampUtc":"2026-10-01T10:00:00Z","Body":{"P":1,"OperatingAlarmActive":0},"Event":{"OperatingAlarmActive":0,"MotorRunning":1}},
         {"TimestampUtc":"2026-10-01T10:00:01Z","Changes":{"P":2}},
         {"TimestampUtc":"2026-10-01T10:00:02Z","Changes":{}},
         {"TimestampUtc":"2026-10-01T10:00:03Z","Changes":{"OperatingAlarmActive":5},"Event":{"OperatingAlarmActive":5}},
         {"TimestampUtc":"2026-10-01T10:00:04Z","Changes":{}}]
        """;

    private sealed class StaticCatalog(IReadOnlyDictionary<string, AlertDefinition> map) : IAlertDefinitionCatalog
    {
        public Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default) => Task.FromResult(map);
    }

    private sealed class BrokenCatalog : IAlertDefinitionCatalog
    {
        public Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default) =>
            throw new IOException("catalog unavailable");
    }

    private static LiveTelemetryProcessor Processor(FakeLiveSessionStore store, IAlertDefinitionCatalog? catalog = null) =>
        new(Devices, catalog ?? new StaticCatalog(TestCatalog.Load()), store, NullLogger<LiveTelemetryProcessor>.Instance);

    private static JsonElement Decoded(LiveBroadcast b) => JsonDocument.Parse(BroadcastEncoderTests.Decode(b.Envelope)).RootElement;

    [Fact]
    public async Task Valid_batch_commits_and_broadcasts_to_the_device_group()
    {
        var store = new FakeLiveSessionStore();

        var b = await Processor(store).ProcessAsync(TestEvents.Build(FirstEntries));

        Assert.NotNull(b);
        Assert.Equal("telemetry:device:9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284", b!.GroupName);
        Assert.Equal(1, store.Writes);
        Assert.Equal(5, store.Head!.TickCount);
        var entries = Decoded(b).GetProperty("entries");
        Assert.Equal(5, entries.GetArrayLength());
        Assert.Equal(2, entries[3].GetProperty("deviceStatus").GetInt32());
        Assert.Equal("Emergency Stop Active", entries[3].GetProperty("events")[0].GetProperty("label").GetString());
    }

    [Fact]
    public async Task Regular_telemetry_is_ignored_without_touching_the_store()
    {
        var store = new FakeLiveSessionStore();
        Assert.Null(await Processor(store).ProcessAsync(TestEvents.Build(FirstEntries, telemetryType: "regular")));
        Assert.Equal(0, store.Loads);
    }

    [Fact]
    public async Task Corrupt_body_is_logged_and_dropped()
    {
        var store = new FakeLiveSessionStore();
        Assert.Null(await Processor(store).ProcessAsync(TestEvents.Build("[]", rawBody: "not-base64!")));
        Assert.Equal(0, store.Loads);
    }

    [Fact]
    public async Task Unknown_gateway_is_logged_and_dropped()
    {
        var store = new FakeLiveSessionStore();
        Assert.Null(await Processor(store).ProcessAsync(TestEvents.Build(FirstEntries, gateway: "gw-999")));
        Assert.Equal(0, store.Loads);
    }

    [Fact]
    public async Task Redelivered_batch_writes_nothing_and_broadcasts_nothing()
    {
        var store = new FakeLiveSessionStore();
        var processor = Processor(store);
        await processor.ProcessAsync(TestEvents.Build(FirstEntries));

        Assert.Null(await processor.ProcessAsync(TestEvents.Build(FirstEntries)));
        Assert.Equal(1, store.Writes);
    }

    [Fact]
    public async Task Head_conflict_reloads_refolds_and_retries()
    {
        var store = new FakeLiveSessionStore { ConflictsToThrow = 2 };

        var b = await Processor(store).ProcessAsync(TestEvents.Build(FirstEntries));

        Assert.NotNull(b);
        Assert.Equal(3, store.Loads);
        Assert.Equal(1, store.Writes);
    }

    [Fact]
    public async Task Persistent_conflict_throws_after_three_retries()
    {
        var store = new FakeLiveSessionStore { ConflictsToThrow = 99 };

        await Assert.ThrowsAsync<LiveHeadConflictException>(() => Processor(store).ProcessAsync(TestEvents.Build(FirstEntries)));
        Assert.Equal(1 + LiveTelemetryProcessor.MaxRetries, store.Loads);
    }

    [Fact]
    public async Task Catalog_failure_still_folds_and_broadcasts_with_unknown_metadata()
    {
        var store = new FakeLiveSessionStore();

        var b = await Processor(store, new BrokenCatalog()).ProcessAsync(TestEvents.Build(FirstEntries));

        Assert.NotNull(b);
        Assert.All(store.Events, e => Assert.Equal("unknown", e.Event.EventType));
        Assert.Equal("unknown", Decoded(b!).GetProperty("entries")[3].GetProperty("events")[0].GetProperty("domain").GetString());
    }

    [Fact]
    public async Task Back_fill_only_batch_commits_without_broadcasting()
    {
        var store = new FakeLiveSessionStore();
        var processor = Processor(store);
        await processor.ProcessAsync(TestEvents.Build("""[{"TimestampUtc":"2026-10-01T10:00:05Z","Changes":{"P":9}}]"""));

        var b = await processor.ProcessAsync(TestEvents.Build(FirstEntries));

        Assert.Null(b);
        Assert.Equal(2, store.Writes);
        Assert.True(store.Head!.HasBaseline);
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveIngestion.Tests --filter "FullyQualifiedName~BroadcastEncoder|FullyQualifiedName~LiveTelemetryProcessor"`
Expected: FAIL — `The type or namespace name 'DeviceResolver' could not be found`.

- [ ] **Step 3: Implement**

`src/Shape.LiveIngestion/Infrastructure/UtcDateTimeOffsetConverter.cs`:
```csharp
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.LiveIngestion.Infrastructure;

/// <summary>Always writes UTC with a literal Z and 7 fractional digits, e.g. 2026-09-28T10:27:00.1230000Z.</summary>
public sealed class UtcDateTimeOffsetConverter : JsonConverter<DateTimeOffset>
{
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        DateTimeOffset.Parse(reader.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture));
}
```

`src/Shape.LiveIngestion/LivePolling/DeviceConfig.cs`:
```csharp
using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>config/device.json — the one device (spec §4.1). Stands in for the SQL device lookup.</summary>
public sealed record DeviceConfig(string SerialNumber, Guid DeviceGuid, string Name, string? GatewayId, string ModuleId)
{
    public static readonly string DefaultPath = Path.Combine(AppContext.BaseDirectory, "config", "device.json");

    public static DeviceConfig Load(string path) =>
        JsonSerializer.Deserialize<DeviceConfig>(File.ReadAllText(path), new JsonSerializerOptions(JsonSerializerDefaults.Web))
        ?? throw new InvalidDataException($"Device config '{path}' is empty.");
}
```

`src/Shape.LiveIngestion/LivePolling/DeviceResolver.cs`:
```csharp
namespace Shape.LiveIngestion.LivePolling;

/// <summary>Gateway id → device (spec §5.1 step 3). IoT Hub device ids are case-sensitive.</summary>
public sealed class DeviceResolver(DeviceConfig device)
{
    public bool TryResolve(string gatewayId, out ResolvedDevice resolved)
    {
        if (device.GatewayId is not null && string.Equals(device.GatewayId, gatewayId, StringComparison.Ordinal))
        {
            resolved = new ResolvedDevice(device.DeviceGuid, device.SerialNumber);
            return true;
        }
        resolved = default!;
        return false;
    }
}
```

`src/Shape.LiveIngestion/LivePolling/SignalRNames.cs`:
```csharp
namespace Shape.LiveIngestion.LivePolling;

public static class SignalRNames
{
    public const string Hub = "iot_telemetry";
    public const string Target = "liveTelemetryUpdate";
    public const string ConnectionSetting = "AzureSignalRConnectionString";

    public static string DeviceGroup(Guid deviceGuid) => DeviceGroup(deviceGuid.ToString());
    public static string DeviceGroup(string deviceId) => $"telemetry:device:{deviceId}";
}
```

`src/Shape.LiveIngestion/LivePolling/BroadcastEncoder.cs`:
```csharp
using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Serialization;
using Shape.LiveIngestion.Infrastructure;

namespace Shape.LiveIngestion.LivePolling;

public sealed record LiveEnvelope(
    [property: JsonPropertyName("encoding")] string Encoding,
    [property: JsonPropertyName("payload")] string Payload);

/// <summary>
/// Spec §5.1 step 8: compact camelCase JSON, nulls written (every entry key always present), field-name dictionary
/// keys untouched, gzip, base64. Event objects carry no timestampUtc — the entry's own is the event's.
/// </summary>
public static class BroadcastEncoder
{
    public const string Encoding = "gzip+base64";

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
        Converters = { new UtcDateTimeOffsetConverter() },
    };

    private sealed record WireBatch(Guid SessionId, Guid DeviceGuid, DateTimeOffset BatchSentUtc, IReadOnlyList<BroadcastEntry> Entries);

    public static string BatchJson(Guid sessionId, Guid deviceGuid, DateTimeOffset batchSentUtc, IReadOnlyList<BroadcastEntry> entries) =>
        JsonSerializer.Serialize(new WireBatch(sessionId, deviceGuid, batchSentUtc, entries), Json);

    public static LiveEnvelope Encode(Guid sessionId, Guid deviceGuid, DateTimeOffset batchSentUtc, IReadOnlyList<BroadcastEntry> entries)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(BatchJson(sessionId, deviceGuid, batchSentUtc, entries));
        using var output = new MemoryStream();
        using (var gz = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gz.Write(bytes);
        return new LiveEnvelope(Encoding, Convert.ToBase64String(output.ToArray()));
    }
}
```

`BroadcastEntry` property order (Task 7) is `TimestampUtc, DeviceStatus, Snapshot, Changes, Events`, and `LiveEvent` (Task 6) is `Parameter, Value, State, EventType, Domain, Severity, Label` — the key-order assertions above rely on both.

`src/Shape.LiveIngestion/LivePolling/LiveTelemetryProcessor.cs`:
```csharp
using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

public sealed record LiveBroadcast(string GroupName, LiveEnvelope Envelope);

/// <summary>Spec §5.1 steps 1–8. Returns null whenever nothing should be broadcast.</summary>
public sealed class LiveTelemetryProcessor(
    DeviceResolver devices, IAlertDefinitionCatalog catalog, ILiveSessionStore store, ILogger<LiveTelemetryProcessor> logger)
{
    public const int MaxRetries = 3;

    public async Task<LiveBroadcast?> ProcessAsync(string eventJson, CancellationToken ct = default)
    {
        var decoded = LiveEventGridDecoder.Decode(eventJson);
        if (decoded.Status != DecodeStatus.Ok)
        {
            logger.LogError("Dropped live telemetry event: {Status} {Reason}", decoded.Status, decoded.Reason);
            return null;
        }
        var batch = decoded.Batch!;

        if (!devices.TryResolve(batch.GatewayId, out var device))
        {
            logger.LogError("Dropped live telemetry batch from unknown gateway {GatewayId}", batch.GatewayId);
            return null;
        }

        IReadOnlyDictionary<string, AlertDefinition> definitions;
        try
        {
            definitions = await catalog.GetAsync(ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Alert catalog unavailable; events resolve as unknown");
            definitions = new Dictionary<string, AlertDefinition>();
        }

        for (var attempt = 0; ; attempt++)
        {
            var head = await store.LoadHeadAsync(batch.SessionId, ct);
            var fold = SnapshotFolder.Fold(head, batch, device, definitions, logger);
            if (!fold.HasWrites)
            {
                logger.LogInformation("Session {SessionId}: batch of {Count} entries is not newer than the head; skipped",
                    batch.SessionId, batch.Entries.Count);
                return null;
            }

            try
            {
                await store.WriteBatchAsync(batch.SessionId, fold.Head, fold.ChangePoints, fold.Events, ct);
            }
            catch (LiveHeadConflictException ex) when (attempt < MaxRetries)
            {
                logger.LogWarning(ex, "Session {SessionId}: head conflict, re-folding (retry {Retry})", batch.SessionId, attempt + 1);
                continue;
            }

            logger.LogInformation(
                "Session {SessionId}: applied {Applied}, back-filled {Backfilled}, {Rows} rows, {Events} events",
                batch.SessionId, fold.Applied, fold.Backfilled, fold.ChangePoints.Count, fold.Events.Count);
            if (fold.Broadcast.Count == 0) return null;
            return new LiveBroadcast(SignalRNames.DeviceGroup(device.DeviceGuid),
                BroadcastEncoder.Encode(batch.SessionId, device.DeviceGuid, batch.BatchSentUtc, fold.Broadcast));
        }
    }
}
```

`src/Shape.LiveIngestion/Functions/LiveTelemetryProcessorFunction.cs`:
```csharp
using Microsoft.Azure.Functions.Worker;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Functions;

public sealed class LiveTelemetryProcessorFunction(LiveTelemetryProcessor processor)
{
    [Function("LiveTelemetryProcessorEventGrid")]
    [SignalROutput(HubName = SignalRNames.Hub, ConnectionStringSetting = SignalRNames.ConnectionSetting)]
    public async Task<SignalRMessageAction?> Run([EventGridTrigger] string eventJson, CancellationToken ct)
    {
        var broadcast = await processor.ProcessAsync(eventJson, ct);
        return broadcast is null
            ? null
            : new SignalRMessageAction(SignalRNames.Target, [broadcast.Envelope]) { GroupName = broadcast.GroupName };
    }
}
```

`src/Shape.LiveIngestion/Functions/NegotiatorFunctions.cs`:
```csharp
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Functions;

/// <summary>Spec §5.3. No auth: the caller names itself with x-user-id.</summary>
public sealed class NegotiatorFunctions
{
    public const string UserHeader = "x-user-id";

    public sealed class GroupActionOutput
    {
        [SignalROutput(HubName = SignalRNames.Hub, ConnectionStringSetting = SignalRNames.ConnectionSetting)]
        public SignalRGroupAction? Action { get; set; }

        public HttpResponseData Response { get; set; } = default!;
    }

    [Function("negotiate")]
    public static async Task<HttpResponseData> Negotiate(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "negotiate")] HttpRequestData request,
        [SignalRConnectionInfoInput(HubName = SignalRNames.Hub, UserId = "{headers.x-user-id}",
            ConnectionStringSetting = SignalRNames.ConnectionSetting)] string connectionInfo)
    {
        if (UserId(request) is null) return await BadRequest(request, $"Header '{UserHeader}' is required.");
        var response = request.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/json");
        await response.WriteStringAsync(connectionInfo);
        return response;
    }

    [Function("join-device")]
    public static Task<GroupActionOutput> Join(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "join-device")] HttpRequestData request) =>
        Group(request, SignalRGroupActionType.Add);

    [Function("leave-device")]
    public static Task<GroupActionOutput> Leave(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "leave-device")] HttpRequestData request) =>
        Group(request, SignalRGroupActionType.Remove);

    private static async Task<GroupActionOutput> Group(HttpRequestData request, SignalRGroupActionType type)
    {
        var user = UserId(request);
        var deviceId = System.Web.HttpUtility.ParseQueryString(request.Url.Query)["deviceId"];
        if (user is null) return new GroupActionOutput { Response = await BadRequest(request, $"Header '{UserHeader}' is required.") };
        if (string.IsNullOrWhiteSpace(deviceId))
            return new GroupActionOutput { Response = await BadRequest(request, "Query parameter 'deviceId' is required.") };

        return new GroupActionOutput
        {
            Action = new SignalRGroupAction(type) { UserId = user, GroupName = SignalRNames.DeviceGroup(deviceId) },
            Response = request.CreateResponse(HttpStatusCode.OK),
        };
    }

    private static string? UserId(HttpRequestData request) =>
        request.Headers.TryGetValues(UserHeader, out var values) && values.FirstOrDefault() is { Length: > 0 } v ? v : null;

    private static async Task<HttpResponseData> BadRequest(HttpRequestData request, string message)
    {
        var response = request.CreateResponse(HttpStatusCode.BadRequest);
        await response.WriteStringAsync(message);
        return response;
    }
}
```

`src/Shape.LiveIngestion/Program.cs` (replace):
```csharp
using Azure.Core;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shape.LiveIngestion.LivePolling;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        var config = context.Configuration;
        services.AddSingleton(new DeviceResolver(DeviceConfig.Load(DeviceConfig.DefaultPath)));
        services.AddSingleton<IAlertDefinitionCatalog>(sp =>
            new AlertDefinitionCatalog(AlertDefinitionCatalog.DefaultPath, sp.GetRequiredService<ILogger<AlertDefinitionCatalog>>()));
        services.AddSingleton(_ =>
        {
            var options = new TableClientOptions();
            options.Retry.Mode = RetryMode.Exponential;   // 503/500 back-off (spec §9)
            options.Retry.MaxRetries = 5;
            options.Retry.Delay = TimeSpan.FromMilliseconds(200);
            return new TableServiceClient(config["LiveTableStorage"]
                ?? throw new InvalidOperationException("LiveTableStorage is not configured."), options);
        });
        services.AddSingleton<ILiveSessionStore>(sp => new TableLiveSessionStore(sp.GetRequiredService<TableServiceClient>()));
        services.AddSingleton<LiveTelemetryProcessor>();
    })
    .Build();

host.Run();
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveIngestion.Tests`
Expected: `Passed!  - Failed: 0, Passed: 89` (75 + encoder 5 + processor 9).

- [ ] **Step 5: Manual verification against the emulators**

Azurite must be running (Task 8). In a second window: `asrs-emulator start` (installed in phase 1; otherwise `dotnet tool install -g Microsoft.Azure.SignalR.Emulator`). Then:
```powershell
dotnet build src/Shape.LiveIngestion -v q
Push-Location src/Shape.LiveIngestion; Start-Process pwsh -ArgumentList '-NoProfile','-NoExit','-Command','func start'; Pop-Location
```
Wait for `Functions:` listing `LiveTelemetryProcessorEventGrid`, `join-device`, `leave-device`, `negotiate`, then:
```powershell
Invoke-RestMethod -Method Post http://localhost:7071/api/negotiate -Headers @{ 'x-user-id' = 'local' }
Invoke-WebRequest -Method Post 'http://localhost:7071/api/join-device?deviceId=9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284' -Headers @{ 'x-user-id' = 'local' } | Select-Object StatusCode
Invoke-WebRequest -Method Post 'http://localhost:7071/api/join-device' -SkipHttpErrorCheck | Select-Object StatusCode
dotnet run --project src/Shape.Gateway.Simulator -- --seed 42
```
Expected: negotiate returns `{ url: "http://localhost:8888/client/?hub=iot_telemetry", accessToken: "..." }`; join `200` (the user need not be connected — the emulator records the group membership or ignores it; if the emulator log shows an error for a group action on an unconnected user, note it for `docs/findings.md`, the smoke client in Task 14 joins after connecting); missing header `400`. In a third window start a session on the simulator:
```powershell
Invoke-RestMethod -Method Post http://localhost:5100/twins/gw-001/modules/DefaultModeModule/methods -ContentType application/json -Body '{"methodName":"FastPollingEnable","payload":{},"responseTimeoutInSeconds":30}'
```
Expected in the function window within ~6 s: `Session <guid>: applied 5, back-filled 0, 10x rows, 1 events` (the first batch takes the chunked path), then `applied 5` every ~5 s; no exceptions. Stop the simulator session with `FastPollingDisable` and close the windows.

- [ ] **Step 6: Commit**

```powershell
git add src/Shape.LiveIngestion tests/Shape.LiveIngestion.Tests
git commit -m "feat(ingestion): add live telemetry processor, gzip broadcast and negotiator functions"
```

---


### Task 10: Backend foundation — envelope, DTOs, options, device directory, SQLite session repository

**Files:**
- Create: `src/Shape.LiveBackend/{Shape.LiveBackend.csproj,Program.cs,appsettings.json}`
- Create: `src/Shape.LiveBackend/Api/{ApiResponse,ErrorCodes,Dtos}.cs`
- Create: `src/Shape.LiveBackend/Infrastructure/{RepoPaths,UtcDateTimeOffsetConverter}.cs`
- Create: `src/Shape.LiveBackend/Devices/{DeviceConfig,IDeviceDirectory}.cs`
- Create: `src/Shape.LiveBackend/Options/LivePollingOptions.cs`
- Create: `src/Shape.LiveBackend/Sessions/{LivePollingSessionRecord,ILivePollingSessionRepository,SqliteLivePollingSessionRepository,SessionSchema}.cs`
- Test: `tests/Shape.LiveBackend.Tests/{TempDb,ApiResponseTests,LivePollingOptionsTests,DeviceDirectoryTests,InfrastructureTests,SqliteLivePollingSessionRepositoryTests}.cs`

**Interfaces:**
- Consumes: `config/device.json` (Task 1).
- Produces:
  - `ApiResponse { bool Succeeded; string? Message; IReadOnlyList<string>? Errors (omitted when null); object? Data }`; `ApiResponse.Ok(object? data, string? message = null)`; `ApiResponse.Fail(string code, string message, IEnumerable<string>? details = null)`
  - `ErrorCodes.AlreadyActive/CapacityReached/NotActive/ExtensionLimitReached/InvalidParameters/DeviceNotFound/GatewayNotPaired/GatewayUnreachable` (the exact strings of Global Constraints)
  - DTOs: `StartLiveSessionRequest(int? PollIntervalSeconds, int? SessionDurationMinutes)`; `LiveSessionDto(Guid SessionId, string DeviceId, int PollIntervalSeconds, int SessionDurationMinutes, DateTimeOffset StartedUtc, DateTimeOffset ExpiresUtc, int TotalDurationMinutes, bool CanExtend)`; `LiveSessionStatusDto` (all nullable but `Active`; `LiveSessionStatusDto.Inactive`); `SeriesDto(Guid SessionId, int PollIntervalSeconds, DateTimeOffset StartedUtc, DateTimeOffset? LastTickUtc, long TickCount, IReadOnlyList<DateTimeOffset> T, IReadOnlyList<int> Status, IReadOnlyList<SeriesMetricDto> Series, IReadOnlyList<LiveEventDto> Events)`; `SeriesMetricDto(string Metric, IReadOnlyList<JsonElement?> V)`; `LiveEventDto(DateTimeOffset TimestampUtc, string Parameter, long Value, string State, string EventType, string Domain, string Severity, string? Label)`
  - `UtcDateTimeOffsetConverter` (same output as the ingestion copy); `RepoPaths.Root`, `RepoPaths.Resolve(string path)`
  - `DeviceConfig(string SerialNumber, Guid DeviceGuid, string Name, string? GatewayId, string ModuleId)` with `Load(path)`, `DefaultPath`; `IDeviceDirectory.FindBySerial(string) → DeviceConfig?`; `ConfigDeviceDirectory(DeviceConfig)`
  - `LivePollingOptions` (section `LivePolling`): `PollIntervalSeconds` (clamped 1–60), `SessionDurationMinutes` (clamped 10–30), `ExtensionMinutes`, `MaxConcurrentSessions`, `string[] DefaultSeriesMetrics`, `string? ModuleId`, `DirectMethodTimeoutSeconds`, `IoTHubUrl`, `SessionDbPath`, `TableConnectionString`; `MaxTotalDurationMinutes = 30`
  - `LivePollingSessionRecord` (record, init properties = SQLite columns of spec §7.5) with `TotalDurationMinutes`, `CanExtend`
  - `ILivePollingSessionRepository`: `EnsureSchemaAsync`, `GetAsync(Guid)`, `GetOpenForDeviceAsync(Guid deviceGuid, DateTimeOffset now)`, `CountOpenAsync(DateTimeOffset now)`, `InsertAsync(LivePollingSessionRecord)`, `UpdateExpiryAsync(Guid sessionId, DateTimeOffset expiresUtc, DateTimeOffset extendedUtc)` (also `ExtensionsUsed += 1`), `MarkStoppedAsync(Guid sessionId, DateTimeOffset stoppedUtc, string reason)` — every method takes a trailing `CancellationToken ct = default`
  - `SqliteLivePollingSessionRepository(string dbPath)`; `SessionSchemaInitializer` (hosted service calling `EnsureSchemaAsync`); `SessionSchema.Ddl`; `SessionSchema.Ts(DateTimeOffset)`, `SessionSchema.ParseTs(string)`

- [ ] **Step 1: Create the projects**

```powershell
dotnet new web -n Shape.LiveBackend -o src/Shape.LiveBackend -f net8.0
Remove-Item src/Shape.LiveBackend/Properties -Recurse
Remove-Item src/Shape.LiveBackend/appsettings.Development.json
dotnet sln add src/Shape.LiveBackend
dotnet add src/Shape.LiveBackend package Azure.Data.Tables --version 12.12.0
dotnet add src/Shape.LiveBackend package Microsoft.Data.Sqlite --version 8.0.11
dotnet new xunit -n Shape.LiveBackend.Tests -o tests/Shape.LiveBackend.Tests -f net8.0
Remove-Item tests/Shape.LiveBackend.Tests/UnitTest1.cs
dotnet sln add tests/Shape.LiveBackend.Tests
dotnet add tests/Shape.LiveBackend.Tests reference src/Shape.LiveBackend
dotnet add tests/Shape.LiveBackend.Tests package Microsoft.AspNetCore.Mvc.Testing --version 8.0.11
dotnet add tests/Shape.LiveBackend.Tests package Microsoft.Extensions.TimeProvider.Testing
```
(`Properties/launchSettings.json` is removed so `dotnet run` honours `Urls` from `appsettings.json` instead of a random port.)

Add inside `<Project>` of both `src/Shape.LiveBackend/Shape.LiveBackend.csproj` and `tests/Shape.LiveBackend.Tests/Shape.LiveBackend.Tests.csproj`:
```xml
  <ItemGroup>
    <None Include="..\..\config\*.json" Link="config\%(Filename)%(Extension)" CopyToOutputDirectory="PreserveNewest" />
  </ItemGroup>
```

`src/Shape.LiveBackend/appsettings.json`:
```json
{
  "Logging": { "LogLevel": { "Default": "Information", "Microsoft.AspNetCore": "Warning" } },
  "AllowedHosts": "*",
  "Urls": "http://localhost:5080",
  "Cors": { "Origin": "http://localhost:5173" },
  "LivePolling": {
    "PollIntervalSeconds": 1,
    "SessionDurationMinutes": 10,
    "ExtensionMinutes": 10,
    "MaxConcurrentSessions": 5,
    "DefaultSeriesMetrics": [ "OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight" ],
    "DirectMethodTimeoutSeconds": 10,
    "IoTHubUrl": "http://localhost:5100",
    "SessionDbPath": "data/live-polling.db",
    "TableConnectionString": "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;"
  }
}
```

- [ ] **Step 2: Write failing tests**

`tests/Shape.LiveBackend.Tests/TempDb.cs`:
```csharp
using Microsoft.Data.Sqlite;

namespace Shape.LiveBackend.Tests;

/// <summary>A throwaway SQLite file. Pools are cleared before deletion so Windows releases the handle.</summary>
public sealed class TempDb : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "shape-live-tests", $"{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var f in new[] { Path, Path + "-wal", Path + "-shm" })
            if (File.Exists(f)) File.Delete(f);
    }
}
```

`tests/Shape.LiveBackend.Tests/ApiResponseTests.cs`:
```csharp
using System.Text.Json;
using Shape.LiveBackend.Api;

namespace Shape.LiveBackend.Tests;

public class ApiResponseTests
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    [Fact]
    public void Success_omits_errors_and_writes_message_and_data()
    {
        var json = JsonSerializer.Serialize(ApiResponse.Ok(new { active = false }), Web);
        Assert.Equal("""{"succeeded":true,"message":null,"data":{"active":false}}""", json);
    }

    [Fact]
    public void Failure_puts_the_code_first_and_details_after()
    {
        var r = ApiResponse.Fail(ErrorCodes.InvalidParameters, "Invalid live session parameters.",
            ["pollIntervalSeconds must be between 1 and 60."]);

        Assert.False(r.Succeeded);
        Assert.Equal(new[] { "LIVE_SESSION_INVALID_PARAMETERS", "pollIntervalSeconds must be between 1 and 60." }, r.Errors);
        Assert.Null(r.Data);
    }

    [Fact]
    public void Error_codes_are_the_contract_strings()
    {
        Assert.Equal(
            new[] { "LIVE_SESSION_ALREADY_ACTIVE", "LIVE_SESSION_CAPACITY_REACHED", "LIVE_SESSION_NOT_ACTIVE",
                "LIVE_SESSION_EXTENSION_LIMIT_REACHED", "LIVE_SESSION_INVALID_PARAMETERS", "DEVICE_NOT_FOUND",
                "GATEWAY_NOT_PAIRED", "GATEWAY_UNREACHABLE" },
            new[] { ErrorCodes.AlreadyActive, ErrorCodes.CapacityReached, ErrorCodes.NotActive, ErrorCodes.ExtensionLimitReached,
                ErrorCodes.InvalidParameters, ErrorCodes.DeviceNotFound, ErrorCodes.GatewayNotPaired, ErrorCodes.GatewayUnreachable });
    }
}
```

`tests/Shape.LiveBackend.Tests/LivePollingOptionsTests.cs`:
```csharp
using Microsoft.Extensions.Configuration;
using Shape.LiveBackend.Options;

namespace Shape.LiveBackend.Tests;

public class LivePollingOptionsTests
{
    private static LivePollingOptions Bind(Dictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build().GetSection("LivePolling").Get<LivePollingOptions>()!;

    [Theory]
    [InlineData("0", "5", 1, 10)]
    [InlineData("99", "45", 60, 30)]
    [InlineData("1", "10", 1, 10)]
    [InlineData("30", "20", 30, 20)]
    public void Interval_and_duration_are_clamped(string interval, string duration, int expectedInterval, int expectedDuration)
    {
        var o = Bind(new() { ["LivePolling:PollIntervalSeconds"] = interval, ["LivePolling:SessionDurationMinutes"] = duration });
        Assert.Equal(expectedInterval, o.PollIntervalSeconds);
        Assert.Equal(expectedDuration, o.SessionDurationMinutes);
    }

    [Fact]
    public void Default_series_metrics_bind_as_an_array_without_duplicating_defaults()
    {
        var o = Bind(new() { ["LivePolling:DefaultSeriesMetrics:0"] = "A", ["LivePolling:DefaultSeriesMetrics:1"] = "B" });
        Assert.Equal(new[] { "A", "B" }, o.DefaultSeriesMetrics);
        Assert.Equal(10, o.ExtensionMinutes);
        Assert.Equal(5, o.MaxConcurrentSessions);
    }
}
```

`tests/Shape.LiveBackend.Tests/DeviceDirectoryTests.cs`:
```csharp
using Shape.LiveBackend.Devices;

namespace Shape.LiveBackend.Tests;

public class DeviceDirectoryTests
{
    [Fact]
    public void Configured_device_loads_from_config_and_is_found_case_insensitively()
    {
        var device = DeviceConfig.Load(DeviceConfig.DefaultPath);
        var directory = new ConfigDeviceDirectory(device);

        Assert.Equal(Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), directory.FindBySerial("pump-00417")!.DeviceGuid);
        Assert.Equal("gw-001", device.GatewayId);
        Assert.Equal("DefaultModeModule", device.ModuleId);
    }

    [Fact]
    public void Other_serials_are_not_found() =>
        Assert.Null(new ConfigDeviceDirectory(DeviceConfig.Load(DeviceConfig.DefaultPath)).FindBySerial("PUMP-99999"));
}
```

`tests/Shape.LiveBackend.Tests/InfrastructureTests.cs`:
```csharp
using System.Text.Json;
using Shape.LiveBackend.Infrastructure;

namespace Shape.LiveBackend.Tests;

public class InfrastructureTests
{
    [Fact]
    public void Repo_root_is_the_directory_holding_the_solution()
    {
        Assert.True(File.Exists(Path.Combine(RepoPaths.Root, "ShapeLive.sln")));
        Assert.Equal(Path.Combine(RepoPaths.Root, "data", "x.db"), RepoPaths.Resolve("data/x.db"));
        Assert.Equal(@"C:\abs\x.db", RepoPaths.Resolve(@"C:\abs\x.db"));
    }

    [Fact]
    public void Dates_are_written_in_utc_with_z()
    {
        var options = new JsonSerializerOptions { Converters = { new UtcDateTimeOffsetConverter() } };
        var json = JsonSerializer.Serialize(DateTimeOffset.Parse("2026-09-28T12:27:00.123+02:00"), options);
        Assert.Equal("\"2026-09-28T10:27:00.1230000Z\"", json);
    }
}
```

`tests/Shape.LiveBackend.Tests/SqliteLivePollingSessionRepositoryTests.cs`:
```csharp
using Microsoft.Data.Sqlite;
using Shape.LiveBackend.Sessions;

namespace Shape.LiveBackend.Tests;

public sealed class SqliteLivePollingSessionRepositoryTests : IDisposable
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    private static readonly Guid Device = Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284");
    private readonly TempDb _db = new();
    private readonly SqliteLivePollingSessionRepository _repo;

    public SqliteLivePollingSessionRepositoryTests()
    {
        _repo = new SqliteLivePollingSessionRepository(_db.Path);
        _repo.EnsureSchemaAsync().GetAwaiter().GetResult();
    }

    public void Dispose() => _db.Dispose();

    private static LivePollingSessionRecord Session(DateTimeOffset started, int minutes = 10, Guid? device = null) => new()
    {
        SessionId = Guid.NewGuid(), DeviceGuid = device ?? Device, DeviceId = "PUMP-00417", GatewayId = "gw-001",
        StartedUtc = started, StartedByUserId = "local", PollIntervalSeconds = 1, SessionDurationMinutes = minutes,
        ExpiresUtc = started.AddMinutes(minutes),
    };

    [Fact]
    public async Task Insert_then_get_round_trips_every_column()
    {
        var s = Session(T0);
        await _repo.InsertAsync(s);

        var back = await _repo.GetAsync(s.SessionId);

        Assert.Equal(s, back);
        Assert.Equal(10, back!.TotalDurationMinutes);
        Assert.True(back.CanExtend);
    }

    [Fact]
    public async Task Open_session_excludes_stopped_and_lazily_expired_rows()
    {
        var expired = Session(T0.AddMinutes(-30));
        var stopped = Session(T0.AddMinutes(-1));
        var open = Session(T0);
        foreach (var s in new[] { expired, stopped, open }) await _repo.InsertAsync(s);
        await _repo.MarkStoppedAsync(stopped.SessionId, T0, "ManualStop");

        Assert.Equal(open.SessionId, (await _repo.GetOpenForDeviceAsync(Device, T0.AddSeconds(1)))!.SessionId);
        Assert.Null(await _repo.GetOpenForDeviceAsync(Device, T0.AddMinutes(10)));    // ExpiresUtc > now is strict
    }

    [Fact]
    public async Task Count_open_is_fleet_wide()
    {
        await _repo.InsertAsync(Session(T0));
        await _repo.InsertAsync(Session(T0, device: Guid.NewGuid()));
        await _repo.InsertAsync(Session(T0.AddMinutes(-20), device: Guid.NewGuid()));

        Assert.Equal(2, await _repo.CountOpenAsync(T0.AddMinutes(1)));
    }

    [Fact]
    public async Task Update_expiry_moves_the_end_and_counts_the_extension()
    {
        var s = Session(T0);
        await _repo.InsertAsync(s);

        await _repo.UpdateExpiryAsync(s.SessionId, T0.AddMinutes(20), T0.AddMinutes(5));
        await _repo.UpdateExpiryAsync(s.SessionId, T0.AddMinutes(30), T0.AddMinutes(6));

        var back = (await _repo.GetAsync(s.SessionId))!;
        Assert.Equal(T0.AddMinutes(30), back.ExpiresUtc);
        Assert.Equal(2, back.ExtensionsUsed);
        Assert.Equal(T0.AddMinutes(6), back.LastExtendedUtc);
        Assert.Equal(30, back.TotalDurationMinutes);
        Assert.False(back.CanExtend);
    }

    [Fact]
    public async Task Mark_stopped_stamps_time_and_reason()
    {
        var s = Session(T0);
        await _repo.InsertAsync(s);

        await _repo.MarkStoppedAsync(s.SessionId, T0.AddMinutes(2), "ManualStop");

        var back = (await _repo.GetAsync(s.SessionId))!;
        Assert.Equal(T0.AddMinutes(2), back.StoppedUtc);
        Assert.Equal("ManualStop", back.StopReason);
    }

    [Fact]
    public async Task Schema_is_idempotent_and_uses_wal()
    {
        await _repo.EnsureSchemaAsync();

        await using var c = new SqliteConnection($"Data Source={_db.Path}");
        await c.OpenAsync();
        await using var cmd = c.CreateCommand();
        cmd.CommandText = "PRAGMA journal_mode;";
        Assert.Equal("wal", (string)(await cmd.ExecuteScalarAsync())!);
    }
}
```

- [ ] **Step 3: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveBackend.Tests`
Expected: FAIL — `The type or namespace name 'Api' does not exist in the namespace 'Shape.LiveBackend'`.

- [ ] **Step 4: Implement**

`src/Shape.LiveBackend/Api/ApiResponse.cs`:
```csharp
using System.Text.Json.Serialization;

namespace Shape.LiveBackend.Api;

/// <summary>The backend envelope: { succeeded, message, errors, data }; errors omitted when null, errors[0] = code.</summary>
public sealed class ApiResponse
{
    public bool Succeeded { get; init; }
    public string? Message { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<string>? Errors { get; init; }

    public object? Data { get; init; }

    public static ApiResponse Ok(object? data, string? message = null) => new() { Succeeded = true, Message = message, Data = data };

    public static ApiResponse Fail(string code, string message, IEnumerable<string>? details = null) =>
        new() { Succeeded = false, Message = message, Errors = [code, .. details ?? []] };
}
```

`src/Shape.LiveBackend/Api/ErrorCodes.cs`:
```csharp
namespace Shape.LiveBackend.Api;

public static class ErrorCodes
{
    public const string AlreadyActive = "LIVE_SESSION_ALREADY_ACTIVE";
    public const string CapacityReached = "LIVE_SESSION_CAPACITY_REACHED";
    public const string NotActive = "LIVE_SESSION_NOT_ACTIVE";
    public const string ExtensionLimitReached = "LIVE_SESSION_EXTENSION_LIMIT_REACHED";
    public const string InvalidParameters = "LIVE_SESSION_INVALID_PARAMETERS";
    public const string DeviceNotFound = "DEVICE_NOT_FOUND";
    public const string GatewayNotPaired = "GATEWAY_NOT_PAIRED";
    public const string GatewayUnreachable = "GATEWAY_UNREACHABLE";
}
```

`src/Shape.LiveBackend/Api/Dtos.cs`:
```csharp
using System.Text.Json;

namespace Shape.LiveBackend.Api;

public sealed record StartLiveSessionRequest(int? PollIntervalSeconds, int? SessionDurationMinutes);

/// <summary>Start and extend response. sessionDurationMinutes carries the current total, as in the reference example.</summary>
public sealed record LiveSessionDto(
    Guid SessionId, string DeviceId, int PollIntervalSeconds, int SessionDurationMinutes,
    DateTimeOffset StartedUtc, DateTimeOffset ExpiresUtc, int TotalDurationMinutes, bool CanExtend);

/// <summary>GET /live-session. When Active is false every other field is an explicit null (see F3).</summary>
public sealed record LiveSessionStatusDto(
    bool Active, Guid? SessionId, int? PollIntervalSeconds, DateTimeOffset? ExpiresUtc, int? SecondsRemaining,
    int? TotalDurationMinutes, bool? CanExtend, int? DeviceStatus, Dictionary<string, JsonElement>? Snapshot,
    DateTimeOffset? SnapshotTimestampUtc, long? TickCount, long? BytesTotal)
{
    public static readonly LiveSessionStatusDto Inactive = new(false, null, null, null, null, null, null, null, null, null, null, null);
}

public sealed record SeriesMetricDto(string Metric, IReadOnlyList<JsonElement?> V);

public sealed record LiveEventDto(
    DateTimeOffset TimestampUtc, string Parameter, long Value, string State, string EventType, string Domain, string Severity, string? Label);

public sealed record SeriesDto(
    Guid SessionId, int PollIntervalSeconds, DateTimeOffset StartedUtc, DateTimeOffset? LastTickUtc, long TickCount,
    IReadOnlyList<DateTimeOffset> T, IReadOnlyList<int> Status, IReadOnlyList<SeriesMetricDto> Series, IReadOnlyList<LiveEventDto> Events);
```

`src/Shape.LiveBackend/Infrastructure/UtcDateTimeOffsetConverter.cs`:
```csharp
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.LiveBackend.Infrastructure;

/// <summary>Always writes UTC with a literal Z and 7 fractional digits, e.g. 2026-09-28T10:27:00.1230000Z.</summary>
public sealed class UtcDateTimeOffsetConverter : JsonConverter<DateTimeOffset>
{
    public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
        DateTimeOffset.Parse(reader.GetString()!, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);

    public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options) =>
        writer.WriteStringValue(value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fffffff'Z'", CultureInfo.InvariantCulture));
}
```

`src/Shape.LiveBackend/Infrastructure/RepoPaths.cs`:
```csharp
namespace Shape.LiveBackend.Infrastructure;

/// <summary>Repo-relative paths (data/live-polling.db) resolve against the directory holding ShapeLive.sln.</summary>
public static class RepoPaths
{
    public static string Root { get; } = Find();

    public static string Resolve(string path) =>
        Path.IsPathRooted(path) ? path : Path.GetFullPath(Path.Combine(Root, path.Replace('/', Path.DirectorySeparatorChar)));

    private static string Find()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "ShapeLive.sln"))) return dir.FullName;
        throw new InvalidOperationException($"ShapeLive.sln not found above {AppContext.BaseDirectory}.");
    }
}
```

`src/Shape.LiveBackend/Devices/DeviceConfig.cs`:
```csharp
using System.Text.Json;

namespace Shape.LiveBackend.Devices;

public sealed record DeviceConfig(string SerialNumber, Guid DeviceGuid, string Name, string? GatewayId, string ModuleId)
{
    public static readonly string DefaultPath = Path.Combine(AppContext.BaseDirectory, "config", "device.json");

    public static DeviceConfig Load(string path) =>
        JsonSerializer.Deserialize<DeviceConfig>(File.ReadAllText(path), new JsonSerializerOptions(JsonSerializerDefaults.Web))
        ?? throw new InvalidDataException($"Device config '{path}' is empty.");
}
```

`src/Shape.LiveBackend/Devices/IDeviceDirectory.cs`:
```csharp
namespace Shape.LiveBackend.Devices;

/// <summary>Stand-in for the SQL device + gateway-pairing lookup (spec §11). Serials compare case-insensitively.</summary>
public interface IDeviceDirectory
{
    DeviceConfig? FindBySerial(string serialNumber);
}

public sealed class ConfigDeviceDirectory(DeviceConfig device) : IDeviceDirectory
{
    public DeviceConfig? FindBySerial(string serialNumber) =>
        string.Equals(serialNumber, device.SerialNumber, StringComparison.OrdinalIgnoreCase) ? device : null;
}
```

`src/Shape.LiveBackend/Options/LivePollingOptions.cs`:
```csharp
namespace Shape.LiveBackend.Options;

/// <summary>LivePolling:* (spec §7.1). Interval and duration are clamped to the module's ranges on bind.</summary>
public sealed class LivePollingOptions
{
    public const string Section = "LivePolling";
    public const int MaxTotalDurationMinutes = 30;

    private int _pollIntervalSeconds = 1;
    private int _sessionDurationMinutes = 10;

    public int PollIntervalSeconds { get => _pollIntervalSeconds; set => _pollIntervalSeconds = Math.Clamp(value, 1, 60); }
    public int SessionDurationMinutes { get => _sessionDurationMinutes; set => _sessionDurationMinutes = Math.Clamp(value, 10, 30); }
    public int ExtensionMinutes { get; set; } = 10;
    public int MaxConcurrentSessions { get; set; } = 5;
    public string[] DefaultSeriesMetrics { get; set; } = [];
    public string? ModuleId { get; set; }
    public int DirectMethodTimeoutSeconds { get; set; } = 10;
    public string IoTHubUrl { get; set; } = "http://localhost:5100";
    public string SessionDbPath { get; set; } = "data/live-polling.db";
    public string TableConnectionString { get; set; } = "";
}
```

`src/Shape.LiveBackend/Sessions/LivePollingSessionRecord.cs`:
```csharp
using Shape.LiveBackend.Options;

namespace Shape.LiveBackend.Sessions;

/// <summary>One LivePollingSession row (spec §7.5).</summary>
public sealed record LivePollingSessionRecord
{
    public Guid SessionId { get; init; }
    public Guid DeviceGuid { get; init; }
    public string DeviceId { get; init; } = "";
    public string GatewayId { get; init; } = "";
    public DateTimeOffset StartedUtc { get; init; }
    public string StartedByUserId { get; init; } = "local";
    public int PollIntervalSeconds { get; init; }
    public int SessionDurationMinutes { get; init; }
    public DateTimeOffset ExpiresUtc { get; init; }
    public int ExtensionsUsed { get; init; }
    public DateTimeOffset? LastExtendedUtc { get; init; }
    public DateTimeOffset? StoppedUtc { get; init; }
    public string? StopReason { get; init; }
    public long? TickCount { get; init; }
    public long? BytesTotal { get; init; }
    public DateTimeOffset? PurgedUtc { get; init; }

    public int TotalDurationMinutes => (int)Math.Round((ExpiresUtc - StartedUtc).TotalMinutes);
    public bool CanExtend => TotalDurationMinutes < LivePollingOptions.MaxTotalDurationMinutes;
}
```

`src/Shape.LiveBackend/Sessions/ILivePollingSessionRepository.cs`:
```csharp
namespace Shape.LiveBackend.Sessions;

/// <summary>Stand-in for the Azure SQL repository (spec §7.4). "Open" = StoppedUtc IS NULL AND ExpiresUtc > now.</summary>
public interface ILivePollingSessionRepository
{
    Task EnsureSchemaAsync(CancellationToken ct = default);
    Task<LivePollingSessionRecord?> GetAsync(Guid sessionId, CancellationToken ct = default);
    Task<LivePollingSessionRecord?> GetOpenForDeviceAsync(Guid deviceGuid, DateTimeOffset now, CancellationToken ct = default);
    Task<int> CountOpenAsync(DateTimeOffset now, CancellationToken ct = default);
    Task InsertAsync(LivePollingSessionRecord session, CancellationToken ct = default);
    Task UpdateExpiryAsync(Guid sessionId, DateTimeOffset expiresUtc, DateTimeOffset extendedUtc, CancellationToken ct = default);
    Task MarkStoppedAsync(Guid sessionId, DateTimeOffset stoppedUtc, string reason, CancellationToken ct = default);
}
```

`src/Shape.LiveBackend/Sessions/SessionSchema.cs`:
```csharp
using System.Globalization;

namespace Shape.LiveBackend.Sessions;

/// <summary>Spec §7.5. The ingestion cleanup timer carries an identical copy of the DDL.</summary>
public static class SessionSchema
{
    public const string Ddl = """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS LivePollingSession (
          SessionId TEXT PRIMARY KEY, DeviceGuid TEXT NOT NULL, DeviceId TEXT NOT NULL, GatewayId TEXT NOT NULL,
          StartedUtc TEXT NOT NULL, StartedByUserId TEXT NOT NULL, PollIntervalSeconds INTEGER NOT NULL,
          SessionDurationMinutes INTEGER NOT NULL, ExpiresUtc TEXT NOT NULL, ExtensionsUsed INTEGER NOT NULL DEFAULT 0,
          LastExtendedUtc TEXT, StoppedUtc TEXT, StopReason TEXT, TickCount INTEGER, BytesTotal INTEGER, PurgedUtc TEXT);
        CREATE INDEX IF NOT EXISTS IX_Session_Device_Expires ON LivePollingSession(DeviceGuid, ExpiresUtc);
        CREATE INDEX IF NOT EXISTS IX_Session_Unpurged ON LivePollingSession(PurgedUtc) WHERE PurgedUtc IS NULL;
        """;

    /// <summary>UTC "O" — lexical order is chronological, so SQL string comparison works.</summary>
    public static string Ts(DateTimeOffset value) => value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);

    public static DateTimeOffset ParseTs(string value) =>
        DateTimeOffset.Parse(value, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
}

/// <summary>
/// Creates the schema when the host starts. A hosted service rather than code after Build(), because
/// WebApplicationFactory stops the entry point right after Build() and that code would never run in tests.
/// </summary>
public sealed class SessionSchemaInitializer(ILivePollingSessionRepository repository) : Microsoft.Extensions.Hosting.IHostedService
{
    public Task StartAsync(CancellationToken ct) => repository.EnsureSchemaAsync(ct);
    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
```

`src/Shape.LiveBackend/Sessions/SqliteLivePollingSessionRepository.cs`:
```csharp
using Microsoft.Data.Sqlite;
using static Shape.LiveBackend.Sessions.SessionSchema;

namespace Shape.LiveBackend.Sessions;

public sealed class SqliteLivePollingSessionRepository(string dbPath) : ILivePollingSessionRepository
{
    private const string Columns =
        "SessionId, DeviceGuid, DeviceId, GatewayId, StartedUtc, StartedByUserId, PollIntervalSeconds, SessionDurationMinutes, " +
        "ExpiresUtc, ExtensionsUsed, LastExtendedUtc, StoppedUtc, StopReason, TickCount, BytesTotal, PurgedUtc";

    private readonly string _connectionString = new SqliteConnectionStringBuilder
    {
        DataSource = dbPath, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = true,
    }.ToString();

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
        await using var c = await OpenAsync(ct);
        await using var cmd = c.CreateCommand();
        cmd.CommandText = Ddl;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public Task<LivePollingSessionRecord?> GetAsync(Guid sessionId, CancellationToken ct = default) =>
        QuerySingleAsync($"SELECT {Columns} FROM LivePollingSession WHERE SessionId = $id", ct, ("$id", sessionId.ToString()));

    public Task<LivePollingSessionRecord?> GetOpenForDeviceAsync(Guid deviceGuid, DateTimeOffset now, CancellationToken ct = default) =>
        QuerySingleAsync(
            $"SELECT {Columns} FROM LivePollingSession WHERE DeviceGuid = $d AND StoppedUtc IS NULL AND ExpiresUtc > $now " +
            "ORDER BY StartedUtc DESC LIMIT 1", ct, ("$d", deviceGuid.ToString()), ("$now", Ts(now)));

    public async Task<int> CountOpenAsync(DateTimeOffset now, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await using var cmd = Command(c, "SELECT COUNT(*) FROM LivePollingSession WHERE StoppedUtc IS NULL AND ExpiresUtc > $now",
            ("$now", Ts(now)));
        return Convert.ToInt32(await cmd.ExecuteScalarAsync(ct));
    }

    public async Task InsertAsync(LivePollingSessionRecord s, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await using var cmd = Command(c,
            "INSERT INTO LivePollingSession (SessionId, DeviceGuid, DeviceId, GatewayId, StartedUtc, StartedByUserId, " +
            "PollIntervalSeconds, SessionDurationMinutes, ExpiresUtc, ExtensionsUsed) " +
            "VALUES ($id, $d, $dev, $gw, $start, $user, $interval, $duration, $exp, 0)",
            ("$id", s.SessionId.ToString()), ("$d", s.DeviceGuid.ToString()), ("$dev", s.DeviceId), ("$gw", s.GatewayId),
            ("$start", Ts(s.StartedUtc)), ("$user", s.StartedByUserId), ("$interval", s.PollIntervalSeconds),
            ("$duration", s.SessionDurationMinutes), ("$exp", Ts(s.ExpiresUtc)));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task UpdateExpiryAsync(Guid sessionId, DateTimeOffset expiresUtc, DateTimeOffset extendedUtc, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await using var cmd = Command(c,
            "UPDATE LivePollingSession SET ExpiresUtc = $exp, LastExtendedUtc = $ext, ExtensionsUsed = ExtensionsUsed + 1 WHERE SessionId = $id",
            ("$exp", Ts(expiresUtc)), ("$ext", Ts(extendedUtc)), ("$id", sessionId.ToString()));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    public async Task MarkStoppedAsync(Guid sessionId, DateTimeOffset stoppedUtc, string reason, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await using var cmd = Command(c,
            "UPDATE LivePollingSession SET StoppedUtc = $at, StopReason = $reason WHERE SessionId = $id",
            ("$at", Ts(stoppedUtc)), ("$reason", reason), ("$id", sessionId.ToString()));
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private async Task<SqliteConnection> OpenAsync(CancellationToken ct)
    {
        var c = new SqliteConnection(_connectionString);
        await c.OpenAsync(ct);
        return c;
    }

    private static SqliteCommand Command(SqliteConnection c, string sql, params (string Name, object Value)[] args)
    {
        var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in args) cmd.Parameters.AddWithValue(name, value);
        return cmd;
    }

    private async Task<LivePollingSessionRecord?> QuerySingleAsync(string sql, CancellationToken ct, params (string, object)[] args)
    {
        await using var c = await OpenAsync(ct);
        await using var cmd = Command(c, sql, args);
        await using var r = await cmd.ExecuteReaderAsync(ct);
        return await r.ReadAsync(ct) ? Read(r) : null;
    }

    private static LivePollingSessionRecord Read(SqliteDataReader r) => new()
    {
        SessionId = Guid.Parse(r.GetString(0)),
        DeviceGuid = Guid.Parse(r.GetString(1)),
        DeviceId = r.GetString(2),
        GatewayId = r.GetString(3),
        StartedUtc = ParseTs(r.GetString(4)),
        StartedByUserId = r.GetString(5),
        PollIntervalSeconds = r.GetInt32(6),
        SessionDurationMinutes = r.GetInt32(7),
        ExpiresUtc = ParseTs(r.GetString(8)),
        ExtensionsUsed = r.GetInt32(9),
        LastExtendedUtc = r.IsDBNull(10) ? null : ParseTs(r.GetString(10)),
        StoppedUtc = r.IsDBNull(11) ? null : ParseTs(r.GetString(11)),
        StopReason = r.IsDBNull(12) ? null : r.GetString(12),
        TickCount = r.IsDBNull(13) ? null : r.GetInt64(13),
        BytesTotal = r.IsDBNull(14) ? null : r.GetInt64(14),
        PurgedUtc = r.IsDBNull(15) ? null : ParseTs(r.GetString(15)),
    };
}
```

`src/Shape.LiveBackend/Program.cs` (replace; Tasks 11–12 add services):
```csharp
using Microsoft.Extensions.Options;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.Infrastructure;
using Shape.LiveBackend.Options;
using Shape.LiveBackend.Sessions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<LivePollingOptions>(builder.Configuration.GetSection(LivePollingOptions.Section));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IDeviceDirectory>(_ => new ConfigDeviceDirectory(DeviceConfig.Load(DeviceConfig.DefaultPath)));
builder.Services.AddSingleton<ILivePollingSessionRepository>(sp =>
    new SqliteLivePollingSessionRepository(RepoPaths.Resolve(sp.GetRequiredService<IOptions<LivePollingOptions>>().Value.SessionDbPath)));
builder.Services.AddHostedService<SessionSchemaInitializer>();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p
    .WithOrigins(builder.Configuration["Cors:Origin"] ?? "http://localhost:5173").AllowAnyHeader().AllowAnyMethod().AllowCredentials()));
builder.Services.AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.Converters.Add(new UtcDateTimeOffsetConverter()));

var app = builder.Build();

app.UseCors();
app.MapControllers();
app.Run();

public partial class Program;
```

- [ ] **Step 5: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveBackend.Tests`
Expected: `Passed!  - Failed: 0, Passed: 18` (envelope 3, options 5, directory 2, infrastructure 2, repository 6).

Run: `dotnet run --project src/Shape.LiveBackend` → `Now listening on: http://localhost:5080`; `data/live-polling.db` exists. Stop it (Ctrl+C).

- [ ] **Step 6: Commit**

```powershell
git add src/Shape.LiveBackend tests/Shape.LiveBackend.Tests ShapeLive.sln
git commit -m "feat(backend): add envelope, options, device directory and sqlite session repository"
```

---


### Task 11: Control endpoints — direct-method client, Table reader, start / status / extend / stop

**Files:**
- Create: `src/Shape.LiveBackend/Gateway/{IDirectMethodClient,HttpDirectMethodClient}.cs`
- Create: `src/Shape.LiveBackend/LiveData/{LiveSessionKeys,ILiveSessionReader,TableLiveSessionReader}.cs`
- Create: `src/Shape.LiveBackend/Api/ServiceResult.cs`
- Create: `src/Shape.LiveBackend/Sessions/LiveSessionService.cs`
- Create: `src/Shape.LiveBackend/Controllers/LivePollingController.cs`
- Modify: `src/Shape.LiveBackend/Program.cs`
- Test: `tests/Shape.LiveBackend.Tests/{Fakes,LiveSessionKeysTests,LiveRowReaderTests,HttpDirectMethodClientTests,LiveSessionServiceTests,LivePollingControllerTests}.cs`

**Interfaces:**
- Consumes: Task 10 (`ApiResponse`, `ErrorCodes`, DTOs, `IDeviceDirectory`, `DeviceConfig`, `LivePollingOptions`, `ILivePollingSessionRepository`, `SqliteLivePollingSessionRepository`, `LivePollingSessionRecord`, `UtcDateTimeOffsetConverter`). The Table column contract of Task 8 (`LiveRowMapper`), restated here — the backend does not reference the ingestion project.
- Produces:
  - `DirectMethodResult(int Status, JsonElement? Payload)`; `GatewayUnreachableException`; `IDirectMethodClient.InvokeAsync(string deviceId, string moduleId, string methodName, object? payload, TimeSpan timeout, CancellationToken ct = default) → Task<DirectMethodResult>`; `HttpDirectMethodClient(HttpClient http)` (`BaseAddress` = `IoTHubUrl`) with `TimeSpan Grace { init; }` (default `DefaultGrace` = 5 s: time beyond `responseTimeoutInSeconds` for the hub to return its own `504`)
  - `Shape.LiveBackend.LiveData.LiveSessionKeys` — same members as the ingestion copy (Task 5)
  - `LiveHeadView(Dictionary<string,JsonElement> Snapshot, int? DeviceStatus, DateTimeOffset? LastSnapshotTimestampUtc, long TickCount, long BytesTotal, DateTimeOffset? FirstTickUtc, DateTimeOffset? LastTickUtc)`; `ChangePoint(DateTimeOffset TimestampUtc, JsonElement Value)`
  - `ILiveSessionReader`: `ReadHeadAsync(Guid sessionId, CancellationToken ct = default) → Task<LiveHeadView?>`; `ReadParameterAsync(Guid sessionId, string parameter, CancellationToken ct = default) → Task<IReadOnlyList<ChangePoint>>` (oldest first); `ReadEventsAsync(Guid sessionId, CancellationToken ct = default) → Task<IReadOnlyList<LiveEventDto>>` (oldest first)
  - `TableLiveSessionReader(TableServiceClient service, string tableName = "livesessions")`; `LiveRowReader` (public static): `Value(TableEntity) → JsonElement`, `Head(TableEntity) → LiveHeadView`, `Event(TableEntity) → LiveEventDto`
  - `ServiceResult(int StatusCode, ApiResponse Body)` with `Ok(object? data, string? message = null)`, `Fail(int status, string code, string message, IEnumerable<string>? details = null)`
  - `LiveSessionService`: `StartAsync(string serial, StartLiveSessionRequest? request, CancellationToken ct)`, `GetStatusAsync(string serial, CancellationToken ct)`, `ExtendAsync(string serial, CancellationToken ct)`, `StopAsync(string serial, CancellationToken ct)` — all `Task<ServiceResult>`; `internal static` helpers `LookupDevice(IDeviceDirectory devices, string serial, out DeviceConfig device) → ServiceResult?` (null = found and paired) and `NotActive() → ServiceResult` are reused by Task 12; method-name constants `EnableMethod = "FastPollingEnable"`, `DisableMethod = "FastPollingDisable"`, `ExtendMethod = "Extend-FastPolling"`, `StopReasonManual = "ManualStop"`
  - `LivePollingController` at `api/v1/devices/{serialNumber}/live-session`

- [ ] **Step 1: Write failing tests**

`tests/Shape.LiveBackend.Tests/Fakes.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Options;
using Shape.LiveBackend.Sessions;

namespace Shape.LiveBackend.Tests;

public sealed record MethodCall(string DeviceId, string ModuleId, string MethodName, JsonElement Payload);

public sealed class FakeDirectMethodClient : IDirectMethodClient
{
    public List<MethodCall> Calls { get; } = [];
    public Func<MethodCall, DirectMethodResult> Respond { get; set; } = _ => throw new InvalidOperationException("no response set");

    public Task<DirectMethodResult> InvokeAsync(string deviceId, string moduleId, string methodName, object? payload, TimeSpan timeout,
        CancellationToken ct = default)
    {
        var call = new MethodCall(deviceId, moduleId, methodName, JsonSerializer.SerializeToElement(payload));
        Calls.Add(call);
        return Task.FromResult(Respond(call));
    }

    public static DirectMethodResult Result(int status, string payloadJson) => new(status, JsonDocument.Parse(payloadJson).RootElement.Clone());
}

public sealed class FakeLiveSessionReader : ILiveSessionReader
{
    public Dictionary<Guid, LiveHeadView> Heads { get; } = new();
    public Dictionary<(Guid, string), List<ChangePoint>> Points { get; } = new();
    public Dictionary<Guid, List<LiveEventDto>> Events { get; } = new();
    public Exception? Throw { get; set; }
    public List<string> ParameterReads { get; } = [];

    public Task<LiveHeadView?> ReadHeadAsync(Guid sessionId, CancellationToken ct = default) =>
        Throw is not null ? Task.FromException<LiveHeadView?>(Throw) : Task.FromResult(Heads.GetValueOrDefault(sessionId));

    public Task<IReadOnlyList<ChangePoint>> ReadParameterAsync(Guid sessionId, string parameter, CancellationToken ct = default)
    {
        lock (ParameterReads) ParameterReads.Add(parameter);
        return Task.FromResult<IReadOnlyList<ChangePoint>>(Points.GetValueOrDefault((sessionId, parameter)) ?? []);
    }

    public Task<IReadOnlyList<LiveEventDto>> ReadEventsAsync(Guid sessionId, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<LiveEventDto>>(Events.GetValueOrDefault(sessionId) ?? []);
}

/// <summary>Real SQLite repository on a temp file, fake gateway, fake Table reader, fake clock at T0.</summary>
public sealed class ServiceFixture : IDisposable
{
    public static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    public static readonly DeviceConfig Device =
        new("PUMP-00417", Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), "PREC-NEO30-MARYL-07", "gw-001", "DefaultModeModule");

    private readonly TempDb _db = new();
    public FakeTimeProvider Time { get; } = new(T0);
    public FakeDirectMethodClient Gateway { get; } = new();
    public FakeLiveSessionReader Reader { get; } = new();
    public SqliteLivePollingSessionRepository Sessions { get; }
    public LivePollingOptions Options { get; } = new()
    {
        DefaultSeriesMetrics = ["OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight"],
    };
    public LiveSessionService Service { get; }

    public ServiceFixture(DeviceConfig? device = null)
    {
        Sessions = new SqliteLivePollingSessionRepository(_db.Path);
        Sessions.EnsureSchemaAsync().GetAwaiter().GetResult();
        Service = new LiveSessionService(new ConfigDeviceDirectory(device ?? Device), Sessions, Reader, Gateway,
            Microsoft.Extensions.Options.Options.Create(Options), Time, NullLogger<LiveSessionService>.Instance);
    }

    /// <summary>Gateway answers FastPollingEnable like the module: accepted with the requested values.</summary>
    public void GatewayAccepts(Guid sessionId) => Gateway.Respond = call => call.MethodName switch
    {
        LiveSessionService.EnableMethod => FakeDirectMethodClient.Result(200,
            $$"""{"result":"accepted","durationMinutes":{{call.Payload.GetProperty("sessionDurationMinutes")}},"intervalSeconds":{{call.Payload.GetProperty("pollIntervalSeconds")}},"sessionId":"{{sessionId}}","defaultsApplied":[]}"""),
        LiveSessionService.DisableMethod => FakeDirectMethodClient.Result(200, $$"""{"result":"accepted","sessionId":"{{sessionId}}"}"""),
        _ => throw new InvalidOperationException(call.MethodName),
    };

    public async Task<Guid> StartedSessionAsync()
    {
        var id = Guid.NewGuid();
        GatewayAccepts(id);
        var r = await Service.StartAsync("PUMP-00417", null, default);
        Assert.Equal(200, r.StatusCode);
        Gateway.Calls.Clear();
        return id;
    }

    public void Dispose() => _db.Dispose();
}

public static class Json
{
    public static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web)
    {
        Converters = { new Shape.LiveBackend.Infrastructure.UtcDateTimeOffsetConverter() },
    };

    public static JsonElement Of(ServiceResult r) => JsonSerializer.SerializeToElement(r.Body, Web);
}
```

`tests/Shape.LiveBackend.Tests/LiveSessionKeysTests.cs` (the ingestion copy's tests, against the backend copy):
```csharp
using Shape.LiveBackend.LiveData;

namespace Shape.LiveBackend.Tests;

public class LiveSessionKeysTests
{
    private static readonly DateTimeOffset T = DateTimeOffset.Parse("2026-09-28T10:27:02.218Z");

    private static bool InRange(string key, (string From, string To) r) =>
        string.CompareOrdinal(key, r.From) >= 0 && string.CompareOrdinal(key, r.To) < 0;

    [Fact]
    public void Row_keys_use_O_format_in_utc()
    {
        Assert.Equal("OperatingPressurePSI|2026-09-28T10:27:02.2180000Z", LiveSessionKeys.ChangeRowKey("OperatingPressurePSI", T));
        Assert.Equal("evt|2026-09-28T10:27:02.2180000Z|OperatingAlarmActive", LiveSessionKeys.EventRowKey("OperatingAlarmActive", T));
        Assert.Equal("2026-09-28T10:27:02.2180000Z", LiveSessionKeys.Timestamp(T.ToOffset(TimeSpan.FromHours(5.5))));
    }

    [Fact]
    public void Param_range_excludes_longer_names_with_the_same_prefix()
    {
        var range = LiveSessionKeys.ParamRange("StrokeRate");
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("StrokeRate", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("StrokeRateLeft", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("StrokeRateRight", T), range));
    }

    [Fact]
    public void Event_range_excludes_parameter_rows_and_the_head_row()
    {
        var range = LiveSessionKeys.EventRange();
        Assert.True(InRange(LiveSessionKeys.EventRowKey("OilFilterAlarm", T), range));
        Assert.False(InRange(LiveSessionKeys.HeadRowKey, range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("OperatingPressurePSI", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("evtX", T), range));
    }

    [Fact]
    public void Since_lower_bounds_are_inclusive_and_chronological()
    {
        var range = LiveSessionKeys.ParamRange("A", T);
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("A", T.AddTicks(-1)), range));
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("A", T), range));
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("A", T.AddDays(1)), range));
    }

    [Fact]
    public void Two_events_on_one_tick_do_not_collide()
    {
        Assert.NotEqual(LiveSessionKeys.EventRowKey("OperatingAlarmActive", T), LiveSessionKeys.EventRowKey("MaintenanceAlarmActive", T));
    }

    [Theory]
    [InlineData("evt")]
    [InlineData("a|b")]
    [InlineData("")]
    public void Reserved_or_malformed_parameter_names_are_rejected(string name)
    {
        Assert.Throws<ArgumentException>(() => LiveSessionKeys.ChangeRowKey(name, T));
        Assert.Throws<ArgumentException>(() => LiveSessionKeys.ParamRange(name));
    }
}
```

`tests/Shape.LiveBackend.Tests/LiveRowReaderTests.cs`:
```csharp
using System.Text.Json;
using Azure.Data.Tables;
using Shape.LiveBackend.LiveData;

namespace Shape.LiveBackend.Tests;

public class LiveRowReaderTests
{
    private static readonly DateTimeOffset T = DateTimeOffset.Parse("2026-10-01T10:00:00Z");

    [Theory]
    [InlineData("b", true, "true")]
    [InlineData("i", 54372L, "54372")]
    [InlineData("d", 12.5, "12.5")]
    [InlineData("s", "OK", "\"OK\"")]
    public void Change_point_value_decodes_by_type_code(string code, object value, string json)
    {
        var e = new TableEntity("pk", "P|x") { ["T"] = code, ["V"] = value, ["TimestampUtc"] = T };
        Assert.Equal(json, LiveRowReader.Value(e).GetRawText());
    }

    [Fact]
    public void Type_n_decodes_to_json_null() =>
        Assert.Equal(JsonValueKind.Null, LiveRowReader.Value(new TableEntity("pk", "P|x") { ["T"] = "n" }).ValueKind);

    [Fact]
    public void Event_row_without_label_decodes_to_null_label()
    {
        var e = new TableEntity("pk", "evt|x|MaintenanceAlarmActive")
        {
            ["Parameter"] = "MaintenanceAlarmActive", ["Value"] = 1L, ["State"] = "triggered", ["EventType"] = "alarm",
            ["Domain"] = "maintenance", ["Severity"] = "critical", ["TimestampUtc"] = T,
        };

        Assert.Equal(new Api.LiveEventDto(T, "MaintenanceAlarmActive", 1, "triggered", "alarm", "maintenance", "critical", null),
            LiveRowReader.Event(e));
    }

    [Fact]
    public void Head_decodes_snapshot_and_counters()
    {
        var e = new TableEntity("pk", "!current")
        {
            ["Snapshot"] = """{"P":1,"OK":true}""", ["DeviceStatus"] = 1, ["TickCount"] = 5L, ["BytesTotal"] = 700L,
            ["HasBaseline"] = true, ["FirstTickUtc"] = T, ["LastTickUtc"] = T.AddSeconds(4), ["LastSnapshotTimestampUtc"] = T.AddSeconds(4),
        };

        var h = LiveRowReader.Head(e);

        Assert.Equal(1, h.Snapshot["P"].GetInt32());
        Assert.Equal(1, h.DeviceStatus);
        Assert.Equal(5, h.TickCount);
        Assert.Equal(T.AddSeconds(4), h.LastTickUtc);
    }
}
```

`tests/Shape.LiveBackend.Tests/HttpDirectMethodClientTests.cs`:
```csharp
using System.Net;
using System.Text;
using System.Text.Json;
using Shape.LiveBackend.Gateway;

namespace Shape.LiveBackend.Tests;

public class HttpDirectMethodClientTests
{
    private sealed class StubHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        public HttpRequestMessage? Last { get; private set; }
        public string? LastBody { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Last = request;
            LastBody = request.Content is null ? null : await request.Content.ReadAsStringAsync(ct);
            return await send(request, ct);
        }
    }

    private static (HttpDirectMethodClient, StubHandler) Client(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send,
        TimeSpan? grace = null)
    {
        var handler = new StubHandler(send);
        var client = new HttpDirectMethodClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost:5100") })
        {
            Grace = grace ?? HttpDirectMethodClient.DefaultGrace,
        };
        return (client, handler);
    }

    private static HttpResponseMessage Json(HttpStatusCode code, string body) =>
        new(code) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    [Fact]
    public async Task Posts_the_iot_hub_invocation_and_returns_module_status_and_payload()
    {
        var (client, handler) = Client((_, _) => Task.FromResult(Json(HttpStatusCode.OK, """{"status":409,"payload":{"result":"rejected"}}""")));

        var r = await client.InvokeAsync("gw-001", "DefaultModeModule", "FastPollingEnable", new { pollIntervalSeconds = 1 }, TimeSpan.FromSeconds(10));

        Assert.Equal(409, r.Status);
        Assert.Equal("rejected", r.Payload!.Value.GetProperty("result").GetString());
        Assert.Equal("http://localhost:5100/twins/gw-001/modules/DefaultModeModule/methods", handler.Last!.RequestUri!.ToString());
        var sent = JsonDocument.Parse(handler.LastBody!).RootElement;
        Assert.Equal("FastPollingEnable", sent.GetProperty("methodName").GetString());
        Assert.Equal(1, sent.GetProperty("payload").GetProperty("pollIntervalSeconds").GetInt32());
        Assert.Equal(10, sent.GetProperty("responseTimeoutInSeconds").GetInt32());
    }

    [Theory]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.GatewayTimeout)]
    [InlineData(HttpStatusCode.InternalServerError)]
    public async Task Hub_failures_are_gateway_unreachable(HttpStatusCode code)
    {
        var (client, _) = Client((_, _) => Task.FromResult(Json(code, """{"message":"x"}""")));
        await Assert.ThrowsAsync<GatewayUnreachableException>(() =>
            client.InvokeAsync("gw-001", "m", "FastPollingDisable", null, TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Connection_refused_is_gateway_unreachable()
    {
        var (client, _) = Client((_, _) => throw new HttpRequestException("refused"));
        await Assert.ThrowsAsync<GatewayUnreachableException>(() =>
            client.InvokeAsync("gw-001", "m", "FastPollingDisable", null, TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Hub_that_never_answers_times_out_as_gateway_unreachable()
    {
        var (client, _) = Client(async (_, ct) => { await Task.Delay(Timeout.Infinite, ct); return new HttpResponseMessage(); },
            grace: TimeSpan.Zero);
        await Assert.ThrowsAsync<GatewayUnreachableException>(() =>
            client.InvokeAsync("gw-001", "m", "FastPollingDisable", null, TimeSpan.FromMilliseconds(50)));
    }
}
```

`tests/Shape.LiveBackend.Tests/LiveSessionServiceTests.cs`:
```csharp
using System.Text.Json;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Sessions;
using static Shape.LiveBackend.Tests.ServiceFixture;

namespace Shape.LiveBackend.Tests;

public sealed class LiveSessionServiceTests : IDisposable
{
    private readonly ServiceFixture _f = new();
    public void Dispose() => _f.Dispose();

    private static string Code(ServiceResult r) => r.Body.Errors![0];

    // ---- device lookup ----

    [Fact]
    public async Task Unknown_serial_is_device_not_found()
    {
        var r = await _f.Service.StartAsync("PUMP-99999", null, default);
        Assert.Equal((404, ErrorCodes.DeviceNotFound), (r.StatusCode, Code(r)));
    }

    [Fact]
    public async Task Device_without_gateway_is_gateway_not_paired()
    {
        using var f = new ServiceFixture(Device with { GatewayId = null });
        var r = await f.Service.GetStatusAsync("PUMP-00417", default);
        Assert.Equal((404, ErrorCodes.GatewayNotPaired), (r.StatusCode, Code(r)));
    }

    // ---- start ----

    [Fact]
    public async Task Start_without_body_sends_config_values_and_records_the_session()
    {
        var id = Guid.NewGuid();
        _f.GatewayAccepts(id);
        _f.Options.PollIntervalSeconds = 2;

        var r = await _f.Service.StartAsync("pump-00417", null, default);

        Assert.Equal(200, r.StatusCode);
        var call = Assert.Single(_f.Gateway.Calls);
        Assert.Equal(("gw-001", "DefaultModeModule", "FastPollingEnable"), (call.DeviceId, call.ModuleId, call.MethodName));
        Assert.Equal(2, call.Payload.GetProperty("pollIntervalSeconds").GetInt32());
        Assert.Equal(10, call.Payload.GetProperty("sessionDurationMinutes").GetInt32());
        var dto = Assert.IsType<LiveSessionDto>(r.Body.Data);
        Assert.Equal(new LiveSessionDto(id, "PUMP-00417", 2, 10, T0, T0.AddMinutes(10), 10, true), dto);
        var row = (await _f.Sessions.GetAsync(id))!;
        Assert.Equal(("local", "gw-001", T0.AddMinutes(10)), (row.StartedByUserId, row.GatewayId, row.ExpiresUtc));
    }

    [Fact]
    public async Task Start_uses_the_values_the_gateway_accepted()
    {
        var id = Guid.NewGuid();
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(200,
            $$"""{"result":"accepted","durationMinutes":15,"intervalSeconds":5,"sessionId":"{{id}}","defaultsApplied":[]}""");

        var r = await _f.Service.StartAsync("PUMP-00417", new StartLiveSessionRequest(1, 15), default);

        var dto = Assert.IsType<LiveSessionDto>(r.Body.Data);
        Assert.Equal((5, 15, T0.AddMinutes(15)), (dto.PollIntervalSeconds, dto.TotalDurationMinutes, dto.ExpiresUtc));
    }

    [Fact]
    public async Task Start_out_of_range_lists_every_field_without_calling_the_gateway()
    {
        var r = await _f.Service.StartAsync("PUMP-00417", new StartLiveSessionRequest(0, 31), default);

        Assert.Equal(400, r.StatusCode);
        Assert.Equal(new[] { ErrorCodes.InvalidParameters, "pollIntervalSeconds must be between 1 and 60.",
            "sessionDurationMinutes must be between 10 and 30." }, r.Body.Errors);
        Assert.Empty(_f.Gateway.Calls);
    }

    [Fact]
    public async Task Start_with_an_open_session_is_already_active_and_names_the_expiry()
    {
        await _f.StartedSessionAsync();

        var r = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((409, ErrorCodes.AlreadyActive), (r.StatusCode, Code(r)));
        Assert.Contains("2026-10-01T10:10:00.0000000Z", r.Body.Message);
        Assert.Empty(_f.Gateway.Calls);
    }

    [Fact]
    public async Task Start_at_fleet_capacity_is_capacity_reached()
    {
        for (var i = 0; i < 5; i++)
            await _f.Sessions.InsertAsync(new LivePollingSessionRecord
            {
                SessionId = Guid.NewGuid(), DeviceGuid = Guid.NewGuid(), DeviceId = $"OTHER-{i}", GatewayId = "gw-x",
                StartedUtc = T0, PollIntervalSeconds = 1, SessionDurationMinutes = 10, ExpiresUtc = T0.AddMinutes(10),
            });

        var r = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((409, ErrorCodes.CapacityReached), (r.StatusCode, Code(r)));
    }

    [Theory]
    [InlineData(409, """{"result":"rejected","reason":"A fast polling session is already active."}""", 409, ErrorCodes.AlreadyActive)]
    [InlineData(400, """{"result":"rejected","reason":"Invalid request.","errors":["'pollIntervalSeconds'=0 is outside the allowed range (1-60)."]}""",
        400, ErrorCodes.InvalidParameters)]
    public async Task Start_maps_module_rejections(int moduleStatus, string payload, int http, string code)
    {
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(moduleStatus, payload);

        var r = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((http, code), (r.StatusCode, Code(r)));
        if (moduleStatus == 400) Assert.Equal("'pollIntervalSeconds'=0 is outside the allowed range (1-60).", r.Body.Errors![1]);
        Assert.Null(await _f.Sessions.GetOpenForDeviceAsync(Device.DeviceGuid, T0));
    }

    [Fact]
    public async Task Start_when_unreachable_is_503_and_records_nothing()
    {
        _f.Gateway.Respond = _ => throw new GatewayUnreachableException("timeout");

        var r = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((503, ErrorCodes.GatewayUnreachable), (r.StatusCode, Code(r)));
        Assert.Equal(0, await _f.Sessions.CountOpenAsync(T0));
    }

    // ---- status ----

    [Fact]
    public async Task Status_without_a_session_is_inactive_with_explicit_nulls()
    {
        var r = await _f.Service.GetStatusAsync("PUMP-00417", default);

        var data = Json.Of(r).GetProperty("data");
        Assert.False(data.GetProperty("active").GetBoolean());
        Assert.Equal(12, data.EnumerateObject().Count());
        Assert.All(data.EnumerateObject().Where(p => p.Name != "active"), p => Assert.Equal(JsonValueKind.Null, p.Value.ValueKind));
    }

    [Fact]
    public async Task Status_before_the_first_tick_has_null_live_fields()
    {
        var id = await _f.StartedSessionAsync();
        _f.Time.Advance(TimeSpan.FromSeconds(30.6));

        var dto = Assert.IsType<LiveSessionStatusDto>((await _f.Service.GetStatusAsync("PUMP-00417", default)).Body.Data);

        Assert.True(dto.Active);
        Assert.Equal(id, dto.SessionId);
        Assert.Equal(569, dto.SecondsRemaining);
        Assert.Null(dto.DeviceStatus);
        Assert.Null(dto.Snapshot);
        Assert.Null(dto.TickCount);
    }

    [Fact]
    public async Task Status_with_a_head_returns_snapshot_and_counters()
    {
        var id = await _f.StartedSessionAsync();
        _f.Reader.Heads[id] = new LiveHeadView(new() { ["P"] = JsonSerializer.SerializeToElement(1) }, 0, T0.AddSeconds(14), 15, 2100,
            T0.AddSeconds(1), T0.AddSeconds(14));
        _f.Time.Advance(TimeSpan.FromMinutes(10) - TimeSpan.FromMilliseconds(400));

        var dto = Assert.IsType<LiveSessionStatusDto>((await _f.Service.GetStatusAsync("PUMP-00417", default)).Body.Data);

        Assert.Equal(0, dto.SecondsRemaining);                       // floored, never negative
        Assert.Equal(0, dto.DeviceStatus);
        Assert.Equal(15, dto.TickCount);
        Assert.Equal(2100, dto.BytesTotal);
        Assert.Equal(T0.AddSeconds(14), dto.SnapshotTimestampUtc);
        Assert.Equal(1, dto.Snapshot!["P"].GetInt32());
    }

    [Fact]
    public async Task Status_table_failure_is_500_with_a_message()
    {
        await _f.StartedSessionAsync();
        _f.Reader.Throw = new Azure.RequestFailedException(503, "table down");

        var r = await _f.Service.GetStatusAsync("PUMP-00417", default);

        Assert.Equal(500, r.StatusCode);
        Assert.False(r.Body.Succeeded);
        Assert.NotNull(r.Body.Message);
    }

    // ---- extend ----

    private void GatewayExtends(int total) => _f.Gateway.Respond = call => FakeDirectMethodClient.Result(200,
        $$"""{"result":"accepted","sessionId":"{{call.Payload.GetProperty("sessionId").GetString()}}","extendedByMinutes":10,"totalDurationMinutes":{{total}}}""");

    [Fact]
    public async Task Extend_measures_the_new_expiry_from_the_original_start()
    {
        var id = await _f.StartedSessionAsync();
        _f.Time.Advance(TimeSpan.FromMinutes(5));
        GatewayExtends(20);

        var r = await _f.Service.ExtendAsync("PUMP-00417", default);

        var call = Assert.Single(_f.Gateway.Calls);
        Assert.Equal("Extend-FastPolling", call.MethodName);
        Assert.Equal(id.ToString(), call.Payload.GetProperty("sessionId").GetString());
        Assert.Equal(10, call.Payload.GetProperty("extendSessionDurationMinutes").GetInt32());
        var dto = Assert.IsType<LiveSessionDto>(r.Body.Data);
        Assert.Equal((T0.AddMinutes(20), 20, 20, true), (dto.ExpiresUtc, dto.TotalDurationMinutes, dto.SessionDurationMinutes, dto.CanExtend));
        var row = (await _f.Sessions.GetAsync(id))!;
        Assert.Equal((T0.AddMinutes(20), 1, T0.AddMinutes(5)), (row.ExpiresUtc, row.ExtensionsUsed, row.LastExtendedUtc));
    }

    [Fact]
    public async Task Extend_to_the_cap_reports_cannot_extend()
    {
        await _f.StartedSessionAsync();
        GatewayExtends(30);

        var dto = Assert.IsType<LiveSessionDto>((await _f.Service.ExtendAsync("PUMP-00417", default)).Body.Data);

        Assert.Equal((30, false), (dto.TotalDurationMinutes, dto.CanExtend));
    }

    [Fact]
    public async Task Extend_past_the_cap_is_extension_limit_reached_with_the_module_reason()
    {
        await _f.StartedSessionAsync();
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(400,
            """{"result":"rejected","reason":"Extending by 10 minutes would exceed the 30-minute total session cap."}""");

        var r = await _f.Service.ExtendAsync("PUMP-00417", default);

        Assert.Equal(400, r.StatusCode);
        Assert.Equal(new[] { ErrorCodes.ExtensionLimitReached, "Extending by 10 minutes would exceed the 30-minute total session cap." },
            r.Body.Errors);
    }

    [Fact]
    public async Task Extend_rejected_by_session_mismatch_is_not_active()
    {
        await _f.StartedSessionAsync();
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(409, """{"result":"rejected","reason":"No fast polling session is active."}""");

        var r = await _f.Service.ExtendAsync("PUMP-00417", default);

        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, Code(r)));
    }

    [Fact]
    public async Task Extend_without_a_session_is_not_active()
    {
        var r = await _f.Service.ExtendAsync("PUMP-00417", default);
        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, Code(r)));
    }

    [Fact]
    public async Task Extend_after_lazy_expiry_is_not_active_without_gateway_call()
    {
        await _f.StartedSessionAsync();
        _f.Time.Advance(TimeSpan.FromMinutes(10));            // ExpiresUtc == now: no longer open

        var r = await _f.Service.ExtendAsync("PUMP-00417", default);
        var stop = await _f.Service.StopAsync("PUMP-00417", default);

        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, Code(r)));
        Assert.Equal((409, ErrorCodes.NotActive), (stop.StatusCode, Code(stop)));
        Assert.Empty(_f.Gateway.Calls);
    }

    // ---- stop ----

    [Theory]
    [InlineData(200)]
    [InlineData(409)]
    public async Task Stop_stamps_manual_stop_whether_or_not_the_gateway_was_still_running(int moduleStatus)
    {
        var id = await _f.StartedSessionAsync();
        _f.Time.Advance(TimeSpan.FromMinutes(2));
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(moduleStatus, """{"result":"x"}""");

        var r = await _f.Service.StopAsync("PUMP-00417", default);

        Assert.Equal(200, r.StatusCode);
        Assert.Equal("Live session stopped.", r.Body.Message);
        Assert.Null(r.Body.Data);
        Assert.Equal("FastPollingDisable", Assert.Single(_f.Gateway.Calls).MethodName);
        var row = (await _f.Sessions.GetAsync(id))!;
        Assert.Equal((T0.AddMinutes(2), "ManualStop"), (row.StoppedUtc, row.StopReason));
    }

    [Fact]
    public async Task Stop_when_unreachable_leaves_session_open_and_blocks_start()
    {
        var id = await _f.StartedSessionAsync();
        _f.Gateway.Respond = _ => throw new GatewayUnreachableException("timeout");

        var stop = await _f.Service.StopAsync("PUMP-00417", default);
        var start = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((503, ErrorCodes.GatewayUnreachable), (stop.StatusCode, Code(stop)));
        Assert.Null((await _f.Sessions.GetAsync(id))!.StoppedUtc);
        Assert.Equal((409, ErrorCodes.AlreadyActive), (start.StatusCode, Code(start)));

        _f.Time.Advance(TimeSpan.FromMinutes(10));
        _f.GatewayAccepts(Guid.NewGuid());
        Assert.Equal(200, (await _f.Service.StartAsync("PUMP-00417", null, default)).StatusCode);   // free again after expiry
    }

    [Fact]
    public async Task Stop_without_a_session_is_not_active()
    {
        var r = await _f.Service.StopAsync("PUMP-00417", default);
        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, Code(r)));
    }
}
```

`tests/Shape.LiveBackend.Tests/LivePollingControllerTests.cs`:
```csharp
using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;

namespace Shape.LiveBackend.Tests;

/// <summary>The HTTP shape: routes, envelope, camelCase, Z dates, empty-body handling, model-state errors.</summary>
public sealed class LivePollingControllerTests : IDisposable
{
    private const string Base = "/api/v1/devices/PUMP-00417/live-session";
    private readonly TempDb _db = new();
    private readonly FakeDirectMethodClient _gateway = new();
    private readonly WebApplicationFactory<Program> _factory;

    public LivePollingControllerTests()
    {
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("LivePolling:SessionDbPath", _db.Path);
            b.ConfigureServices(s =>
            {
                s.RemoveAll<IDirectMethodClient>();
                s.AddSingleton<IDirectMethodClient>(_gateway);
                s.RemoveAll<ILiveSessionReader>();
                s.AddSingleton<ILiveSessionReader>(new FakeLiveSessionReader());
            });
        });
    }

    public void Dispose()
    {
        _factory.Dispose();
        _db.Dispose();
    }

    private static async Task<JsonElement> Body(HttpResponseMessage r) => JsonDocument.Parse(await r.Content.ReadAsStringAsync()).RootElement;

    private void GatewayAccepts(Guid id) => _gateway.Respond = call => call.MethodName == "FastPollingEnable"
        ? FakeDirectMethodClient.Result(200, $$"""{"result":"accepted","durationMinutes":10,"intervalSeconds":1,"sessionId":"{{id}}","defaultsApplied":[]}""")
        : FakeDirectMethodClient.Result(200, $$"""{"result":"accepted","sessionId":"{{id}}"}""");

    [Fact]
    public async Task Post_without_a_body_starts_and_returns_the_camel_case_envelope()
    {
        var id = Guid.NewGuid();
        GatewayAccepts(id);

        var r = await _factory.CreateClient().PostAsync(Base, null);

        Assert.Equal(HttpStatusCode.OK, r.StatusCode);
        var body = await Body(r);
        Assert.True(body.GetProperty("succeeded").GetBoolean());
        Assert.False(body.TryGetProperty("errors", out _));
        var data = body.GetProperty("data");
        Assert.Equal(id.ToString(), data.GetProperty("sessionId").GetString());
        Assert.Equal("PUMP-00417", data.GetProperty("deviceId").GetString());
        Assert.EndsWith("Z", data.GetProperty("expiresUtc").GetString());
        Assert.True(data.GetProperty("canExtend").GetBoolean());
    }

    [Fact]
    public async Task Post_with_an_empty_json_object_uses_defaults()
    {
        GatewayAccepts(Guid.NewGuid());
        var r = await _factory.CreateClient().PostAsync(Base, new StringContent("{}", Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.OK, r.StatusCode);
    }

    [Fact]
    public async Task Post_with_a_non_integer_field_is_invalid_parameters_in_the_envelope()
    {
        var r = await _factory.CreateClient().PostAsync(Base,
            new StringContent("""{"pollIntervalSeconds":"fast"}""", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, r.StatusCode);
        var body = await Body(r);
        Assert.False(body.GetProperty("succeeded").GetBoolean());
        Assert.Equal("LIVE_SESSION_INVALID_PARAMETERS", body.GetProperty("errors")[0].GetString());
        Assert.True(body.GetProperty("errors").GetArrayLength() >= 2);
    }

    [Fact]
    public async Task Unknown_device_is_404_device_not_found()
    {
        var r = await _factory.CreateClient().GetAsync("/api/v1/devices/NOPE/live-session");

        Assert.Equal(HttpStatusCode.NotFound, r.StatusCode);
        Assert.Equal("DEVICE_NOT_FOUND", (await Body(r)).GetProperty("errors")[0].GetString());
    }

    [Fact]
    public async Task Get_when_idle_is_200_with_active_false()
    {
        var body = await Body(await _factory.CreateClient().GetAsync(Base));
        var data = body.GetProperty("data");
        Assert.False(data.GetProperty("active").GetBoolean());
        Assert.Equal(JsonValueKind.Null, data.GetProperty("sessionId").ValueKind);
    }

    [Fact]
    public async Task Delete_after_start_returns_the_stopped_message_and_null_data()
    {
        GatewayAccepts(Guid.NewGuid());
        var client = _factory.CreateClient();
        await client.PostAsync(Base, null);

        var r = await client.DeleteAsync(Base);

        var body = await Body(r);
        Assert.Equal(HttpStatusCode.OK, r.StatusCode);
        Assert.Equal("Live session stopped.", body.GetProperty("message").GetString());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("data").ValueKind);
    }

    [Fact]
    public async Task Extend_without_a_session_is_409_not_active()
    {
        var r = await _factory.CreateClient().PostAsync(Base + "/extend", null);
        Assert.Equal(HttpStatusCode.Conflict, r.StatusCode);
        Assert.Equal("LIVE_SESSION_NOT_ACTIVE", (await Body(r)).GetProperty("errors")[0].GetString());
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveBackend.Tests`
Expected: FAIL — `The type or namespace name 'Gateway' does not exist in the namespace 'Shape.LiveBackend'`.

- [ ] **Step 3: Implement**

`src/Shape.LiveBackend/Gateway/IDirectMethodClient.cs`:
```csharp
using System.Text.Json;

namespace Shape.LiveBackend.Gateway;

/// <summary>Status is the module's own status (200/400/409/501), not the hub's HTTP status.</summary>
public sealed record DirectMethodResult(int Status, JsonElement? Payload);

/// <summary>The hub could not reach the module: timeout, 404, 504, any other hub failure, or connection refused.</summary>
public sealed class GatewayUnreachableException(string message, Exception? inner = null) : Exception(message, inner);

public interface IDirectMethodClient
{
    Task<DirectMethodResult> InvokeAsync(string deviceId, string moduleId, string methodName, object? payload, TimeSpan timeout,
        CancellationToken ct = default);
}
```



`src/Shape.LiveBackend/Gateway/HttpDirectMethodClient.cs`:
```csharp
using System.Net.Http.Json;
using System.Text.Json;

namespace Shape.LiveBackend.Gateway;

/// <summary>IoT Hub "invoke module method" REST call, against the simulator's facade (spec §4.2).</summary>
public sealed class HttpDirectMethodClient(HttpClient http) : IDirectMethodClient
{
    public static readonly TimeSpan DefaultGrace = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    /// <summary>Time beyond responseTimeoutInSeconds that the hub gets to return its own 504.</summary>
    public TimeSpan Grace { get; init; } = DefaultGrace;

    public async Task<DirectMethodResult> InvokeAsync(string deviceId, string moduleId, string methodName, object? payload, TimeSpan timeout,
        CancellationToken ct = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout + Grace);
        var body = new { methodName, payload, responseTimeoutInSeconds = Math.Max(1, (int)Math.Ceiling(timeout.TotalSeconds)) };
        try
        {
            using var response = await http.PostAsJsonAsync(
                $"twins/{Uri.EscapeDataString(deviceId)}/modules/{Uri.EscapeDataString(moduleId)}/methods", body, Web, cts.Token);
            if (!response.IsSuccessStatusCode)
                throw new GatewayUnreachableException($"IoT Hub returned {(int)response.StatusCode} for {methodName}.");
            var result = await response.Content.ReadFromJsonAsync<JsonElement>(Web, cts.Token);
            JsonElement? resultPayload = result.TryGetProperty("payload", out var p) ? p.Clone() : null;
            return new DirectMethodResult(result.GetProperty("status").GetInt32(), resultPayload);
        }
        catch (OperationCanceledException ex) when (!ct.IsCancellationRequested)
        {
            throw new GatewayUnreachableException($"{methodName} timed out after {timeout.TotalSeconds:0.#} s.", ex);
        }
        catch (HttpRequestException ex)
        {
            throw new GatewayUnreachableException($"IoT Hub could not be reached for {methodName}: {ex.Message}", ex);
        }
    }
}
```

`src/Shape.LiveBackend/LiveData/LiveSessionKeys.cs` (**byte-identical to the ingestion copy of Task 5 except the namespace** — keep it that way; the seam tests in Task 12 check the two agree):
```csharp
using System.Globalization;

namespace Shape.LiveBackend.LiveData;

/// <summary>
/// Row key scheme for one live session partition (09-20 §5.2, 09-30 §6). Duplicated verbatim in the backend; both
/// copies carry the same tests. Timestamps are UTC "O" (7 fractional digits), so lexical order is chronological.
/// </summary>
public static class LiveSessionKeys
{
    public const string HeadRowKey = "!current";
    public const string EventPrefix = "evt";

    public static string Timestamp(DateTimeOffset ts) => ts.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);

    public static string ChangeRowKey(string parameter, DateTimeOffset ts)
    {
        ValidateParameter(parameter);
        return $"{parameter}|{Timestamp(ts)}";
    }

    /// <summary>[From, To) covering every change point of one parameter; '}' is '|' + 1.</summary>
    public static (string From, string To) ParamRange(string parameter, DateTimeOffset? since = null)
    {
        ValidateParameter(parameter);
        return ($"{parameter}|{(since is { } s ? Timestamp(s) : "")}", $"{parameter}}}");
    }

    public static string EventRowKey(string parameter, DateTimeOffset ts)
    {
        ValidateParameter(parameter);
        return $"{EventPrefix}|{Timestamp(ts)}|{parameter}";
    }

    public static (string From, string To) EventRange(DateTimeOffset? since = null) =>
        ($"{EventPrefix}|{(since is { } s ? Timestamp(s) : "")}", $"{EventPrefix}}}");

    public static void ValidateParameter(string parameter)
    {
        if (string.IsNullOrEmpty(parameter) || parameter.Contains('|') || parameter == EventPrefix)
            throw new ArgumentException($"'{parameter}' cannot be used as a live session parameter name.", nameof(parameter));
    }
}
```

`src/Shape.LiveBackend/LiveData/ILiveSessionReader.cs`:
```csharp
using System.Text.Json;
using Shape.LiveBackend.Api;

namespace Shape.LiveBackend.LiveData;

public sealed record LiveHeadView(
    Dictionary<string, JsonElement> Snapshot, int? DeviceStatus, DateTimeOffset? LastSnapshotTimestampUtc,
    long TickCount, long BytesTotal, DateTimeOffset? FirstTickUtc, DateTimeOffset? LastTickUtc);

public sealed record ChangePoint(DateTimeOffset TimestampUtc, JsonElement Value);

/// <summary>Read side of the live session partition (spec §7.4). Every list is oldest first.</summary>
public interface ILiveSessionReader
{
    Task<LiveHeadView?> ReadHeadAsync(Guid sessionId, CancellationToken ct = default);
    Task<IReadOnlyList<ChangePoint>> ReadParameterAsync(Guid sessionId, string parameter, CancellationToken ct = default);
    Task<IReadOnlyList<LiveEventDto>> ReadEventsAsync(Guid sessionId, CancellationToken ct = default);
}
```

`src/Shape.LiveBackend/LiveData/TableLiveSessionReader.cs`:
```csharp
using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Shape.LiveBackend.Api;

namespace Shape.LiveBackend.LiveData;

/// <summary>
/// Decodes the columns the ingestion writer produces (spec §5.6): change point V + T (b/i/d/s, n = null),
/// head row with Snapshot JSON, event rows with Label absent when null.
/// </summary>
public static class LiveRowReader
{
    private static readonly JsonElement Null = JsonDocument.Parse("null").RootElement.Clone();

    public static JsonElement Value(TableEntity e) => e.GetString("T") switch
    {
        "b" => JsonSerializer.SerializeToElement(e.GetBoolean("V")),
        "i" => JsonSerializer.SerializeToElement(e.GetInt64("V")),
        "d" => JsonSerializer.SerializeToElement(e.GetDouble("V")),
        "s" => JsonSerializer.SerializeToElement(e.GetString("V")),
        _ => Null,
    };

    public static LiveHeadView Head(TableEntity e) => new(
        JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(e.GetString("Snapshot") ?? "{}")!,
        e.GetInt32("DeviceStatus"),
        e.GetDateTimeOffset("LastSnapshotTimestampUtc"),
        e.GetInt64("TickCount") ?? 0,
        e.GetInt64("BytesTotal") ?? 0,
        e.GetDateTimeOffset("FirstTickUtc"),
        e.GetDateTimeOffset("LastTickUtc"));

    public static LiveEventDto Event(TableEntity e) => new(
        e.GetDateTimeOffset("TimestampUtc")!.Value,
        e.GetString("Parameter")!,
        e.GetInt64("Value") ?? 0,
        e.GetString("State")!,
        e.GetString("EventType")!,
        e.GetString("Domain")!,
        e.GetString("Severity")!,
        e.GetString("Label"));
}

public sealed class TableLiveSessionReader(TableServiceClient service, string tableName = TableLiveSessionReader.DefaultTable)
    : ILiveSessionReader
{
    public const string DefaultTable = "livesessions";
    private readonly TableClient _table = service.GetTableClient(tableName);

    public async Task<LiveHeadView?> ReadHeadAsync(Guid sessionId, CancellationToken ct = default)
    {
        try
        {
            var r = await _table.GetEntityIfExistsAsync<TableEntity>(sessionId.ToString(), LiveSessionKeys.HeadRowKey, cancellationToken: ct);
            return r.HasValue ? LiveRowReader.Head(r.Value!) : null;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;   // the table does not exist until the first batch is written
        }
    }

    public async Task<IReadOnlyList<ChangePoint>> ReadParameterAsync(Guid sessionId, string parameter, CancellationToken ct = default)
    {
        var (from, to) = LiveSessionKeys.ParamRange(parameter);
        return (await QueryAsync(sessionId, from, to, ct))
            .Select(e => new ChangePoint(e.GetDateTimeOffset("TimestampUtc")!.Value, LiveRowReader.Value(e)))
            .ToList();
    }

    public async Task<IReadOnlyList<LiveEventDto>> ReadEventsAsync(Guid sessionId, CancellationToken ct = default)
    {
        var (from, to) = LiveSessionKeys.EventRange();
        return (await QueryAsync(sessionId, from, to, ct)).Select(LiveRowReader.Event).ToList();
    }

    /// <summary>One partition, one RowKey range. Table Storage returns rows in RowKey order, i.e. chronologically.</summary>
    private async Task<List<TableEntity>> QueryAsync(Guid sessionId, string from, string to, CancellationToken ct)
    {
        var rows = new List<TableEntity>();
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {sessionId.ToString()} and RowKey ge {from} and RowKey lt {to}");
        try
        {
            await foreach (var e in _table.QueryAsync<TableEntity>(filter, cancellationToken: ct))
                rows.Add(e);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
        }
        return rows;
    }
}
```

`src/Shape.LiveBackend/Api/ServiceResult.cs`:
```csharp
namespace Shape.LiveBackend.Api;

/// <summary>HTTP status plus envelope; the controller only translates this to an ObjectResult.</summary>
public sealed record ServiceResult(int StatusCode, ApiResponse Body)
{
    public static ServiceResult Ok(object? data, string? message = null) => new(200, ApiResponse.Ok(data, message));

    public static ServiceResult Fail(int status, string code, string message, IEnumerable<string>? details = null) =>
        new(status, ApiResponse.Fail(code, message, details));
}
```

`src/Shape.LiveBackend/Sessions/LiveSessionService.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Options;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Options;

namespace Shape.LiveBackend.Sessions;

/// <summary>
/// Spec §7.2. Every "now" comes from the injected TimeProvider. An open session is
/// StoppedUtc IS NULL AND ExpiresUtc > now — expiry is lazy, nothing marks a row expired.
/// </summary>
public sealed class LiveSessionService
{
    public const string EnableMethod = "FastPollingEnable";
    public const string DisableMethod = "FastPollingDisable";
    public const string ExtendMethod = "Extend-FastPolling";
    public const string StopReasonManual = "ManualStop";
    public const string LocalUser = "local";

    private readonly IDeviceDirectory _devices;
    private readonly ILivePollingSessionRepository _sessions;
    private readonly ILiveSessionReader _live;
    private readonly IDirectMethodClient _gateway;
    private readonly LivePollingOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<LiveSessionService> _logger;

    public LiveSessionService(IDeviceDirectory devices, ILivePollingSessionRepository sessions, ILiveSessionReader live,
        IDirectMethodClient gateway, IOptions<LivePollingOptions> options, TimeProvider time, ILogger<LiveSessionService> logger)
    {
        _devices = devices;
        _sessions = sessions;
        _live = live;
        _gateway = gateway;
        _options = options.Value;
        _time = time;
        _logger = logger;
    }

    public async Task<ServiceResult> StartAsync(string serial, StartLiveSessionRequest? request, CancellationToken ct)
    {
        if (LookupDevice(_devices, serial, out var device) is { } notFound) return notFound;

        var errors = new List<string>();
        if (request?.PollIntervalSeconds is { } i && (i < 1 || i > 60)) errors.Add("pollIntervalSeconds must be between 1 and 60.");
        if (request?.SessionDurationMinutes is { } d && (d < 10 || d > 30)) errors.Add("sessionDurationMinutes must be between 10 and 30.");
        if (errors.Count > 0) return ServiceResult.Fail(400, ErrorCodes.InvalidParameters, "Invalid live session parameters.", errors);

        var now = _time.GetUtcNow();
        if (await _sessions.GetOpenForDeviceAsync(device.DeviceGuid, now, ct) is { } open)
            return ServiceResult.Fail(409, ErrorCodes.AlreadyActive,
                $"A live session is already active on this device until {SessionSchema.Ts(open.ExpiresUtc)}.");
        if (await _sessions.CountOpenAsync(now, ct) >= _options.MaxConcurrentSessions)
            return ServiceResult.Fail(409, ErrorCodes.CapacityReached, "The maximum number of concurrent live sessions has been reached.");

        var payload = new
        {
            sessionDurationMinutes = request?.SessionDurationMinutes ?? _options.SessionDurationMinutes,
            pollIntervalSeconds = request?.PollIntervalSeconds ?? _options.PollIntervalSeconds,
        };
        DirectMethodResult result;
        try
        {
            result = await InvokeAsync(device, EnableMethod, payload, ct);
        }
        catch (GatewayUnreachableException ex)
        {
            return Unreachable(ex);
        }

        switch (result.Status)
        {
            case 200:
                var p = result.Payload!.Value;
                var duration = p.GetProperty("durationMinutes").GetInt32();
                var session = new LivePollingSessionRecord
                {
                    SessionId = p.GetProperty("sessionId").GetGuid(),
                    DeviceGuid = device.DeviceGuid,
                    DeviceId = device.SerialNumber,
                    GatewayId = device.GatewayId!,
                    StartedUtc = now,
                    StartedByUserId = LocalUser,
                    PollIntervalSeconds = p.GetProperty("intervalSeconds").GetInt32(),
                    SessionDurationMinutes = duration,
                    ExpiresUtc = now.AddMinutes(duration),
                };
                await _sessions.InsertAsync(session, ct);
                _logger.LogInformation("Live session {SessionId} started on {Serial} until {Expires}", session.SessionId, serial, session.ExpiresUtc);
                return ServiceResult.Ok(ToDto(session));
            case 409:
                return ServiceResult.Fail(409, ErrorCodes.AlreadyActive, "The gateway reports a live session is already active.");
            case 400:
                return ServiceResult.Fail(400, ErrorCodes.InvalidParameters, "The gateway rejected the live session parameters.",
                    ModuleErrors(result.Payload));
            default:
                return Unreachable(new GatewayUnreachableException($"{EnableMethod} returned module status {result.Status}."));
        }
    }

    public async Task<ServiceResult> GetStatusAsync(string serial, CancellationToken ct)
    {
        if (LookupDevice(_devices, serial, out var device) is { } notFound) return notFound;
        var now = _time.GetUtcNow();
        var open = await _sessions.GetOpenForDeviceAsync(device.DeviceGuid, now, ct);
        if (open is null) return ServiceResult.Ok(LiveSessionStatusDto.Inactive);

        LiveHeadView? head;
        try
        {
            head = await _live.ReadHeadAsync(open.SessionId, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Reading the live head of session {SessionId} failed", open.SessionId);
            return new ServiceResult(500, new ApiResponse { Succeeded = false, Message = "Live session data could not be read." });
        }

        var ticked = head is { TickCount: > 0 };
        return ServiceResult.Ok(new LiveSessionStatusDto(
            true, open.SessionId, open.PollIntervalSeconds, open.ExpiresUtc,
            Math.Max(0, (int)Math.Floor((open.ExpiresUtc - now).TotalSeconds)),
            open.TotalDurationMinutes, open.CanExtend,
            ticked ? head!.DeviceStatus : null,
            ticked ? head!.Snapshot : null,
            ticked ? head!.LastSnapshotTimestampUtc : null,
            ticked ? head!.TickCount : null,
            ticked ? head!.BytesTotal : null));
    }

    public async Task<ServiceResult> ExtendAsync(string serial, CancellationToken ct)
    {
        if (LookupDevice(_devices, serial, out var device) is { } notFound) return notFound;
        var now = _time.GetUtcNow();
        var open = await _sessions.GetOpenForDeviceAsync(device.DeviceGuid, now, ct);
        if (open is null) return NotActive();

        DirectMethodResult result;
        try
        {
            result = await InvokeAsync(device, ExtendMethod,
                new { sessionId = open.SessionId, extendSessionDurationMinutes = _options.ExtensionMinutes }, ct);
        }
        catch (GatewayUnreachableException ex)
        {
            return Unreachable(ex);
        }

        switch (result.Status)
        {
            case 200:
                var total = result.Payload!.Value.GetProperty("totalDurationMinutes").GetInt32();
                var extended = open with { ExpiresUtc = open.StartedUtc.AddMinutes(total), ExtensionsUsed = open.ExtensionsUsed + 1, LastExtendedUtc = now };
                await _sessions.UpdateExpiryAsync(open.SessionId, extended.ExpiresUtc, now, ct);
                return ServiceResult.Ok(ToDto(extended));
            case 400 when HasFieldErrors(result.Payload):
                return ServiceResult.Fail(400, ErrorCodes.InvalidParameters, "The gateway rejected the extension request.",
                    ModuleErrors(result.Payload));
            case 400:
                return ServiceResult.Fail(400, ErrorCodes.ExtensionLimitReached,
                    $"The session cannot be extended beyond {LivePollingOptions.MaxTotalDurationMinutes} minutes.", ModuleErrors(result.Payload));
            case 409:
                return ServiceResult.Fail(409, ErrorCodes.NotActive, "The gateway's live session no longer matches this session.");
            default:
                return Unreachable(new GatewayUnreachableException($"{ExtendMethod} returned module status {result.Status}."));
        }
    }

    public async Task<ServiceResult> StopAsync(string serial, CancellationToken ct)
    {
        if (LookupDevice(_devices, serial, out var device) is { } notFound) return notFound;
        var now = _time.GetUtcNow();
        var open = await _sessions.GetOpenForDeviceAsync(device.DeviceGuid, now, ct);
        if (open is null) return NotActive();

        DirectMethodResult result;
        try
        {
            result = await InvokeAsync(device, DisableMethod, null, ct);
        }
        catch (GatewayUnreachableException ex)
        {
            return Unreachable(ex);
        }
        if (result.Status is not (200 or 409))
            return Unreachable(new GatewayUnreachableException($"{DisableMethod} returned module status {result.Status}."));

        await _sessions.MarkStoppedAsync(open.SessionId, now, StopReasonManual, ct);
        _logger.LogInformation("Live session {SessionId} stopped (module status {Status})", open.SessionId, result.Status);
        return ServiceResult.Ok(null, "Live session stopped.");
    }

    internal static ServiceResult? LookupDevice(IDeviceDirectory devices, string serial, out DeviceConfig device)
    {
        device = devices.FindBySerial(serial)!;
        if (device is null) return ServiceResult.Fail(404, ErrorCodes.DeviceNotFound, $"Device '{serial}' was not found.");
        if (string.IsNullOrWhiteSpace(device.GatewayId))
            return ServiceResult.Fail(404, ErrorCodes.GatewayNotPaired, $"Device '{serial}' has no active gateway pairing.");
        return null;
    }

    internal static ServiceResult NotActive() =>
        ServiceResult.Fail(409, ErrorCodes.NotActive, "No live session is active on this device.");

    private Task<DirectMethodResult> InvokeAsync(DeviceConfig device, string method, object? payload, CancellationToken ct) =>
        _gateway.InvokeAsync(device.GatewayId!, _options.ModuleId ?? device.ModuleId, method, payload,
            TimeSpan.FromSeconds(_options.DirectMethodTimeoutSeconds), ct);

    private ServiceResult Unreachable(GatewayUnreachableException ex)
    {
        _logger.LogWarning(ex, "Gateway unreachable");
        return ServiceResult.Fail(503, ErrorCodes.GatewayUnreachable, "The gateway did not respond.");
    }

    private static LiveSessionDto ToDto(LivePollingSessionRecord s) => new(
        s.SessionId, s.DeviceId, s.PollIntervalSeconds, s.TotalDurationMinutes, s.StartedUtc, s.ExpiresUtc, s.TotalDurationMinutes, s.CanExtend);

    private static bool HasFieldErrors(JsonElement? payload) =>
        payload is { ValueKind: JsonValueKind.Object } p && p.TryGetProperty("errors", out var e) && e.ValueKind == JsonValueKind.Array;

    private static List<string> ModuleErrors(JsonElement? payload)
    {
        if (payload is not { ValueKind: JsonValueKind.Object } p) return [];
        if (HasFieldErrors(p)) return p.GetProperty("errors").EnumerateArray().Select(e => e.GetString() ?? e.GetRawText()).ToList();
        return p.TryGetProperty("reason", out var reason) && reason.ValueKind == JsonValueKind.String ? [reason.GetString()!] : [];
    }
}
```

`src/Shape.LiveBackend/Controllers/LivePollingController.cs`:
```csharp
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Sessions;

namespace Shape.LiveBackend.Controllers;

[ApiController]
[Route("api/v1/devices/{serialNumber}/live-session")]
public sealed class LivePollingController(LiveSessionService service) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> Start(string serialNumber,
        [FromBody(EmptyBodyBehavior = EmptyBodyBehavior.Allow)] StartLiveSessionRequest? request, CancellationToken ct) =>
        From(await service.StartAsync(serialNumber, request, ct));

    [HttpGet]
    public async Task<IActionResult> Status(string serialNumber, CancellationToken ct) =>
        From(await service.GetStatusAsync(serialNumber, ct));

    [HttpPost("extend")]
    public async Task<IActionResult> Extend(string serialNumber, CancellationToken ct) =>
        From(await service.ExtendAsync(serialNumber, ct));

    [HttpDelete]
    public async Task<IActionResult> Stop(string serialNumber, CancellationToken ct) =>
        From(await service.StopAsync(serialNumber, ct));

    private static ObjectResult From(ServiceResult r) => new(r.Body) { StatusCode = r.StatusCode };
}
```

`src/Shape.LiveBackend/Program.cs` — add the usings
```csharp
using Azure.Data.Tables;
using Microsoft.AspNetCore.Mvc;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;
```
then insert after the `ILivePollingSessionRepository` registration:
```csharp
builder.Services.AddHttpClient<IDirectMethodClient, HttpDirectMethodClient>((sp, http) =>
{
    http.BaseAddress = new Uri(sp.GetRequiredService<IOptions<LivePollingOptions>>().Value.IoTHubUrl);
    http.Timeout = Timeout.InfiniteTimeSpan;   // HttpDirectMethodClient applies responseTimeout + grace itself
});
builder.Services.AddSingleton(sp => new TableServiceClient(sp.GetRequiredService<IOptions<LivePollingOptions>>().Value.TableConnectionString));
builder.Services.AddSingleton<ILiveSessionReader>(sp => new TableLiveSessionReader(sp.GetRequiredService<TableServiceClient>()));
builder.Services.AddScoped<LiveSessionService>();
```
and replace the `AddControllers()` chain with:
```csharp
builder.Services.AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.Converters.Add(new UtcDateTimeOffsetConverter()))
    .ConfigureApiBehaviorOptions(o => o.InvalidModelStateResponseFactory = context =>
        new BadRequestObjectResult(ApiResponse.Fail(ErrorCodes.InvalidParameters, "Invalid live session parameters.",
            context.ModelState
                .Where(kv => kv.Value is { Errors.Count: > 0 })
                .Select(kv => $"{kv.Key.TrimStart('$', '.')}: {kv.Value!.Errors[0].ErrorMessage}"))));
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveBackend.Tests`
Expected: `Passed!  - Failed: 0, Passed: 70` (Task 10's 18 + keys 8 + row reader 7 + direct-method client 6 + service 24 + controller 7; theory cases count individually).

- [ ] **Step 5: Manual check against the simulator**

```powershell
Start-Process pwsh -ArgumentList '-NoProfile','-NoExit','-Command','dotnet run --project src/Shape.Gateway.Simulator -- --seed 42'
Start-Process pwsh -ArgumentList '-NoProfile','-NoExit','-Command','dotnet run --project src/Shape.LiveBackend'
$b = 'http://localhost:5080/api/v1/devices/PUMP-00417/live-session'
Invoke-RestMethod -Method Post $b | ConvertTo-Json -Depth 5
Invoke-WebRequest -Method Post $b -SkipHttpErrorCheck | Select-Object StatusCode, Content
Invoke-RestMethod $b | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Post "$b/extend" | ConvertTo-Json -Depth 5
Invoke-RestMethod -Method Delete $b | ConvertTo-Json -Depth 5
```
Expected: start `200` with `totalDurationMinutes: 10`; second start `409` with `LIVE_SESSION_ALREADY_ACTIVE`; status `active: true` (live fields null when the function host is not running — that is correct); extend `expiresUtc` = `startedUtc` + 20 min; delete `"Live session stopped."`. Restart the simulator with `--unreachable` and repeat the start: `503 GATEWAY_UNREACHABLE` after ~11 s. Close both windows.

- [ ] **Step 6: Commit**

```powershell
git add src/Shape.LiveBackend tests/Shape.LiveBackend.Tests
git commit -m "feat(backend): add live session start, status, extend and stop over sqlite and the gateway facade"
```

---


### Task 12: Series endpoint — metric registry, step expansion, and the writer → reader seam

**Files:**
- Create: `src/Shape.LiveBackend/Series/{MetricRegistry,SeriesStepExpander,LiveSeriesService}.cs`
- Modify: `src/Shape.LiveBackend/Controllers/LivePollingController.cs`, `src/Shape.LiveBackend/Program.cs`
- Test: `tests/Shape.LiveBackend.Tests/{MetricRegistryTests,SeriesStepExpanderTests,LiveSeriesServiceTests}.cs`, append to `tests/Shape.LiveBackend.Tests/LivePollingControllerTests.cs`
- Create: `tests/Shape.Seam.Tests/{Shape.Seam.Tests.csproj,SeamStorage.cs,WriterReaderSeamTests.cs}`

**Interfaces:**
- Consumes: Task 11 (`ILiveSessionReader`, `ChangePoint`, `LiveHeadView`, `TableLiveSessionReader`, `LiveSessionService.LookupDevice`, `LiveSessionService.NotActive`, `ServiceResult`, backend `LiveSessionKeys`); Task 10 (`SeriesDto`, `SeriesMetricDto`, `LiveEventDto`, `ILivePollingSessionRepository`, `SqliteLivePollingSessionRepository`); ingestion `TableLiveSessionStore`, `SnapshotFolder`, `LiveTelemetryBatch`, `LiveEntry`, `ResolvedDevice`, `AlertDefinitionCatalog`, ingestion `LiveSessionKeys` (Tasks 5–8).
- Produces:
  - `MetricSelection(IReadOnlyList<string> Metrics, IReadOnlyList<string> Errors, string? Message)`
  - `MetricRegistry(IEnumerable<string> fieldNames)` (adds `DeviceStatus`); `MetricRegistry.Load(string seedPath)`; `DefaultSeedPath`; `DeviceStatus = "DeviceStatus"`; `MaxMetrics = 10`; `IReadOnlyList<string> Names` (ordinal-sorted); `Select(IReadOnlyList<string?>? requested, IReadOnlyList<string> defaults) → MetricSelection`
  - `SeriesStepExpander.Grid(DateTimeOffset? firstTick, DateTimeOffset? lastTick, int intervalSeconds, DateTimeOffset? since) → IReadOnlyList<DateTimeOffset>`; `Expand(IReadOnlyList<DateTimeOffset> grid, IReadOnlyList<ChangePoint> points, int intervalSeconds) → IReadOnlyList<JsonElement?>`; `ExpandStatus(...same...) → IReadOnlyList<int>`
  - `LiveSeriesService(IDeviceDirectory, ILivePollingSessionRepository, ILiveSessionReader, MetricRegistry, IOptions<LivePollingOptions>, TimeProvider, ILogger<LiveSeriesService>)`; `GetSeriesAsync(string serial, IReadOnlyList<string?>? metrics, DateTimeOffset? since, CancellationToken ct) → Task<ServiceResult>`
  - `GET api/v1/devices/{serialNumber}/live-session/series?metric=…&since=…`

- [ ] **Step 1: Write failing tests**

`tests/Shape.LiveBackend.Tests/MetricRegistryTests.cs`:
```csharp
using Shape.LiveBackend.Series;

namespace Shape.LiveBackend.Tests;

public class MetricRegistryTests
{
    private static readonly MetricRegistry Registry =
        new(["OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight", "MotorRunning"]);
    private static readonly string[] Defaults = ["OperatingPressurePSI", "StrokeRateLeft"];

    [Fact]
    public void No_metrics_means_the_configured_defaults()
    {
        Assert.Equal(Defaults, Registry.Select(null, Defaults).Metrics);
        Assert.Equal(Defaults, Registry.Select([], Defaults).Metrics);
    }

    [Fact]
    public void Names_match_case_insensitively_and_come_back_in_registry_casing_in_request_order()
    {
        var s = Registry.Select(["strokerateright", "OPERATINGPRESSUREPSI", "devicestatus"], Defaults);
        Assert.Empty(s.Errors);
        Assert.Equal(new[] { "StrokeRateRight", "OperatingPressurePSI", "DeviceStatus" }, s.Metrics);
    }

    [Fact]
    public void Duplicates_collapse_to_the_first_occurrence()
    {
        Assert.Equal(new[] { "StrokeRateLeft", "MotorRunning" },
            Registry.Select(["StrokeRateLeft", "motorrunning", "strokerateleft"], Defaults).Metrics);
    }

    [Fact]
    public void Unknown_and_blank_names_are_each_listed_then_the_sorted_valid_set()
    {
        var s = Registry.Select(["OperatingPresurePSI", "", "OperatingPressurePSI,StrokeRateLeft"], Defaults);

        Assert.Empty(s.Metrics);
        Assert.Equal("Unknown metric requested.", s.Message);
        Assert.Equal(new[]
        {
            "'OperatingPresurePSI' is not a known metric.",
            "Metric names must not be blank.",
            "'OperatingPressurePSI,StrokeRateLeft' is not a known metric.",
            "Valid metrics: DeviceStatus, HydraulicOilTemperatureF, MotorRunning, OperatingPressurePSI, StrokeRateLeft, StrokeRateRight",
        }, s.Errors);
    }

    [Fact]
    public void More_than_ten_distinct_metrics_are_rejected()
    {
        var registry = new MetricRegistry(Enumerable.Range(1, 12).Select(i => $"M{i}"));
        var s = registry.Select(Enumerable.Range(1, 11).Select(i => (string?)$"M{i}").ToList(), Defaults);

        Assert.Equal("Too many metrics requested.", s.Message);
        Assert.Equal("At most 10 metrics may be requested; 11 were.", s.Errors[0]);
        Assert.StartsWith("Valid metrics: ", s.Errors[^1]);
    }

    [Fact]
    public void Ten_metrics_with_duplicates_beyond_ten_are_accepted()
    {
        var registry = new MetricRegistry(Enumerable.Range(1, 12).Select(i => $"M{i}"));
        var request = Enumerable.Range(1, 10).Select(i => (string?)$"M{i}").Append("m1").ToList();
        Assert.Equal(10, registry.Select(request, Defaults).Metrics.Count);
    }

    [Fact]
    public void Real_registry_is_the_seed_field_names_plus_device_status()
    {
        var registry = MetricRegistry.Load(MetricRegistry.DefaultSeedPath);
        Assert.Contains("DeviceStatus", registry.Names);
        Assert.Contains("OperatingPressurePSI", registry.Names);
        Assert.Contains("HydraulicOilTemperatureF", registry.Names);
        Assert.True(registry.Names.Count >= 74, $"{registry.Names.Count} names");
    }
}
```

`tests/Shape.LiveBackend.Tests/SeriesStepExpanderTests.cs`:
```csharp
using System.Text.Json;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Series;

namespace Shape.LiveBackend.Tests;

public class SeriesStepExpanderTests
{
    private static readonly DateTimeOffset First = DateTimeOffset.Parse("2026-10-01T10:00:00.22Z");

    private static ChangePoint P(double seconds, object value) =>
        new(First.AddSeconds(seconds), JsonSerializer.SerializeToElement(value));

    [Fact]
    public void Grid_is_first_tick_plus_k_intervals_up_to_the_last_tick()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(8), 2, null);
        Assert.Equal(new[] { 0, 2, 4, 6, 8 }, grid.Select(t => (int)(t - First).TotalSeconds));
    }

    [Fact]
    public void Grid_is_empty_before_the_first_tick() =>
        Assert.Empty(SeriesStepExpander.Grid(null, null, 1, null));

    [Fact]
    public void Since_keeps_only_grid_points_at_or_after_it()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(4), 1, First.AddSeconds(2.5));
        Assert.Equal(new[] { 3, 4 }, grid.Select(t => (int)Math.Round((t - First).TotalSeconds)));
    }

    [Fact]
    public void Grid_drops_the_newest_tick_when_jitter_shortens_the_span()
    {
        // Five ticks whose last one arrived 2 ms early: floor(3.998) = 3 → four grid points (finding F4).
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(3.998), 1, null);
        Assert.Equal(4, grid.Count);
    }

    [Fact]
    public void A_point_jittered_by_less_than_half_an_interval_lands_on_its_tick()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(3), 1, null);
        var v = SeriesStepExpander.Expand(grid, [P(0, 1), P(0.99, 2), P(3.4, 3)], 1);

        Assert.Equal(new[] { "1", "2", "2", "3" }, v.Select(e => e!.Value.GetRawText()));
    }

    [Fact]
    public void Values_are_null_before_the_first_report()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(2), 1, null);
        var v = SeriesStepExpander.Expand(grid, [P(1, 54000)], 1);

        Assert.Null(v[0]);
        Assert.Equal(54000, v[1]!.Value.GetInt32());
        Assert.Equal(54000, v[2]!.Value.GetInt32());
    }

    [Fact]
    public void A_point_exactly_half_an_interval_late_belongs_to_the_next_tick()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(2), 2, null);
        var v = SeriesStepExpander.Expand(grid, [P(0, "a"), P(1, "b")], 2);

        Assert.Equal(new[] { "\"a\"", "\"b\"" }, v.Select(e => e!.Value.GetRawText()));
    }

    [Fact]
    public void Status_carries_forward_and_is_zero_before_the_first_row()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(3), 1, null);
        Assert.Equal(new[] { 0, 2, 2, 0 }, SeriesStepExpander.ExpandStatus(grid, [P(1, 2L), P(3, 0L)], 1));
    }
}
```

`tests/Shape.LiveBackend.Tests/LiveSeriesServiceTests.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Series;
using static Shape.LiveBackend.Tests.ServiceFixture;

namespace Shape.LiveBackend.Tests;

public sealed class LiveSeriesServiceTests : IDisposable
{
    private readonly ServiceFixture _f = new();
    private readonly LiveSeriesService _series;

    public LiveSeriesServiceTests() =>
        _series = new LiveSeriesService(new ConfigDeviceDirectory(Device), _f.Sessions, _f.Reader,
            new MetricRegistry(["OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight", "OperatingAlarmActive"]),
            Microsoft.Extensions.Options.Options.Create(_f.Options), _f.Time, NullLogger<LiveSeriesService>.Instance);

    public void Dispose() => _f.Dispose();

    private static ChangePoint P(DateTimeOffset at, object value) => new(at, JsonSerializer.SerializeToElement(value));

    private static LiveEventDto E(DateTimeOffset at, string parameter, long value, string state, string? label = null) =>
        new(at, parameter, value, state, "alarm", "operational", "critical", label);

    /// <summary>Session started at T0, ticks at T0+1 … T0+5.</summary>
    private async Task<Guid> SessionWithTicksAsync()
    {
        var id = await _f.StartedSessionAsync();
        var first = T0.AddSeconds(1);
        _f.Reader.Heads[id] = new LiveHeadView(new(), 2, first.AddSeconds(4), 5, 3500, first, first.AddSeconds(4));
        _f.Reader.Points[(id, "OperatingPressurePSI")] = [P(first, 54000), P(first.AddSeconds(2), 54100)];
        _f.Reader.Points[(id, "StrokeRateLeft")] = [P(first, 90)];
        _f.Reader.Points[(id, "DeviceStatus")] = [P(first, 2L), P(first.AddSeconds(3), 0L)];
        _f.Reader.Events[id] =
        [
            E(first, "OperatingAlarmActive", 5, "active", "Emergency Stop Active"),
            E(first.AddSeconds(1), "OperatingWarningActive", 21, "triggered", "Left Overstroke Warning"),
            E(first.AddSeconds(3), "OperatingAlarmActive", 0, "cleared"),
        ];
        _f.Time.Advance(TimeSpan.FromSeconds(6));
        return id;
    }

    [Fact]
    public async Task Default_call_returns_grid_status_default_series_and_events()
    {
        var id = await SessionWithTicksAsync();

        var r = await _series.GetSeriesAsync("PUMP-00417", null, null, default);

        var dto = Assert.IsType<SeriesDto>(r.Body.Data);
        Assert.Equal((id, 1, T0, 5L), (dto.SessionId, dto.PollIntervalSeconds, dto.StartedUtc, dto.TickCount));
        Assert.Equal(T0.AddSeconds(5), dto.LastTickUtc);
        Assert.Equal(5, dto.T.Count);
        Assert.Equal(new[] { 2, 2, 2, 0, 0 }, dto.Status);
        Assert.Equal(new[] { "OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight" },
            dto.Series.Select(s => s.Metric));
        Assert.Equal(new[] { "54000", "54000", "54100", "54100", "54100" }, dto.Series[0].V.Select(v => v!.Value.GetRawText()));
        Assert.All(dto.Series[1].V, v => Assert.Null(v));         // valid but never reported: nulls, not an error
        Assert.Equal(3, dto.Events.Count);
    }

    [Fact]
    public async Task Metric_filter_keeps_status_and_events_and_request_order()
    {
        await SessionWithTicksAsync();

        var dto = Assert.IsType<SeriesDto>((await _series.GetSeriesAsync("PUMP-00417", ["strokerateleft", "OperatingPressurePSI"], null, default)).Body.Data);

        Assert.Equal(new[] { "StrokeRateLeft", "OperatingPressurePSI" }, dto.Series.Select(s => s.Metric));
        Assert.Equal(5, dto.Status.Count);
        Assert.Equal(3, dto.Events.Count);
    }

    [Fact]
    public async Task Each_metric_status_and_events_are_read_once()
    {
        await SessionWithTicksAsync();

        await _series.GetSeriesAsync("PUMP-00417", ["StrokeRateLeft"], null, default);

        Assert.Equal(new[] { "DeviceStatus", "StrokeRateLeft" }, _f.Reader.ParameterReads.Order());
    }

    [Fact]
    public async Task Since_trims_the_grid_and_transitions_but_keeps_active_events()
    {
        await SessionWithTicksAsync();
        var since = T0.AddSeconds(3);

        var dto = Assert.IsType<SeriesDto>((await _series.GetSeriesAsync("PUMP-00417", ["OperatingPressurePSI"], since, default)).Body.Data);

        Assert.Equal(new[] { 3, 4, 5 }, dto.T.Select(t => (int)(t - T0).TotalSeconds));
        Assert.Equal(new[] { 2, 0, 0 }, dto.Status);
        Assert.Equal(new[] { "active", "cleared" }, dto.Events.Select(e => e.State));
    }

    [Fact]
    public async Task Session_without_ticks_returns_empty_arrays()
    {
        await _f.StartedSessionAsync();

        var dto = Assert.IsType<SeriesDto>((await _series.GetSeriesAsync("PUMP-00417", null, null, default)).Body.Data);

        Assert.Empty(dto.T);
        Assert.Empty(dto.Status);
        Assert.All(dto.Series, s => Assert.Empty(s.V));
        Assert.Null(dto.LastTickUtc);
        Assert.Equal(0, dto.TickCount);
    }

    [Fact]
    public async Task No_open_session_is_not_active()
    {
        var r = await _series.GetSeriesAsync("PUMP-00417", null, null, default);
        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, r.Body.Errors![0]));
    }

    [Fact]
    public async Task Invalid_metrics_are_rejected_with_details()
    {
        await SessionWithTicksAsync();

        var r = await _series.GetSeriesAsync("PUMP-00417", ["Nope"], null, default);

        Assert.Equal(400, r.StatusCode);
        Assert.Equal(ErrorCodes.InvalidParameters, r.Body.Errors![0]);
        Assert.Equal("'Nope' is not a known metric.", r.Body.Errors[1]);
        Assert.StartsWith("Valid metrics: ", r.Body.Errors[^1]);
    }
}
```

Append to the `LivePollingControllerTests` class in `tests/Shape.LiveBackend.Tests/LivePollingControllerTests.cs`:
```csharp
    [Fact]
    public async Task Comma_separated_metrics_are_one_invalid_name()
    {
        var r = await _factory.CreateClient().GetAsync(Base + "/series?metric=OperatingPressurePSI,StrokeRateLeft");

        Assert.Equal(HttpStatusCode.BadRequest, r.StatusCode);
        var errors = (await Body(r)).GetProperty("errors");
        Assert.Equal("LIVE_SESSION_INVALID_PARAMETERS", errors[0].GetString());
        Assert.Equal("'OperatingPressurePSI,StrokeRateLeft' is not a known metric.", errors[1].GetString());
        Assert.StartsWith("Valid metrics: ", errors[errors.GetArrayLength() - 1].GetString());
    }

    [Fact]
    public async Task Repeated_metric_parameters_bind_and_reach_the_session_check()
    {
        var r = await _factory.CreateClient().GetAsync(Base + "/series?metric=OperatingPressurePSI&metric=strokerateleft");

        Assert.Equal(HttpStatusCode.Conflict, r.StatusCode);
        Assert.Equal("LIVE_SESSION_NOT_ACTIVE", (await Body(r)).GetProperty("errors")[0].GetString());
    }

    [Fact]
    public async Task Unparseable_since_is_invalid_parameters()
    {
        var r = await _factory.CreateClient().GetAsync(Base + "/series?since=yesterday");

        Assert.Equal(HttpStatusCode.BadRequest, r.StatusCode);
        Assert.Equal("LIVE_SESSION_INVALID_PARAMETERS", (await Body(r)).GetProperty("errors")[0].GetString());
    }
```

Create the seam project:
```powershell
dotnet new xunit -n Shape.Seam.Tests -o tests/Shape.Seam.Tests -f net8.0
Remove-Item tests/Shape.Seam.Tests/UnitTest1.cs
dotnet sln add tests/Shape.Seam.Tests
dotnet add tests/Shape.Seam.Tests reference src/Shape.LiveIngestion src/Shape.LiveBackend
dotnet add tests/Shape.Seam.Tests package Microsoft.Extensions.TimeProvider.Testing
```
Add inside `<Project>` of `tests/Shape.Seam.Tests/Shape.Seam.Tests.csproj`:
```xml
  <ItemGroup>
    <None Include="..\..\config\*.json" Link="config\%(Filename)%(Extension)" CopyToOutputDirectory="PreserveNewest" />
  </ItemGroup>
```

`tests/Shape.Seam.Tests/SeamStorage.cs`:
```csharp
using Azure.Data.Tables;
using Microsoft.Data.Sqlite;

namespace Shape.Seam.Tests;

/// <summary>Azurite (10100–10102) plus a temp SQLite file. Each test gets its own table and file.</summary>
public sealed class SeamStorage : IDisposable
{
    public const string ConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;";

    public TableServiceClient Service { get; } = new(ConnectionString);
    public string Table { get; } = "seam" + Guid.NewGuid().ToString("N")[..20];
    public string DbPath { get; } = Path.Combine(Path.GetTempPath(), "shape-live-tests", $"{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        try { Service.DeleteTable(Table); } catch (Azure.RequestFailedException) { }
        SqliteConnection.ClearAllPools();
        foreach (var f in new[] { DbPath, DbPath + "-wal", DbPath + "-shm" })
            if (File.Exists(f)) File.Delete(f);
    }
}
```

`tests/Shape.Seam.Tests/WriterReaderSeamTests.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Series;
using Shape.LiveBackend.Sessions;
using Ing = Shape.LiveIngestion.LivePolling;

namespace Shape.Seam.Tests;

/// <summary>The ingestion writer and the backend reader share no code — only the Table. These tests are that contract.</summary>
[Trait("Category", "Integration")]
public sealed class WriterReaderSeamTests : IDisposable
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    private static readonly Guid DeviceGuid = Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284");

    private readonly SeamStorage _storage = new();
    private readonly Guid _session = Guid.NewGuid();
    private readonly Ing.TableLiveSessionStore _writer;
    private readonly TableLiveSessionReader _reader;

    public WriterReaderSeamTests()
    {
        _writer = new Ing.TableLiveSessionStore(_storage.Service, _storage.Table);
        _reader = new TableLiveSessionReader(_storage.Service, _storage.Table);
    }

    public void Dispose() => _storage.Dispose();

    private static Dictionary<string, JsonElement>? M(string? json) => json is null
        ? null
        : JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());

    private static Ing.LiveEntry Entry(int sec, string? body = null, string? changes = null, string? ev = null) =>
        new(T0.AddSeconds(sec), M(body), M(changes), M(ev));

    private async Task WriteFirstBatchAsync()
    {
        var catalog = await new Ing.AlertDefinitionCatalog(Ing.AlertDefinitionCatalog.DefaultPath,
            NullLogger<Ing.AlertDefinitionCatalog>.Instance).GetAsync();
        var batch = new Ing.LiveTelemetryBatch(_session, T0.AddSeconds(5), "gw-001", 700,
        [
            Entry(0, body: """{"OK":true,"P":54372,"Temp":104.5,"Mode":"Auto","OperatingAlarmActive":0,"MaintenanceAlarmActive":false}""",
                ev: """{"OperatingAlarmActive":0,"MaintenanceAlarmActive":0,"MotorRunning":1}"""),
            Entry(1, changes: """{"P":54380}"""),
            Entry(2, changes: """{"OperatingAlarmActive":5,"MaintenanceAlarmActive":true}""",
                ev: """{"OperatingAlarmActive":5,"MaintenanceAlarmActive":1}"""),
            Entry(3, changes: "{}"),
            Entry(4, changes: """{"OperatingAlarmActive":0,"MaintenanceAlarmActive":false}""",
                ev: """{"OperatingAlarmActive":0,"MaintenanceAlarmActive":0}"""),
        ]);
        var fold = Ing.SnapshotFolder.Fold(null, batch, new Ing.ResolvedDevice(DeviceGuid, "PUMP-00417"), catalog, NullLogger.Instance);
        await _writer.WriteBatchAsync(_session, fold.Head, fold.ChangePoints, fold.Events);
    }

    private async Task<string[]> Raw(string parameter) =>
        (await _reader.ReadParameterAsync(_session, parameter)).Select(p => p.Value.GetRawText()).ToArray();

    [Fact]
    public async Task Change_points_round_trip_every_native_type()
    {
        await WriteFirstBatchAsync();

        Assert.Equal(new[] { "true" }, await Raw("OK"));
        Assert.Equal(new[] { "54372", "54380" }, await Raw("P"));
        Assert.Equal(new[] { "104.5" }, await Raw("Temp"));
        Assert.Equal(new[] { "\"Auto\"" }, await Raw("Mode"));
        Assert.Equal(new[] { T0, T0.AddSeconds(1) }, (await _reader.ReadParameterAsync(_session, "P")).Select(p => p.TimestampUtc));
    }

    [Fact]
    public async Task Status_rows_are_int64_and_only_where_status_moves()
    {
        await WriteFirstBatchAsync();

        var rows = await _reader.ReadParameterAsync(_session, "DeviceStatus");

        Assert.Equal(new[] { (T0, 0L), (T0.AddSeconds(2), 2L), (T0.AddSeconds(4), 0L) },
            rows.Select(r => (r.TimestampUtc, r.Value.GetInt64())));
    }

    [Fact]
    public async Task Event_rows_round_trip_with_absent_label_read_as_null()
    {
        await WriteFirstBatchAsync();

        var events = await _reader.ReadEventsAsync(_session);

        Assert.Equal(new LiveEventDto[]
        {
            new(T0, "MotorRunning", 1, "active", "status", "operational", "minor", null),
            new(T0.AddSeconds(2), "MaintenanceAlarmActive", 1, "triggered", "alarm", "maintenance", "critical", null),
            new(T0.AddSeconds(2), "OperatingAlarmActive", 5, "triggered", "alarm", "operational", "critical", "Emergency Stop Active"),
            new(T0.AddSeconds(4), "MaintenanceAlarmActive", 0, "cleared", "alarm", "maintenance", "critical", null),
            new(T0.AddSeconds(4), "OperatingAlarmActive", 0, "cleared", "alarm", "operational", "critical", null),
        }, events);
    }

    [Fact]
    public async Task Head_row_round_trips()
    {
        await WriteFirstBatchAsync();

        var head = (await _reader.ReadHeadAsync(_session))!;

        Assert.Equal((5L, 700L, (int?)0), (head.TickCount, head.BytesTotal, head.DeviceStatus));
        Assert.Equal((T0, T0.AddSeconds(4), T0.AddSeconds(4)), (head.FirstTickUtc!.Value, head.LastTickUtc!.Value, head.LastSnapshotTimestampUtc!.Value));
        Assert.Equal(54380, head.Snapshot["P"].GetInt32());
        Assert.True(head.Snapshot["OK"].GetBoolean());
    }

    [Fact]
    public async Task Reading_a_session_that_was_never_written_returns_nothing()
    {
        Assert.Null(await _reader.ReadHeadAsync(_session));
        Assert.Empty(await _reader.ReadParameterAsync(_session, "P"));
        Assert.Empty(await _reader.ReadEventsAsync(_session));
    }

    [Theory]
    [InlineData("OperatingPressurePSI")]
    [InlineData("DeviceStatus")]
    [InlineData("StrokeRate")]
    public void The_two_key_copies_agree(string parameter)
    {
        var ts = DateTimeOffset.Parse("2026-09-28T10:27:02.2180000+02:00");
        Assert.Equal(Ing.LiveSessionKeys.ChangeRowKey(parameter, ts), LiveSessionKeys.ChangeRowKey(parameter, ts));
        Assert.Equal(Ing.LiveSessionKeys.EventRowKey(parameter, ts), LiveSessionKeys.EventRowKey(parameter, ts));
        Assert.Equal(Ing.LiveSessionKeys.ParamRange(parameter, ts), LiveSessionKeys.ParamRange(parameter, ts));
        Assert.Equal(Ing.LiveSessionKeys.EventRange(ts), LiveSessionKeys.EventRange(ts));
        Assert.Equal(Ing.LiveSessionKeys.HeadRowKey, LiveSessionKeys.HeadRowKey);
    }

    [Fact]
    public async Task Series_over_a_written_partition_matches_what_was_folded()
    {
        await WriteFirstBatchAsync();
        var sessions = new SqliteLivePollingSessionRepository(_storage.DbPath);
        await sessions.EnsureSchemaAsync();
        await sessions.InsertAsync(new LivePollingSessionRecord
        {
            SessionId = _session, DeviceGuid = DeviceGuid, DeviceId = "PUMP-00417", GatewayId = "gw-001", StartedUtc = T0.AddSeconds(-1),
            PollIntervalSeconds = 1, SessionDurationMinutes = 10, ExpiresUtc = T0.AddMinutes(10),
        });
        var series = new LiveSeriesService(
            new ConfigDeviceDirectory(new DeviceConfig("PUMP-00417", DeviceGuid, "PREC-NEO30-MARYL-07", "gw-001", "DefaultModeModule")),
            sessions, _reader, new MetricRegistry(["OK", "P", "Temp", "Mode", "OperatingAlarmActive", "MaintenanceAlarmActive"]),
            Microsoft.Extensions.Options.Options.Create(new Shape.LiveBackend.Options.LivePollingOptions()),
            new FakeTimeProvider(T0.AddSeconds(10)), NullLogger<LiveSeriesService>.Instance);

        var r = await series.GetSeriesAsync("PUMP-00417", ["P", "OK"], null, default);

        var dto = Assert.IsType<SeriesDto>(r.Body.Data);
        Assert.Equal(5, dto.T.Count);
        Assert.Equal(new[] { 0, 0, 2, 2, 0 }, dto.Status);
        Assert.Equal(new[] { "54372", "54380", "54380", "54380", "54380" }, dto.Series[0].V.Select(v => v!.Value.GetRawText()));
        Assert.All(dto.Series[1].V, v => Assert.True(v!.Value.GetBoolean()));
        Assert.Equal(5, dto.Events.Count);
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveBackend.Tests`
Expected: FAIL — `The type or namespace name 'Series' does not exist in the namespace 'Shape.LiveBackend'`.

- [ ] **Step 3: Implement**

`src/Shape.LiveBackend/Series/MetricRegistry.cs`:
```csharp
using System.Text.Json;

namespace Shape.LiveBackend.Series;

public sealed record MetricSelection(IReadOnlyList<string> Metrics, IReadOnlyList<string> Errors, string? Message);

/// <summary>
/// Stand-in for dbo.ParameterRegistry (spec §11): the seed's field names plus the synthetic DeviceStatus.
/// Matched case-insensitively, returned in registry casing.
/// </summary>
public sealed class MetricRegistry
{
    public const string DeviceStatus = "DeviceStatus";
    public const int MaxMetrics = 10;
    public static readonly string DefaultSeedPath = Path.Combine(AppContext.BaseDirectory, "config", "pump-seed.json");

    private readonly Dictionary<string, string> _byName;

    public MetricRegistry(IEnumerable<string> fieldNames)
    {
        _byName = fieldNames.Append(DeviceStatus).Distinct(StringComparer.OrdinalIgnoreCase)
            .ToDictionary(n => n, n => n, StringComparer.OrdinalIgnoreCase);
        Names = _byName.Values.Order(StringComparer.Ordinal).ToList();
    }

    public IReadOnlyList<string> Names { get; }

    public static MetricRegistry Load(string seedPath)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(seedPath));
        return new MetricRegistry(doc.RootElement.EnumerateObject().Select(p => p.Name).ToList());
    }

    public MetricSelection Select(IReadOnlyList<string?>? requested, IReadOnlyList<string> defaults)
    {
        IReadOnlyList<string?> input = requested is { Count: > 0 } ? requested : defaults;
        var chosen = new List<string>();
        var errors = new List<string>();
        var unknown = false;

        foreach (var name in input)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                errors.Add("Metric names must not be blank.");
                unknown = true;
            }
            else if (!_byName.TryGetValue(name, out var canonical))
            {
                errors.Add($"'{name}' is not a known metric.");
                unknown = true;
            }
            else if (!chosen.Contains(canonical))
            {
                chosen.Add(canonical);
            }
        }
        if (chosen.Count > MaxMetrics) errors.Add($"At most {MaxMetrics} metrics may be requested; {chosen.Count} were.");

        if (errors.Count == 0) return new MetricSelection(chosen, [], null);
        errors.Add("Valid metrics: " + string.Join(", ", Names));
        return new MetricSelection([], errors, unknown ? "Unknown metric requested." : "Too many metrics requested.");
    }
}
```

`src/Shape.LiveBackend/Series/SeriesStepExpander.cs`:
```csharp
using System.Text.Json;
using Shape.LiveBackend.LiveData;

namespace Shape.LiveBackend.Series;

/// <summary>
/// Spec §7.3. Grid tₖ = FirstTickUtc + k·interval for k = 0..floor((LastTickUtc − FirstTickUtc)/interval).
/// The value at tₖ is the last change point with TimestampUtc &lt; tₖ + interval/2 (absorbs gateway jitter).
/// </summary>
public static class SeriesStepExpander
{
    public static IReadOnlyList<DateTimeOffset> Grid(DateTimeOffset? firstTick, DateTimeOffset? lastTick, int intervalSeconds, DateTimeOffset? since)
    {
        if (firstTick is not { } first || lastTick is not { } last || last < first) return [];
        var interval = TimeSpan.FromSeconds(intervalSeconds);
        var steps = (last - first).Ticks / interval.Ticks;           // integer division = floor for non-negative spans
        var grid = new List<DateTimeOffset>((int)steps + 1);
        for (var k = 0L; k <= steps; k++)
        {
            var t = first + TimeSpan.FromTicks(interval.Ticks * k);
            if (since is null || t >= since) grid.Add(t);
        }
        return grid;
    }

    public static IReadOnlyList<JsonElement?> Expand(IReadOnlyList<DateTimeOffset> grid, IReadOnlyList<ChangePoint> points, int intervalSeconds)
    {
        var half = TimeSpan.FromSeconds(intervalSeconds) / 2;
        var values = new List<JsonElement?>(grid.Count);
        JsonElement? current = null;
        var j = 0;
        foreach (var t in grid)
        {
            while (j < points.Count && points[j].TimestampUtc < t + half) current = points[j++].Value;
            values.Add(current);
        }
        return values;
    }

    public static IReadOnlyList<int> ExpandStatus(IReadOnlyList<DateTimeOffset> grid, IReadOnlyList<ChangePoint> points, int intervalSeconds) =>
        Expand(grid, points, intervalSeconds).Select(v => v is { ValueKind: JsonValueKind.Number } n ? n.GetInt32() : 0).ToList();
}
```

`src/Shape.LiveBackend/Series/LiveSeriesService.cs`:
```csharp
using Microsoft.Extensions.Options;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Options;
using Shape.LiveBackend.Sessions;

namespace Shape.LiveBackend.Series;

/// <summary>GET /live-session/series (spec §7.3). One range read per metric, plus status and events, in parallel.</summary>
public sealed class LiveSeriesService(
    IDeviceDirectory devices, ILivePollingSessionRepository sessions, ILiveSessionReader live, MetricRegistry registry,
    IOptions<LivePollingOptions> options, TimeProvider time, ILogger<LiveSeriesService> logger)
{
    public async Task<ServiceResult> GetSeriesAsync(string serial, IReadOnlyList<string?>? metrics, DateTimeOffset? since, CancellationToken ct)
    {
        if (LiveSessionService.LookupDevice(devices, serial, out var device) is { } notFound) return notFound;

        var selection = registry.Select(metrics, options.Value.DefaultSeriesMetrics);
        if (selection.Errors.Count > 0)
            return ServiceResult.Fail(400, ErrorCodes.InvalidParameters, selection.Message!, selection.Errors);

        var open = await sessions.GetOpenForDeviceAsync(device.DeviceGuid, time.GetUtcNow(), ct);
        if (open is null) return LiveSessionService.NotActive();

        var id = open.SessionId;
        var interval = open.PollIntervalSeconds;
        try
        {
            var head = live.ReadHeadAsync(id, ct);
            var reads = selection.Metrics.Select(m => live.ReadParameterAsync(id, m, ct)).ToList();
            var status = selection.Metrics.Contains(MetricRegistry.DeviceStatus)
                ? reads[selection.Metrics.ToList().IndexOf(MetricRegistry.DeviceStatus)]
                : live.ReadParameterAsync(id, MetricRegistry.DeviceStatus, ct);
            var events = live.ReadEventsAsync(id, ct);
            await Task.WhenAll(reads.Cast<Task>().Append(head).Append(status).Append(events));

            var h = await head;
            var grid = SeriesStepExpander.Grid(h?.FirstTickUtc, h?.LastTickUtc, interval, since);
            var series = new List<SeriesMetricDto>();
            for (var i = 0; i < selection.Metrics.Count; i++)
                series.Add(new SeriesMetricDto(selection.Metrics[i], SeriesStepExpander.Expand(grid, await reads[i], interval)));
            var kept = (await events)
                .Where(e => since is null || e.State == "active" || e.TimestampUtc >= since)
                .ToList();

            return ServiceResult.Ok(new SeriesDto(id, interval, open.StartedUtc, h?.LastTickUtc, h?.TickCount ?? 0, grid,
                SeriesStepExpander.ExpandStatus(grid, await status, interval), series, kept));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Reading the series of session {SessionId} failed", id);
            return new ServiceResult(500, new ApiResponse { Succeeded = false, Message = "Live session data could not be read." });
        }
    }
}
```

In `src/Shape.LiveBackend/Controllers/LivePollingController.cs` add `using Shape.LiveBackend.Series;` and this action:
```csharp
    /// <summary>Repeat ?metric= for several; a comma-separated value is one (invalid) name, as in the reference contract.</summary>
    [HttpGet("series")]
    public async Task<IActionResult> Series(string serialNumber, [FromQuery(Name = "metric")] string?[]? metric,
        [FromQuery] DateTimeOffset? since, [FromServices] LiveSeriesService series, CancellationToken ct) =>
        From(await series.GetSeriesAsync(serialNumber, metric, since, ct));
```

In `src/Shape.LiveBackend/Program.cs` add `using Shape.LiveBackend.Series;` and, after `AddScoped<LiveSessionService>()`:
```csharp
builder.Services.AddSingleton(_ => MetricRegistry.Load(MetricRegistry.DefaultSeedPath));
builder.Services.AddScoped<LiveSeriesService>();
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveBackend.Tests`
Expected: `Passed!  - Failed: 0, Passed: 95` (70 + registry 7 + expander 8 + series service 7 + controller 3).

With Azurite running: `dotnet test tests/Shape.Seam.Tests`
Expected: `Passed!  - Failed: 0, Passed: 9` (6 facts + 3 theory cases).

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.LiveBackend tests/Shape.LiveBackend.Tests tests/Shape.Seam.Tests ShapeLive.sln
git commit -m "feat(backend): add live series endpoint with step expansion and writer-reader seam tests"
```

---


### Task 13: `LivePollingCleanupTimer` — purge finished sessions from SQLite-driven list

**Files:**
- Create: `src/Shape.LiveIngestion/Infrastructure/RepoPaths.cs`
- Create: `src/Shape.LiveIngestion/LivePolling/{CleanupSessionSource,LivePollingCleanup}.cs`
- Create: `src/Shape.LiveIngestion/Functions/LivePollingCleanupTimerFunction.cs`
- Modify: `src/Shape.LiveIngestion/Shape.LiveIngestion.csproj` (Timer extension), `src/Shape.LiveIngestion/Program.cs`
- Test: `tests/Shape.LiveIngestion.Tests/LivePollingCleanupTests.cs`, `tests/Shape.Seam.Tests/CleanupSeamTests.cs`

**Interfaces:**
- Consumes: `ILiveSessionStore` (`LoadHeadAsync`, `DeletePartitionAsync`), `TableLiveSessionStore`, `SnapshotFolder` (Tasks 7–8); `FakeLiveSessionStore` is **not** used (it ignores the session id) — the test below has its own multi-session fake. Seam test: backend `SqliteLivePollingSessionRepository`, `LivePollingSessionRecord`, `TableLiveSessionReader` (Tasks 10–11).
- Produces:
  - `RepoPaths.Root`, `RepoPaths.Resolve(string)` (ingestion copy, same behaviour as the backend's)
  - `ICleanupSessionSource`: `EnsureSchemaAsync(ct)`, `GetFinishedUnpurgedAsync(DateTimeOffset now, ct) → Task<IReadOnlyList<Guid>>`, `RecordCountersAsync(Guid sessionId, long tickCount, long bytesTotal, ct)`, `MarkPurgedAsync(Guid sessionId, DateTimeOffset purgedUtc, ct)`; `SqliteCleanupSessionSource(string dbPath)`; `CleanupSessionSource.Ddl` (identical to the backend's `SessionSchema.Ddl`)
  - `CleanupReport(int Purged, int Failed)`; `LivePollingCleanup(ICleanupSessionSource, ILiveSessionStore, TimeProvider, ILogger<LivePollingCleanup>)`; `RunAsync(CancellationToken ct = default) → Task<CleanupReport>`
  - Function `LivePollingCleanupTimer` on `%LivePolling:CleanupSchedule%` (local `*/15 * * * * *`, set in Task 5 — faster than the spec's one-minute local default so the smoke run's "within 2 timer runs" stays short)

- [ ] **Step 1: Add the Timer extension**

```powershell
dotnet add src/Shape.LiveIngestion package Microsoft.Azure.Functions.Worker.Extensions.Timer --version 4.3.1
```
(4.3.1 is pinned: newer Timer extensions pull Microsoft.Extensions 10, which the Core Tools 4.9 host cannot load.)

- [ ] **Step 2: Write failing tests**

`tests/Shape.LiveIngestion.Tests/LivePollingCleanupTests.cs`:
```csharp
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public sealed class LivePollingCleanupTests : IDisposable
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");

    /// <summary>Per-session heads and row counts; DeleteFails makes one session's delete throw.</summary>
    private sealed class MultiSessionStore : ILiveSessionStore
    {
        public Dictionary<Guid, LiveHead> Heads { get; } = new();
        public Dictionary<Guid, int> Rows { get; } = new();
        public HashSet<Guid> DeleteFails { get; } = [];
        public List<Guid> Deleted { get; } = [];

        public Task<LiveHead?> LoadHeadAsync(Guid sessionId, CancellationToken ct = default) =>
            Task.FromResult(Heads.GetValueOrDefault(sessionId));

        public Task WriteBatchAsync(Guid sessionId, LiveHead head, IReadOnlyList<ChangePointWrite> changePoints,
            IReadOnlyList<LiveEventWrite> events, CancellationToken ct = default) => throw new NotSupportedException();

        public Task<int> DeletePartitionAsync(Guid sessionId, CancellationToken ct = default)
        {
            if (DeleteFails.Contains(sessionId)) throw new InvalidOperationException("table down");
            Deleted.Add(sessionId);
            Heads.Remove(sessionId);
            return Task.FromResult(Rows.Remove(sessionId, out var n) ? n : 0);
        }
    }

    private readonly string _db = Path.Combine(Path.GetTempPath(), "shape-live-tests", $"{Guid.NewGuid():N}.db");
    private readonly SqliteCleanupSessionSource _source;
    private readonly MultiSessionStore _store = new();
    private readonly FakeTimeProvider _time = new(T0);
    private readonly LivePollingCleanup _cleanup;

    public LivePollingCleanupTests()
    {
        _source = new SqliteCleanupSessionSource(_db);
        _source.EnsureSchemaAsync().GetAwaiter().GetResult();
        _cleanup = new LivePollingCleanup(_source, _store, _time, NullLogger<LivePollingCleanup>.Instance);
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var f in new[] { _db, _db + "-wal", _db + "-shm" })
            if (File.Exists(f)) File.Delete(f);
    }

    private Guid Insert(DateTimeOffset expires, DateTimeOffset? stopped = null, DateTimeOffset? purged = null)
    {
        var id = Guid.NewGuid();
        using var c = new SqliteConnection($"Data Source={_db}");
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText =
            "INSERT INTO LivePollingSession (SessionId, DeviceGuid, DeviceId, GatewayId, StartedUtc, StartedByUserId, PollIntervalSeconds, " +
            "SessionDurationMinutes, ExpiresUtc, StoppedUtc, PurgedUtc) VALUES ($id, 'd', 'PUMP-00417', 'gw-001', $start, 'local', 1, 10, $exp, $stop, $purged)";
        cmd.Parameters.AddWithValue("$id", id.ToString());
        cmd.Parameters.AddWithValue("$start", CleanupSessionSource.Ts(expires.AddMinutes(-10)));
        cmd.Parameters.AddWithValue("$exp", CleanupSessionSource.Ts(expires));
        cmd.Parameters.AddWithValue("$stop", stopped is { } s ? CleanupSessionSource.Ts(s) : DBNull.Value);
        cmd.Parameters.AddWithValue("$purged", purged is { } p ? CleanupSessionSource.Ts(p) : DBNull.Value);
        cmd.ExecuteNonQuery();
        return id;
    }

    private (string? Purged, long? Ticks, long? Bytes) Row(Guid id)
    {
        using var c = new SqliteConnection($"Data Source={_db}");
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT PurgedUtc, TickCount, BytesTotal FROM LivePollingSession WHERE SessionId = $id";
        cmd.Parameters.AddWithValue("$id", id.ToString());
        using var r = cmd.ExecuteReader();
        r.Read();
        return (r.IsDBNull(0) ? null : r.GetString(0), r.IsDBNull(1) ? null : r.GetInt64(1), r.IsDBNull(2) ? null : r.GetInt64(2));
    }

    private void HeadFor(Guid id, long ticks, long bytes, int rows)
    {
        _store.Heads[id] = new LiveHead { TickCount = ticks, BytesTotal = bytes };
        _store.Rows[id] = rows;
    }

    [Fact]
    public async Task Purges_stopped_and_expired_sessions_but_not_open_ones()
    {
        var open = Insert(T0.AddMinutes(5));
        var stopped = Insert(T0.AddMinutes(5), stopped: T0.AddMinutes(-1));
        var expired = Insert(T0.AddSeconds(-1));
        foreach (var id in new[] { open, stopped, expired }) HeadFor(id, 10, 1000, 30);

        var report = await _cleanup.RunAsync();

        Assert.Equal(new CleanupReport(2, 0), report);
        Assert.Equal(new[] { stopped, expired }.Order(), _store.Deleted.Order());
        Assert.Null(Row(open).Purged);
        Assert.Equal(CleanupSessionSource.Ts(T0), Row(stopped).Purged);
    }

    [Fact]
    public async Task Counters_are_harvested_from_the_head_before_the_partition_is_deleted()
    {
        var id = Insert(T0.AddSeconds(-1));
        HeadFor(id, 412, 97644, 900);

        await _cleanup.RunAsync();

        var row = Row(id);
        Assert.Equal((CleanupSessionSource.Ts(T0), 412L, 97644L), (row.Purged, row.Ticks!.Value, row.Bytes!.Value));
    }

    [Fact]
    public async Task A_session_that_never_ticked_is_purged_without_counters()
    {
        var id = Insert(T0.AddSeconds(-1));

        await _cleanup.RunAsync();

        var row = Row(id);
        Assert.NotNull(row.Purged);
        Assert.Null(row.Ticks);
        Assert.Null(row.Bytes);
    }

    [Fact]
    public async Task Already_purged_sessions_are_not_touched_again()
    {
        Insert(T0.AddMinutes(-30), purged: T0.AddMinutes(-15));

        var report = await _cleanup.RunAsync();

        Assert.Equal(new CleanupReport(0, 0), report);
        Assert.Empty(_store.Deleted);
    }

    [Fact]
    public async Task A_failing_session_is_skipped_this_run_and_retried_on_the_next()
    {
        var bad = Insert(T0.AddSeconds(-1));
        var good = Insert(T0.AddSeconds(-2));
        HeadFor(bad, 5, 500, 10);
        HeadFor(good, 5, 500, 10);
        _store.DeleteFails.Add(bad);

        Assert.Equal(new CleanupReport(1, 1), await _cleanup.RunAsync());
        Assert.Null(Row(bad).Purged);

        _store.DeleteFails.Clear();
        _time.Advance(TimeSpan.FromSeconds(15));
        Assert.Equal(new CleanupReport(1, 0), await _cleanup.RunAsync());
        Assert.Equal(CleanupSessionSource.Ts(T0.AddSeconds(15)), Row(bad).Purged);
    }
}
```

`tests/Shape.Seam.Tests/CleanupSeamTests.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Sessions;
using Ing = Shape.LiveIngestion.LivePolling;

namespace Shape.Seam.Tests;

/// <summary>Backend writes the SQLite rows, ingestion writes the partitions, the cleanup timer reads one and purges the other.</summary>
[Trait("Category", "Integration")]
public sealed class CleanupSeamTests : IDisposable
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    private static readonly Guid DeviceGuid = Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284");
    private readonly SeamStorage _storage = new();

    public void Dispose() => _storage.Dispose();

    private static LivePollingSessionRecord Session(DateTimeOffset started) => new()
    {
        SessionId = Guid.NewGuid(), DeviceGuid = DeviceGuid, DeviceId = "PUMP-00417", GatewayId = "gw-001", StartedUtc = started,
        PollIntervalSeconds = 1, SessionDurationMinutes = 10, ExpiresUtc = started.AddMinutes(10),
    };

    /// <summary>The 73-field seed as a baseline plus four 8-field deltas: more than 100 rows.</summary>
    private static Ing.FoldResult LargeBatch(Guid sessionId)
    {
        var seed = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "config", "pump-seed.json")))!;
        var keys = new[] { "OperatingPressurePSI", "OperatingPressureBar", "StrokeRateLeft", "StrokeRateRight",
            "StrokeTimeLeft", "StrokeTimeRight", "StrokeCounterTW1", "ProportionalOutputValue" };
        var entries = new List<Ing.LiveEntry> { new(T0, seed, null, null) };
        for (var i = 1; i < 5; i++)
            entries.Add(new(T0.AddSeconds(i), null, keys.ToDictionary(k => k, _ => JsonSerializer.SerializeToElement(i)), null));
        return Ing.SnapshotFolder.Fold(null, new Ing.LiveTelemetryBatch(sessionId, T0.AddSeconds(5), "gw-001", 1234, entries),
            new Ing.ResolvedDevice(DeviceGuid, "PUMP-00417"), new Dictionary<string, Ing.AlertDefinition>(), NullLogger.Instance);
    }

    [Fact]
    public async Task Cleanup_purges_only_finished_sessions_harvests_counters_and_deletes_large_partitions()
    {
        var sessions = new SqliteLivePollingSessionRepository(_storage.DbPath);
        await sessions.EnsureSchemaAsync();
        var open = Session(T0);
        var stopped = Session(T0.AddMinutes(-2));
        var expired = Session(T0.AddMinutes(-11));
        foreach (var s in new[] { open, stopped, expired }) await sessions.InsertAsync(s);
        await sessions.MarkStoppedAsync(stopped.SessionId, T0.AddMinutes(-1), "ManualStop");

        var store = new Ing.TableLiveSessionStore(_storage.Service, _storage.Table);
        foreach (var s in new[] { open, stopped, expired })
        {
            var fold = LargeBatch(s.SessionId);
            Assert.True(fold.EntityCount > 100);
            await store.WriteBatchAsync(s.SessionId, fold.Head, fold.ChangePoints, fold.Events);
        }

        var cleanup = new Ing.LivePollingCleanup(new Ing.SqliteCleanupSessionSource(_storage.DbPath), store,
            new FakeTimeProvider(T0.AddSeconds(30)), NullLogger<Ing.LivePollingCleanup>.Instance);
        var report = await cleanup.RunAsync();

        Assert.Equal(new Ing.CleanupReport(2, 0), report);
        var reader = new TableLiveSessionReader(_storage.Service, _storage.Table);
        foreach (var s in new[] { stopped, expired })
        {
            var row = (await sessions.GetAsync(s.SessionId))!;
            Assert.Equal((T0.AddSeconds(30), 5L, 1234L), (row.PurgedUtc!.Value, row.TickCount!.Value, row.BytesTotal!.Value));
            Assert.Null(await reader.ReadHeadAsync(s.SessionId));
            Assert.Empty(await reader.ReadParameterAsync(s.SessionId, "OperatingPressurePSI"));
        }
        Assert.Null((await sessions.GetAsync(open.SessionId))!.PurgedUtc);
        Assert.NotNull(await reader.ReadHeadAsync(open.SessionId));
    }

    [Fact]
    public async Task Schema_created_by_the_function_is_the_backend_schema()
    {
        await new Ing.SqliteCleanupSessionSource(_storage.DbPath).EnsureSchemaAsync();
        var sessions = new SqliteLivePollingSessionRepository(_storage.DbPath);

        var s = Session(T0);
        await sessions.InsertAsync(s);

        Assert.Equal(s, await sessions.GetAsync(s.SessionId));
        Assert.Equal(Ing.CleanupSessionSource.Ddl, SessionSchema.Ddl);
    }
}
```

- [ ] **Step 3: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.LiveIngestion.Tests --filter FullyQualifiedName~LivePollingCleanup`
Expected: FAIL — `The type or namespace name 'SqliteCleanupSessionSource' could not be found`.

- [ ] **Step 4: Implement**

`src/Shape.LiveIngestion/Infrastructure/RepoPaths.cs`:
```csharp
namespace Shape.LiveIngestion.Infrastructure;

/// <summary>Repo-relative paths (data/live-polling.db) resolve against the directory holding ShapeLive.sln.</summary>
public static class RepoPaths
{
    public static string Root { get; } = Find();

    public static string Resolve(string path) =>
        Path.IsPathRooted(path) ? path : Path.GetFullPath(Path.Combine(Root, path.Replace('/', Path.DirectorySeparatorChar)));

    private static string Find()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "ShapeLive.sln"))) return dir.FullName;
        throw new InvalidOperationException($"ShapeLive.sln not found above {AppContext.BaseDirectory}.");
    }
}
```

`src/Shape.LiveIngestion/LivePolling/CleanupSessionSource.cs`:
```csharp
using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>The cleanup timer's view of LivePollingSession (spec §5.4). Never scans Table Storage.</summary>
public interface ICleanupSessionSource
{
    Task EnsureSchemaAsync(CancellationToken ct = default);
    Task<IReadOnlyList<Guid>> GetFinishedUnpurgedAsync(DateTimeOffset now, CancellationToken ct = default);
    Task RecordCountersAsync(Guid sessionId, long tickCount, long bytesTotal, CancellationToken ct = default);
    Task MarkPurgedAsync(Guid sessionId, DateTimeOffset purgedUtc, CancellationToken ct = default);
}

public static class CleanupSessionSource
{
    /// <summary>Identical to the backend's SessionSchema.Ddl (checked by Shape.Seam.Tests).</summary>
    public const string Ddl = """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS LivePollingSession (
          SessionId TEXT PRIMARY KEY, DeviceGuid TEXT NOT NULL, DeviceId TEXT NOT NULL, GatewayId TEXT NOT NULL,
          StartedUtc TEXT NOT NULL, StartedByUserId TEXT NOT NULL, PollIntervalSeconds INTEGER NOT NULL,
          SessionDurationMinutes INTEGER NOT NULL, ExpiresUtc TEXT NOT NULL, ExtensionsUsed INTEGER NOT NULL DEFAULT 0,
          LastExtendedUtc TEXT, StoppedUtc TEXT, StopReason TEXT, TickCount INTEGER, BytesTotal INTEGER, PurgedUtc TEXT);
        CREATE INDEX IF NOT EXISTS IX_Session_Device_Expires ON LivePollingSession(DeviceGuid, ExpiresUtc);
        CREATE INDEX IF NOT EXISTS IX_Session_Unpurged ON LivePollingSession(PurgedUtc) WHERE PurgedUtc IS NULL;
        """;

    public static string Ts(DateTimeOffset value) => value.UtcDateTime.ToString("O", CultureInfo.InvariantCulture);
}

public sealed class SqliteCleanupSessionSource(string dbPath) : ICleanupSessionSource
{
    private readonly string _connectionString = new SqliteConnectionStringBuilder
    {
        DataSource = dbPath, Mode = SqliteOpenMode.ReadWriteCreate, Pooling = true,
    }.ToString();

    public async Task EnsureSchemaAsync(CancellationToken ct = default)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(dbPath))!);
        await ExecuteAsync(CleanupSessionSource.Ddl, ct);
    }

    public async Task<IReadOnlyList<Guid>> GetFinishedUnpurgedAsync(DateTimeOffset now, CancellationToken ct = default)
    {
        await using var c = await OpenAsync(ct);
        await using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT SessionId FROM LivePollingSession " +
                          "WHERE PurgedUtc IS NULL AND (StoppedUtc IS NOT NULL OR ExpiresUtc < $now) ORDER BY ExpiresUtc";
        cmd.Parameters.AddWithValue("$now", CleanupSessionSource.Ts(now));
        var ids = new List<Guid>();
        await using var r = await cmd.ExecuteReaderAsync(ct);
        while (await r.ReadAsync(ct)) ids.Add(Guid.Parse(r.GetString(0)));
        return ids;
    }

    public Task RecordCountersAsync(Guid sessionId, long tickCount, long bytesTotal, CancellationToken ct = default) =>
        ExecuteAsync("UPDATE LivePollingSession SET TickCount = $ticks, BytesTotal = $bytes WHERE SessionId = $id", ct,
            ("$ticks", tickCount), ("$bytes", bytesTotal), ("$id", sessionId.ToString()));

    public Task MarkPurgedAsync(Guid sessionId, DateTimeOffset purgedUtc, CancellationToken ct = default) =>
        ExecuteAsync("UPDATE LivePollingSession SET PurgedUtc = $at WHERE SessionId = $id", ct,
            ("$at", CleanupSessionSource.Ts(purgedUtc)), ("$id", sessionId.ToString()));

    private async Task<SqliteConnection> OpenAsync(CancellationToken ct)
    {
        var c = new SqliteConnection(_connectionString);
        await c.OpenAsync(ct);
        return c;
    }

    private async Task ExecuteAsync(string sql, CancellationToken ct, params (string Name, object Value)[] args)
    {
        await using var c = await OpenAsync(ct);
        await using var cmd = c.CreateCommand();
        cmd.CommandText = sql;
        foreach (var (name, value) in args) cmd.Parameters.AddWithValue(name, value);
        await cmd.ExecuteNonQueryAsync(ct);
    }
}
```

`src/Shape.LiveIngestion/LivePolling/LivePollingCleanup.cs`:
```csharp
using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

public sealed record CleanupReport(int Purged, int Failed);

/// <summary>
/// Spec §5.4: for every finished, unpurged session — harvest TickCount/BytesTotal from the head (if any), delete the
/// partition (≤ 100 per transaction), stamp PurgedUtc. A failure leaves PurgedUtc null, so the next run retries it.
/// </summary>
public sealed class LivePollingCleanup(
    ICleanupSessionSource sessions, ILiveSessionStore store, TimeProvider time, ILogger<LivePollingCleanup> logger)
{
    public async Task<CleanupReport> RunAsync(CancellationToken ct = default)
    {
        await sessions.EnsureSchemaAsync(ct);
        var now = time.GetUtcNow();
        int purged = 0, failed = 0;
        foreach (var id in await sessions.GetFinishedUnpurgedAsync(now, ct))
        {
            try
            {
                if (await store.LoadHeadAsync(id, ct) is { } head)
                    await sessions.RecordCountersAsync(id, head.TickCount, head.BytesTotal, ct);
                var rows = await store.DeletePartitionAsync(id, ct);
                await sessions.MarkPurgedAsync(id, now, ct);
                logger.LogInformation("Purged live session {SessionId}: {Rows} rows", id, rows);
                purged++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Purging live session {SessionId} failed; retrying next run", id);
                failed++;
            }
        }
        return new CleanupReport(purged, failed);
    }
}
```

`src/Shape.LiveIngestion/Functions/LivePollingCleanupTimerFunction.cs`:
```csharp
using Microsoft.Azure.Functions.Worker;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Functions;

public sealed class LivePollingCleanupTimerFunction(LivePollingCleanup cleanup)
{
    [Function("LivePollingCleanupTimer")]
    public Task Run([TimerTrigger("%LivePolling:CleanupSchedule%")] TimerInfo timer, CancellationToken ct) => cleanup.RunAsync(ct);
}
```

In `src/Shape.LiveIngestion/Program.cs` add `using Shape.LiveIngestion.Infrastructure;` and, inside `ConfigureServices` after the `LiveTelemetryProcessor` registration:
```csharp
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<ICleanupSessionSource>(_ =>
            new SqliteCleanupSessionSource(RepoPaths.Resolve(config["LivePolling:SessionDbPath"] ?? "data/live-polling.db")));
        services.AddSingleton<LivePollingCleanup>();
```

- [ ] **Step 5: Run tests — expect pass**

Run: `dotnet test tests/Shape.LiveIngestion.Tests`
Expected: `Passed!  - Failed: 0, Passed: 94` (89 + cleanup 5).

With Azurite running: `dotnet test tests/Shape.Seam.Tests`
Expected: `Passed!  - Failed: 0, Passed: 11` (9 + cleanup seam 2).

Manual: start Azurite and the SignalR emulator, then `func start` in `src/Shape.LiveIngestion`. Expected in the host listing: `LivePollingCleanupTimer: timerTrigger`, and every 15 s no error line from it (an empty run logs nothing). `data/live-polling.db` exists at the repo root afterwards.

- [ ] **Step 6: Commit**

```powershell
git add src/Shape.LiveIngestion tests/Shape.LiveIngestion.Tests tests/Shape.Seam.Tests
git commit -m "feat(ingestion): add cleanup timer that purges finished sessions and harvests counters"
```

---


### Task 14: End-to-end smoke client, run scripts, findings log and README

**Files:**
- Create: `src/Shape.SmokeClient/{Shape.SmokeClient.csproj,Program.cs,SmokeConfig.cs,LiveFeed.cs,Scenarios.cs}`
- Create: `scripts/run-local.ps1`, `scripts/smoke.ps1`
- Create: `docs/findings.md`, `README.md`

**Interfaces:**
- Consumes (over the wire only — the smoke client references no other project): backend REST (Tasks 10–12), negotiator + `liveTelemetryUpdate` (Task 9), simulator `/sim/*` and fault flags (Task 4), the SQLite file and the `livesessions` table (Tasks 8, 10, 13).
- Produces: `dotnet run --project src/Shape.SmokeClient -- <scenario>...` with scenarios `basic`, `events`, `extend`, `errors`, `stop`, `alarm-at-start`, `faults`, `unreachable`; exit code 0 only when every scenario passes. `scripts/smoke.ps1` runs them in four simulator configurations. `scripts/run-local.ps1 [-Infra]`.

- [ ] **Step 1: Create the project**

```powershell
dotnet new console -n Shape.SmokeClient -o src/Shape.SmokeClient -f net8.0
Remove-Item src/Shape.SmokeClient/Program.cs
dotnet sln add src/Shape.SmokeClient
dotnet add src/Shape.SmokeClient package Microsoft.AspNetCore.SignalR.Client --version 8.0.11
dotnet add src/Shape.SmokeClient package Microsoft.Data.Sqlite --version 8.0.11
dotnet add src/Shape.SmokeClient package Azure.Data.Tables --version 12.12.0
```

- [ ] **Step 2: Write the client**

`src/Shape.SmokeClient/SmokeConfig.cs`:
```csharp
namespace Shape.SmokeClient;

/// <summary>
/// Local endpoints. DeviceGuid is configured, not discovered: no live-session response returns it (finding F1),
/// so the frontend team will hit the same wall.
/// </summary>
public sealed record SmokeConfig(
    string BackendUrl, string NegotiatorUrl, string SimulatorUrl, string Serial, string DeviceGuid,
    string TableConnectionString, string DbPath, int CleanupWaitSeconds)
{
    public static SmokeConfig Default() => new(
        BackendUrl: "http://localhost:5080",
        NegotiatorUrl: "http://localhost:7071",
        SimulatorUrl: "http://localhost:5100",
        Serial: "PUMP-00417",
        DeviceGuid: "9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284",
        TableConnectionString: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;",
        DbPath: Path.Combine(FindRepoRoot(), "data", "live-polling.db"),
        CleanupWaitSeconds: 45);   // two runs of the 15-second local cleanup schedule, plus slack

    private static string FindRepoRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "ShapeLive.sln"))) return dir.FullName;
        throw new InvalidOperationException("ShapeLive.sln not found.");
    }
}

public sealed class SmokeFailure(string message) : Exception(message);
```

`src/Shape.SmokeClient/LiveFeed.cs`:
```csharp
using System.IO.Compression;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR.Client;

namespace Shape.SmokeClient;

/// <summary>negotiate → connect → join-device, then every liveTelemetryUpdate decoded and contract-checked.</summary>
public sealed class LiveFeed : IAsyncDisposable
{
    public const string UserId = "smoke";
    private static readonly string[] EntryKeys = ["timestampUtc", "deviceStatus", "snapshot", "changes", "events"];

    private readonly HubConnection _connection;
    private readonly Channel<JsonElement> _incoming = Channel.CreateUnbounded<JsonElement>();

    private LiveFeed(HubConnection connection) => _connection = connection;

    public List<string> ContractViolations { get; } = [];

    public static async Task<LiveFeed> ConnectAsync(string negotiatorUrl, string deviceGuid)
    {
        using var http = new HttpClient();
        using var negotiate = new HttpRequestMessage(HttpMethod.Post, $"{negotiatorUrl}/api/negotiate");
        negotiate.Headers.Add("x-user-id", UserId);
        var info = JsonDocument.Parse(await (await http.SendAsync(negotiate)).EnsureSuccessStatusCode().Content.ReadAsStringAsync()).RootElement;
        var token = info.GetProperty("accessToken").GetString();

        var connection = new HubConnectionBuilder()
            .WithUrl(info.GetProperty("url").GetString()!, o => o.AccessTokenProvider = () => Task.FromResult(token))
            .WithAutomaticReconnect()
            .Build();
        var feed = new LiveFeed(connection);
        connection.On<JsonElement>("liveTelemetryUpdate", feed.OnEnvelope);
        await connection.StartAsync();

        using var join = new HttpRequestMessage(HttpMethod.Post, $"{negotiatorUrl}/api/join-device?deviceId={deviceGuid}");
        join.Headers.Add("x-user-id", UserId);
        (await http.SendAsync(join)).EnsureSuccessStatusCode();
        return feed;
    }

    /// <summary>Discards anything already received, so a step only sees batches that follow it.</summary>
    public void Drain()
    {
        while (_incoming.Reader.TryRead(out _)) { }
    }

    public async Task<List<JsonElement>> NextBatchesAsync(int count, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var batches = new List<JsonElement>();
        try
        {
            while (batches.Count < count) batches.Add(await _incoming.Reader.ReadAsync(cts.Token));
        }
        catch (OperationCanceledException)
        {
            throw new SmokeFailure($"received {batches.Count} of {count} SignalR batches within {timeout.TotalSeconds:0} s");
        }
        return batches;
    }

    public async Task<JsonElement> WaitForEntryAsync(Func<JsonElement, bool> predicate, TimeSpan timeout, string what)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            while (true)
            {
                var batch = await _incoming.Reader.ReadAsync(cts.Token);
                foreach (var entry in batch.GetProperty("entries").EnumerateArray())
                    if (predicate(entry)) return entry;
            }
        }
        catch (OperationCanceledException)
        {
            throw new SmokeFailure($"no SignalR entry with {what} within {timeout.TotalSeconds:0} s");
        }
    }

    public static JsonElement Decode(JsonElement envelope)
    {
        if (envelope.GetProperty("encoding").GetString() != "gzip+base64")
            throw new SmokeFailure($"unexpected envelope encoding {envelope.GetProperty("encoding")}");
        using var gz = new GZipStream(new MemoryStream(Convert.FromBase64String(envelope.GetProperty("payload").GetString()!)),
            CompressionMode.Decompress);
        return JsonDocument.Parse(gz).RootElement.Clone();
    }

    private void OnEnvelope(JsonElement envelope)
    {
        var batch = Decode(envelope);
        Check(batch);
        _incoming.Writer.TryWrite(batch);
    }

    /// <summary>The frontend contract's per-entry rules (integration doc, "Decoded batch").</summary>
    private void Check(JsonElement batch)
    {
        DateTimeOffset? previous = null;
        var i = 0;
        foreach (var e in batch.GetProperty("entries").EnumerateArray())
        {
            var keys = e.EnumerateObject().Select(p => p.Name).ToArray();
            foreach (var missing in EntryKeys.Except(keys)) ContractViolations.Add($"entry {i}: missing key '{missing}'");
            var hasSnapshot = e.TryGetProperty("snapshot", out var s) && s.ValueKind == JsonValueKind.Object;
            var hasChanges = e.TryGetProperty("changes", out var c) && c.ValueKind == JsonValueKind.Object;
            if (hasSnapshot == hasChanges) ContractViolations.Add($"entry {i}: exactly one of snapshot/changes must be non-null");
            if (i == 0 && !hasSnapshot) ContractViolations.Add("entry 0 has no snapshot");
            if (!e.TryGetProperty("deviceStatus", out var st) || st.ValueKind != JsonValueKind.Number || !st.TryGetInt32(out var sv) || sv is < 0 or > 2)
                ContractViolations.Add($"entry {i}: deviceStatus is not 0/1/2");
            var ts = e.GetProperty("timestampUtc").GetDateTimeOffset();
            if (previous is not null && ts <= previous) ContractViolations.Add($"entry {i}: timestamps not ascending");
            previous = ts;
            i++;
        }
    }

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}
```

`src/Shape.SmokeClient/Scenarios.cs`:
```csharp
using System.Net.Http.Json;
using System.Text.Json;
using Azure.Data.Tables;
using Microsoft.Data.Sqlite;

namespace Shape.SmokeClient;

/// <summary>Spec §10 end-to-end list. Each scenario throws SmokeFailure with the first broken expectation.</summary>
public sealed class Scenarios : IAsyncDisposable
{
    public static readonly string[] DefaultOrder = ["basic", "events", "extend", "errors", "stop"];

    private readonly SmokeConfig _c;
    private readonly LiveFeed _feed;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(60) };

    private Scenarios(SmokeConfig c, LiveFeed feed)
    {
        _c = c;
        _feed = feed;
    }

    public static async Task<Scenarios> CreateAsync(SmokeConfig c) => new(c, await LiveFeed.ConnectAsync(c.NegotiatorUrl, c.DeviceGuid));

    private string Base => $"{_c.BackendUrl}/api/v1/devices/{_c.Serial}/live-session";

    public Task RunAsync(string name) => name switch
    {
        "basic" => Basic(),
        "events" => Events(),
        "extend" => Extend(),
        "errors" => Errors(),
        "stop" => Stop(),
        "alarm-at-start" => AlarmAtStart(),
        "faults" => Faults(),
        "unreachable" => Unreachable(),
        _ => throw new ArgumentException($"Unknown scenario '{name}'."),
    };

    // 1. Start → 3 batches → GET snapshot + tickCount ≥ 15 → /series grid matches ticks.
    private async Task Basic()
    {
        var sessionId = await EnsureSessionAsync();
        var batches = await _feed.NextBatchesAsync(3, TimeSpan.FromSeconds(45));
        Check(batches.All(b => b.GetProperty("sessionId").GetString() == sessionId), "a SignalR batch carried another sessionId");
        Check(batches.All(b => b.GetProperty("deviceGuid").GetString() == _c.DeviceGuid), "a SignalR batch carried another deviceGuid");

        var status = Data(await Expect(HttpMethod.Get, Base, 200));
        Check(status.GetProperty("snapshot").ValueKind == JsonValueKind.Object, "GET snapshot is null after 3 batches");
        Check(status.GetProperty("deviceStatus").ValueKind == JsonValueKind.Number, "GET deviceStatus is not a number");
        Check(status.GetProperty("tickCount").GetInt64() >= 15, $"tickCount {status.GetProperty("tickCount")} < 15");

        var series = Data(await Expect(HttpMethod.Get, Base + "/series", 200));
        var t = series.GetProperty("t").GetArrayLength();
        var ticks = series.GetProperty("tickCount").GetInt64();
        Check(t == ticks || t == ticks - 1, $"grid length {t} vs tickCount {ticks} (F4 allows one short)");
        Check(series.GetProperty("status").GetArrayLength() == t, "status[] length differs from t[]");
        foreach (var s in series.GetProperty("series").EnumerateArray())
            Check(s.GetProperty("v").GetArrayLength() == t, $"{s.GetProperty("metric")} length differs from t[]");
        Check(_feed.ContractViolations.Count == 0, "contract: " + string.Join("; ", _feed.ContractViolations));
    }

    // 2. Alarm 5 → triggered with label on SignalR and /series, status 2; alarm 0 → cleared, status 0.
    private async Task Events()
    {
        await EnsureSessionAsync();
        _feed.Drain();

        await SimAsync("/sim/events", new { OperatingAlarmActive = 5 });
        var on = await _feed.WaitForEntryAsync(e => HasEvent(e, "OperatingAlarmActive", "triggered"), TimeSpan.FromSeconds(30), "an alarm trigger");
        var trigger = EventOf(on, "OperatingAlarmActive");
        Check(trigger.GetProperty("label").GetString() == "Emergency Stop Active", $"label was {trigger.GetProperty("label")}");
        Check(trigger.GetProperty("value").GetInt64() == 5, "value was not 5");
        Check(on.GetProperty("deviceStatus").GetInt32() == 2, $"deviceStatus {on.GetProperty("deviceStatus")} on the trigger tick");
        Check(SeriesEvents(Data(await Expect(HttpMethod.Get, Base + "/series", 200)))
            .Any(e => Is(e, "OperatingAlarmActive", "triggered") && e.GetProperty("label").GetString() == "Emergency Stop Active"),
            "/series has no labelled trigger");

        await SimAsync("/sim/events", new { OperatingAlarmActive = 0 });
        var off = await _feed.WaitForEntryAsync(e => HasEvent(e, "OperatingAlarmActive", "cleared"), TimeSpan.FromSeconds(30), "an alarm clear");
        Check(EventOf(off, "OperatingAlarmActive").GetProperty("label").ValueKind == JsonValueKind.Null, "label on cleared is not null");
        Check(off.GetProperty("deviceStatus").GetInt32() == 0, $"deviceStatus {off.GetProperty("deviceStatus")} after clear");
        Check(SeriesEvents(Data(await Expect(HttpMethod.Get, Base + "/series", 200))).Any(e => Is(e, "OperatingAlarmActive", "cleared")),
            "/series has no clear");
    }

    // 3. Extend → +20; again → 30 and canExtend false; again → 400 EXTENSION_LIMIT_REACHED.
    private async Task Extend()
    {
        await EnsureSessionAsync();
        var first = Data(await Expect(HttpMethod.Post, Base + "/extend", 200));
        var started = first.GetProperty("startedUtc").GetDateTimeOffset();
        Check(first.GetProperty("expiresUtc").GetDateTimeOffset() == started.AddMinutes(20), "first extend: expiresUtc ≠ startedUtc + 20 min");
        Check(first.GetProperty("canExtend").GetBoolean(), "first extend: canExtend false");

        var second = Data(await Expect(HttpMethod.Post, Base + "/extend", 200));
        Check(second.GetProperty("totalDurationMinutes").GetInt32() == 30, "second extend: total ≠ 30");
        Check(!second.GetProperty("canExtend").GetBoolean(), "second extend: canExtend still true");

        var third = await Expect(HttpMethod.Post, Base + "/extend", 400, "LIVE_SESSION_EXTENSION_LIMIT_REACHED");
        Check(third.GetProperty("errors").GetArrayLength() >= 2, "third extend: no module reason in errors[1]");
    }

    // 7a. Second start while active → 409; unknown device → 404; bad metric → 400.
    private async Task Errors()
    {
        await EnsureSessionAsync();
        await Expect(HttpMethod.Post, Base, 409, "LIVE_SESSION_ALREADY_ACTIVE");
        await Expect(HttpMethod.Get, $"{_c.BackendUrl}/api/v1/devices/NO-SUCH-PUMP/live-session", 404, "DEVICE_NOT_FOUND");
        await Expect(HttpMethod.Get, Base + "/series?metric=OperatingPressurePSI,StrokeRateLeft", 400, "LIVE_SESSION_INVALID_PARAMETERS");
    }

    // 4. Stop → /series 409 → within two timer runs the partition is gone and the row has PurgedUtc + counters.
    private async Task Stop()
    {
        var sessionId = await EnsureSessionAsync();
        var stopped = await Expect(HttpMethod.Delete, Base, 200);
        Check(stopped.GetProperty("message").GetString() == "Live session stopped.", "stop message");
        await Expect(HttpMethod.Get, Base + "/series", 409, "LIVE_SESSION_NOT_ACTIVE");
        Check(!Data(await Expect(HttpMethod.Get, Base, 200)).GetProperty("active").GetBoolean(), "GET still active after stop");

        var deadline = DateTime.UtcNow.AddSeconds(_c.CleanupWaitSeconds);
        (string? Purged, long? Ticks, long? Bytes) row;
        while (true)
        {
            row = ReadRow(sessionId);
            if (row.Purged is not null || DateTime.UtcNow > deadline) break;
            await Task.Delay(2000);
        }
        Check(row.Purged is not null, $"PurgedUtc still null after {_c.CleanupWaitSeconds} s");
        Check(row.Ticks > 0 && row.Bytes > 0, $"counters not harvested (TickCount {row.Ticks}, BytesTotal {row.Bytes})");
        await Task.Delay(5000);   // a late partial-batch flush must not resurrect the partition (finding F6 if it does)
        Check(await PartitionRowsAsync(sessionId) == 0, "the partition still has rows after purge");
    }

    // 5. --alarm-at-start → status[0] = 2 and one active OperatingAlarmActive event.
    private async Task AlarmAtStart()
    {
        _feed.Drain();
        await EnsureSessionAsync();
        await _feed.NextBatchesAsync(1, TimeSpan.FromSeconds(30));

        var series = Data(await Expect(HttpMethod.Get, Base + "/series", 200));
        Check(series.GetProperty("status")[0].GetInt32() == 2, $"status[0] = {series.GetProperty("status")[0]}");
        var active = SeriesEvents(series).Where(e => Is(e, "OperatingAlarmActive", "active")).ToList();
        Check(active.Count == 1, $"{active.Count} active OperatingAlarmActive events");
        Check(active[0].GetProperty("label").GetString() == "Emergency Stop Active", "active event label");
        await Expect(HttpMethod.Delete, Base, 200);
    }

    // 6. --duplicate --reorder → no duplicate entries or events; grid evenly spaced.
    private async Task Faults()
    {
        _feed.Drain();
        await EnsureSessionAsync();
        var batches = await _feed.NextBatchesAsync(4, TimeSpan.FromSeconds(60));

        var stamps = batches.SelectMany(b => b.GetProperty("entries").EnumerateArray())
            .Select(e => e.GetProperty("timestampUtc").GetDateTimeOffset()).ToList();
        Check(stamps.Zip(stamps.Skip(1)).All(p => p.First < p.Second), "broadcast entries repeat or go backwards");

        var series = Data(await Expect(HttpMethod.Get, Base + "/series", 200));
        var t = series.GetProperty("t").EnumerateArray().Select(x => x.GetDateTimeOffset()).ToList();
        var interval = TimeSpan.FromSeconds(series.GetProperty("pollIntervalSeconds").GetInt32());
        Check(t.Zip(t.Skip(1)).All(p => p.Second - p.First == interval), "grid is not evenly spaced");
        var events = SeriesEvents(series).Select(e => (e.GetProperty("timestampUtc").GetString(), e.GetProperty("parameter").GetString())).ToList();
        Check(events.Count == events.Distinct().Count(), "duplicate event rows");
        Console.Write($"[grid {t.Count}, tickCount {series.GetProperty("tickCount")}] ");   // reordered batches are skipped: see F7
        await Expect(HttpMethod.Delete, Base, 200);
    }

    // 7b. --unreachable → 503 GATEWAY_UNREACHABLE and nothing recorded.
    private async Task Unreachable()
    {
        await Expect(HttpMethod.Post, Base, 503, "GATEWAY_UNREACHABLE");
        Check(!Data(await Expect(HttpMethod.Get, Base, 200)).GetProperty("active").GetBoolean(), "a session was recorded");
    }

    // ---- helpers ----

    private async Task<string> EnsureSessionAsync()
    {
        var status = Data(await Expect(HttpMethod.Get, Base, 200));
        if (status.GetProperty("active").GetBoolean()) return status.GetProperty("sessionId").GetString()!;
        var started = Data(await Expect(HttpMethod.Post, Base, 200, body: new { pollIntervalSeconds = 1, sessionDurationMinutes = 10 }));
        return started.GetProperty("sessionId").GetString()!;
    }

    private async Task<JsonElement> Expect(HttpMethod method, string url, int status, string? code = null, object? body = null)
    {
        using var request = new HttpRequestMessage(method, url);
        if (body is not null) request.Content = JsonContent.Create(body);
        using var response = await _http.SendAsync(request);
        var text = await response.Content.ReadAsStringAsync();
        Check((int)response.StatusCode == status, $"{method} {url} → {(int)response.StatusCode}, expected {status}: {text}");
        var json = JsonDocument.Parse(text).RootElement.Clone();
        if (code is not null)
            Check(json.GetProperty("errors")[0].GetString() == code, $"{method} {url} → errors[0] {json.GetProperty("errors")[0]}, expected {code}");
        return json;
    }

    private async Task SimAsync(string path, object body) =>
        (await _http.PostAsJsonAsync(_c.SimulatorUrl + path, body)).EnsureSuccessStatusCode();

    private (string? Purged, long? Ticks, long? Bytes) ReadRow(string sessionId)
    {
        using var c = new SqliteConnection($"Data Source={_c.DbPath};Mode=ReadOnly");
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT PurgedUtc, TickCount, BytesTotal FROM LivePollingSession WHERE SessionId = $id";
        cmd.Parameters.AddWithValue("$id", sessionId);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) throw new SmokeFailure($"no SQLite row for session {sessionId}");
        return (r.IsDBNull(0) ? null : r.GetString(0), r.IsDBNull(1) ? null : r.GetInt64(1), r.IsDBNull(2) ? null : r.GetInt64(2));
    }

    private async Task<int> PartitionRowsAsync(string sessionId)
    {
        var table = new TableServiceClient(_c.TableConnectionString).GetTableClient("livesessions");
        var n = 0;
        await foreach (var _ in table.QueryAsync<TableEntity>(TableClient.CreateQueryFilter($"PartitionKey eq {sessionId}"), select: ["RowKey"]))
            n++;
        return n;
    }

    private static JsonElement Data(JsonElement body) => body.GetProperty("data");

    private static IEnumerable<JsonElement> SeriesEvents(JsonElement series) => series.GetProperty("events").EnumerateArray();

    private static bool Is(JsonElement e, string parameter, string state) =>
        e.GetProperty("parameter").GetString() == parameter && e.GetProperty("state").GetString() == state;

    private static bool HasEvent(JsonElement entry, string parameter, string state) =>
        entry.GetProperty("events") is { ValueKind: JsonValueKind.Array } events && events.EnumerateArray().Any(e => Is(e, parameter, state));

    private static JsonElement EventOf(JsonElement entry, string parameter) =>
        entry.GetProperty("events").EnumerateArray().First(e => e.GetProperty("parameter").GetString() == parameter);

    private static void Check(bool ok, string message)
    {
        if (!ok) throw new SmokeFailure(message);
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _feed.DisposeAsync();
    }
}
```

`src/Shape.SmokeClient/Program.cs`:
```csharp
using System.Diagnostics;
using Shape.SmokeClient;

var names = args.Length > 0 ? args : Scenarios.DefaultOrder;
await using var smoke = await Scenarios.CreateAsync(SmokeConfig.Default());

var failed = 0;
foreach (var name in names)
{
    Console.Write($"{name,-16}");
    var clock = Stopwatch.StartNew();
    try
    {
        await smoke.RunAsync(name);
        Console.WriteLine($"PASS  {clock.Elapsed.TotalSeconds,5:0.0} s");
    }
    catch (Exception ex)
    {
        failed++;
        Console.WriteLine($"FAIL  {clock.Elapsed.TotalSeconds,5:0.0} s{Environment.NewLine}    {ex.Message}");
    }
}
return failed == 0 ? 0 : 1;
```

Run: `dotnet build src/Shape.SmokeClient`
Expected: `Build succeeded.` with 0 warnings.

- [ ] **Step 3: Write the scripts**

`scripts/run-local.ps1`:
```powershell
<#
.SYNOPSIS  Starts the local stack, each process in its own window.
.PARAMETER Infra  Start only Azurite, the SignalR emulator and the Functions host (smoke.ps1 starts the rest itself).
#>
param([switch]$Infra)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

# Build once, up front: parallel builds from several windows fight over obj/ (CS2012 "being used by another process").
Write-Host 'Building solution...'
dotnet build (Join-Path $root 'ShapeLive.sln') -v q -nologo
if ($LASTEXITCODE -ne 0) { throw 'Build failed; stop any running simulator/backend/func/test processes and retry.' }

function Start-Window([string]$Title, [string]$Dir, [string]$Command) {
    # -NoProfile: a broken user profile must not break the stack.
    Start-Process pwsh -WorkingDirectory $Dir -ArgumentList '-NoProfile', '-NoExit', '-Command', "`$Host.UI.RawUI.WindowTitle = '$Title'; $Command"
}

Start-Window 'azurite' $root 'azurite --silent --location .azurite --blobPort 10100 --queuePort 10101 --tablePort 10102'
Start-Window 'signalr-emulator' $root 'asrs-emulator start'
Start-Window 'functions' (Join-Path $root 'src/Shape.LiveIngestion') 'func start'

if (-not $Infra) {
    Start-Window 'backend' $root 'dotnet run --no-build --project src/Shape.LiveBackend'
    Start-Window 'simulator' $root 'dotnet run --no-build --project src/Shape.Gateway.Simulator -- --seed 42'
    Start-Window 'web' (Join-Path $root 'web') 'npm run dev'
    Write-Host 'Validation UI: http://localhost:5173'
}
```

`scripts/smoke.ps1`:
```powershell
<#
.SYNOPSIS  End-to-end smoke run (spec §10): REST + negotiator + SignalR against the running stack.
.PARAMETER SkipInfra  Azurite, the SignalR emulator and the Functions host are already running.
#>
param([switch]$SkipInfra)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

if ($SkipInfra) {
    dotnet build (Join-Path $root 'ShapeLive.sln') -v q -nologo
    if ($LASTEXITCODE -ne 0) { throw 'Build failed.' }
} else {
    & (Join-Path $PSScriptRoot 'run-local.ps1') -Infra
}

function Wait-Port([int]$Port, [int]$Seconds = 90, [switch]$Free) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try { $open = $client.ConnectAsync('127.0.0.1', $Port).Wait(500) } catch { $open = $false } finally { $client.Dispose() }
        if ($open -ne [bool]$Free) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "Port $Port did not become $(if ($Free) { 'free' } else { 'ready' }) within $Seconds s."
}

# Start the built DLLs directly: killing `dotnet run` leaves its child process holding the port.
function Start-App([string]$Project, [string[]]$AppArgs) {
    $bin = Join-Path $root "src/$Project/bin/Debug/net8.0"
    Start-Process dotnet -WorkingDirectory $bin -ArgumentList (@("$Project.dll") + $AppArgs) -PassThru -WindowStyle Minimized
}

Wait-Port 10102; Wait-Port 8888; Wait-Port 7071
$backend = Start-App 'Shape.LiveBackend' @()
Wait-Port 5080

$groups = @(
    @{ Flags = @('--seed', '42');                              Scenarios = @('basic', 'events', 'extend', 'errors', 'stop') },
    @{ Flags = @('--seed', '42', '--alarm-at-start');          Scenarios = @('alarm-at-start') },
    @{ Flags = @('--seed', '42', '--duplicate', '--reorder');  Scenarios = @('faults') },
    @{ Flags = @('--seed', '42', '--unreachable');             Scenarios = @('unreachable') }
)

$failed = 0
try {
    foreach ($g in $groups) {
        Write-Host "`n== simulator $($g.Flags -join ' ')"
        $sim = Start-App 'Shape.Gateway.Simulator' $g.Flags
        try {
            Wait-Port 5100
            dotnet run --no-build --project (Join-Path $root 'src/Shape.SmokeClient') -- @($g.Scenarios)
            if ($LASTEXITCODE -ne 0) { $failed++ }
        } finally {
            Stop-Process -Id $sim.Id -Force
            Wait-Port 5100 -Free
        }
    }
} finally {
    Stop-Process -Id $backend.Id -Force
}

if ($failed -gt 0) { Write-Host "`n$failed group(s) failed" -ForegroundColor Red; exit 1 }
Write-Host "`nAll smoke scenarios passed" -ForegroundColor Green
```

- [ ] **Step 4: Run the smoke suite**

Run: `pwsh -NoProfile -File scripts/smoke.ps1`
Expected (≈ 3–4 minutes):
```
== simulator --seed 42
basic           PASS
events          PASS
extend          PASS
errors          PASS
stop            PASS
== simulator --seed 42 --alarm-at-start
alarm-at-start  PASS
== simulator --seed 42 --duplicate --reorder
faults          [grid N, tickCount M] PASS
== simulator --seed 42 --unreachable
unreachable     PASS
All smoke scenarios passed
```
A failure is either a harness bug (fix it in the owning task's code, with a unit test that reproduces it) or a contract/design gap — then record it in `docs/findings.md` with the failing output as evidence. Note the `faults` grid/tickCount pair for F7. Close the infra windows afterwards.

- [ ] **Step 5: Write `docs/findings.md`**

```markdown
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
  = **108** entities. A later 5-delta batch is 41. The 09-30 design measured 87 with 55 fields (13 of headroom);
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
  `[grid N, tickCount M]` with N > M: the skipped ticks exist in the grid only as carried-forward values.
- **Impact:** an alarm transition inside a reordered batch never reaches the store or SignalR; the status column
  shows it late, from the next snapshot, with no event.
- **Proposal:** accept and document, or let an older batch back-fill change points and events (not the head) the way
  an out-of-order baseline already does.

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
```

If Task 9's `Real_seed_batch_stays_inside_the_message_budget` was skipped, append:
```markdown
## F8 — Broadcast exceeds the message budget with the 73-field seed

- **Evidence:** `BroadcastEncoderTests.Real_seed_batch_stays_inside_the_message_budget` (skipped): decoded batch
  <decoded bytes from the skip reason> bytes (budget 4,096), envelope <envelope bytes> bytes (budget 2,048).
- **Impact:** each message bills more than the design's single 2 KB chunk.
- **Proposal:** reconcile the field count (F2), or accept the cost and update the design's estimate.
```
filling in the two byte counts recorded in that test's `Skip` text.

- [ ] **Step 6: Write `README.md`**

````markdown
# Shape live polling — validation harness

Local stand-ins for the live polling gateway module, ingestion function and backend API, plus a validation UI,
used to validate the backend design before its contract goes to the frontend team.
Design: `docs/superpowers/specs/2026-10-01-live-polling-validation-harness-design.md` · Findings: `docs/findings.md`
· Reference contracts: `docs/reference/`.

## Prerequisites

- .NET SDK 8.0.425 (`global.json`), PowerShell 7
- Node.js 20+ (`web/`, Azurite): `npm i -g azurite`
- Azure Functions Core Tools 4.x (4.9 tested)
- Azure SignalR emulator: `dotnet tool install -g Microsoft.Azure.SignalR.Emulator`

## Run

```powershell
pwsh -NoProfile -File scripts/run-local.ps1      # everything, one window each; UI at http://localhost:5173
pwsh -NoProfile -File scripts/smoke.ps1          # end-to-end scenarios (starts its own simulator/backend)
```

| Process | Port |
|---|---|
| Gateway simulator (IoT Hub facade, `/sim/*`) | 5100 |
| Backend API | 5080 |
| Functions host (processor, negotiator, cleanup) | 7071 |
| SignalR emulator | 8888 |
| Azurite blob/queue/table | 10100/10101/10102 |
| Validation UI | 5173 |

Simulator flags: `--seed N`, `--alarm-at-start`, `--drop-every N`, `--reorder`, `--duplicate`, `--unreachable`.

## Test

```powershell
dotnet test --filter "Category!=Integration"     # unit tests, no infrastructure
dotnet test                                       # everything; Azurite must be running on 10100–10102
cd web; npm test                                  # frontend unit tests
```
````

- [ ] **Step 7: Commit**

```powershell
git add src/Shape.SmokeClient scripts docs/findings.md README.md ShapeLive.sln
git commit -m "feat: add end-to-end smoke client, run scripts, findings log and readme"
```

---


### Task 15: Web foundation — contract types, envelope decode, contract checks, frames, pacing, series seed, event glyphs

**Files:**
- Create: `web/{package.json,tsconfig.json,vite.config.ts,index.html,.env.development}`
- Create: `web/src/{config.ts,vite-env.d.ts,main.tsx}`
- Create: `web/src/contract/{types.ts,decode.ts,inspect.ts}`
- Create: `web/src/live/{frames.ts,FrameQueue.ts,seriesSeed.ts,events.ts,units.ts}`
- Test: `web/src/contract/{decode.test.ts,inspect.test.ts}`, `web/src/live/{frames.test.ts,FrameQueue.test.ts,seriesSeed.test.ts,events.test.ts,units.test.ts}`, `web/src/test/fixtures.ts`

**Interfaces:**
- Consumes: the wire contract only (REST envelope and DTOs of Tasks 10–12, the SignalR batch of Task 9) — restated as TypeScript types below.
- Produces (all exported):
  - `contract/types.ts`: `FieldValue`, `FieldMap`, `LiveEvent`, `SeriesEvent`, `LiveEntry`, `LiveBatch`, `LiveEnvelope`, `ApiResponse<T>`, `LiveSessionDto`, `LiveSessionStatus`, `SeriesResponse`, `EVENT_STATES`, `EVENT_TYPES`, `DOMAINS`, `SEVERITIES`
  - `contract/decode.ts`: `decodeEnvelope(envelope: LiveEnvelope): Promise<DecodedBatch>`; `DecodedBatch { batch: LiveBatch; envelopeBytes: number; decodedBytes: number }`
  - `contract/inspect.ts`: `inspectBatch(batch: LiveBatch): string[]` (violations; empty = clean)
  - `live/frames.ts`: `Frame { t: number; values: FieldMap; status: number; gapBefore: boolean }`, `TimedEvent { t: number; event: LiveEvent }`, `Timeline { frames: Frame[]; events: TimedEvent[]; lastT: number | null; sessionStartT: number | null }`, `emptyTimeline()`, `batchToFrames(batch): Frame[]`, `batchEvents(batch): TimedEvent[]`, `appendFrames(tl, frames, events, intervalMs)`, `appendBatch(tl, batch, intervalMs)`, `GAP_FACTOR = 1.5`
  - `live/FrameQueue.ts`: `FrameQueue(maxBacklog = 10, drainTo = 5)` with `push(items)`, `tick(): T[]`, `size`, `clear()` (generic over the queued item)
  - `live/seriesSeed.ts`: `timelineFromSeries(series: SeriesResponse): Timeline`
  - `live/events.ts`: `MarkerKind`, `markerKind(e): MarkerKind | null`, `eventText(e): string`, `MARKER_STYLE: Record<MarkerKind, { glyph: string; color: string; label: string }>`
  - `live/units.ts`: `unitFor(field): string`, `axisFor(field): 'psi' | 'temp' | 'rate' | null`, `formatValue(field, value): string`
  - `config.ts`: `config { backendUrl, negotiatorUrl, simulatorUrl, deviceSerial, deviceGuid }`

- [ ] **Step 1: Scaffold the app**

`web/package.json`:
```json
{
  "name": "shape-live-validation",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "test": "vitest run",
    "test:watch": "vitest"
  },
  "dependencies": {
    "@microsoft/signalr": "^8.0.7",
    "react": "^18.3.1",
    "react-dom": "^18.3.1",
    "recharts": "^2.13.3"
  },
  "devDependencies": {
    "@types/node": "^20.17.0",
    "@types/react": "^18.3.12",
    "@types/react-dom": "^18.3.1",
    "@vitejs/plugin-react": "^4.3.3",
    "typescript": "~5.6.3",
    "vite": "^5.4.10",
    "vitest": "^2.1.4"
  }
}
```

`web/tsconfig.json`:
```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true,
    "skipLibCheck": true,
    "isolatedModules": true,
    "noEmit": true,
    "types": ["vite/client", "node"]
  },
  "include": ["src", "vite.config.ts"]
}
```

`web/vite.config.ts`:
```ts
/// <reference types="vitest" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, strictPort: true },
  test: { environment: 'node', include: ['src/**/*.test.ts'] },
});
```

`web/.env.development`:
```
VITE_BACKEND_URL=http://localhost:5080
VITE_NEGOTIATOR_URL=http://localhost:7071
VITE_SIMULATOR_URL=http://localhost:5100
VITE_DEVICE_SERIAL=PUMP-00417
# Not obtainable from the live-session API (docs/findings.md F1).
VITE_DEVICE_GUID=9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284
```

`web/index.html`:
```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Live Mode — validation</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

`web/src/vite-env.d.ts`:
```ts
/// <reference types="vite/client" />
```

`web/src/config.ts`:
```ts
const env = import.meta.env;

export const config = {
  backendUrl: env.VITE_BACKEND_URL ?? 'http://localhost:5080',
  negotiatorUrl: env.VITE_NEGOTIATOR_URL ?? 'http://localhost:7071',
  simulatorUrl: env.VITE_SIMULATOR_URL ?? 'http://localhost:5100',
  deviceSerial: env.VITE_DEVICE_SERIAL ?? 'PUMP-00417',
  /** Configured, because no live-session response carries it (findings F1). */
  deviceGuid: env.VITE_DEVICE_GUID ?? '9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284',
};
```

`web/src/main.tsx` (placeholder until Task 16 adds `App`):
```tsx
import { createRoot } from 'react-dom/client';

createRoot(document.getElementById('root')!).render(<p>Live Mode validation UI — components arrive in Task 16.</p>);
```

```powershell
cd web; npm install; cd ..
```
Expected: `added N packages`, no `ERR!`. `web/node_modules/` is already git-ignored.

- [ ] **Step 2: Write the contract types**

`web/src/contract/types.ts`:
```ts
/** Wire contract — docs/reference/live-polling-frontend-integration.md. */

export type FieldValue = boolean | number | string | null;
export type FieldMap = Record<string, FieldValue>;

export const EVENT_STATES = ['triggered', 'cleared', 'active'] as const;
export const EVENT_TYPES = ['alarm', 'warning', 'maintenance', 'status', 'unknown'] as const;
export const DOMAINS = ['operational', 'maintenance', 'unknown'] as const;
export const SEVERITIES = ['critical', 'major', 'minor', 'unknown'] as const;

export interface LiveEvent {
  parameter: string;
  value: number;
  state: string;
  eventType: string;
  domain: string;
  severity: string;
  label: string | null;
}

export interface SeriesEvent extends LiveEvent {
  timestampUtc: string;
}

export interface LiveEntry {
  timestampUtc: string;
  deviceStatus: number;
  snapshot: FieldMap | null;
  changes: FieldMap | null;
  events: LiveEvent[] | null;
}

export interface LiveBatch {
  sessionId: string;
  deviceGuid: string;
  batchSentUtc: string;
  entries: LiveEntry[];
}

export interface LiveEnvelope {
  encoding: string;
  payload: string;
}

export interface ApiResponse<T> {
  succeeded: boolean;
  message: string | null;
  errors?: string[];
  data: T | null;
}

export interface LiveSessionDto {
  sessionId: string;
  deviceId: string;
  pollIntervalSeconds: number;
  sessionDurationMinutes: number;
  startedUtc: string;
  expiresUtc: string;
  totalDurationMinutes: number;
  canExtend: boolean;
}

export interface LiveSessionStatus {
  active: boolean;
  sessionId: string | null;
  pollIntervalSeconds: number | null;
  expiresUtc: string | null;
  secondsRemaining: number | null;
  totalDurationMinutes: number | null;
  canExtend: boolean | null;
  deviceStatus: number | null;
  snapshot: FieldMap | null;
  snapshotTimestampUtc: string | null;
  tickCount: number | null;
  bytesTotal: number | null;
}

export interface SeriesResponse {
  sessionId: string;
  pollIntervalSeconds: number;
  startedUtc: string;
  lastTickUtc: string | null;
  tickCount: number;
  t: string[];
  status: number[];
  series: { metric: string; v: FieldValue[] }[];
  events: SeriesEvent[];
}
```

- [ ] **Step 3: Write failing tests**

`web/src/test/fixtures.ts`:
```ts
import { gzipSync } from 'node:zlib';
import type { LiveBatch, LiveEntry, LiveEnvelope, LiveEvent, SeriesResponse } from '../contract/types';

export const T0 = Date.parse('2026-10-01T10:00:00Z');
/** The backend's 7-digit form, e.g. 2026-10-01T10:00:01.0000000Z. */
export const iso = (ms: number) => new Date(ms).toISOString().replace(/Z$/, '0000Z');
export const at = (seconds: number) => iso(T0 + seconds * 1000);

export const ev = (parameter: string, value: number, state: string, extra: Partial<LiveEvent> = {}): LiveEvent => ({
  parameter, value, state, eventType: 'alarm', domain: 'operational', severity: 'critical', label: null, ...extra,
});

export const entry = (seconds: number, part: Partial<LiveEntry>): LiveEntry => ({
  timestampUtc: at(seconds), deviceStatus: 0, snapshot: null, changes: null, events: null, ...part,
});

/** Five ticks from `from` seconds: snapshot first, then the given changes (default heartbeats). */
export const batch = (from: number, snapshot: Record<string, number | boolean>, changes: Record<string, number>[] = []): LiveBatch => ({
  sessionId: 'cc27db6b-508b-488e-a816-faae684491d8',
  deviceGuid: '9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284',
  batchSentUtc: at(from + 5),
  entries: [0, 1, 2, 3, 4].map(i =>
    i === 0 ? entry(from, { snapshot }) : entry(from + i, { changes: changes[i - 1] ?? {} })),
});

export const envelope = (b: unknown): LiveEnvelope => ({
  encoding: 'gzip+base64',
  payload: gzipSync(Buffer.from(JSON.stringify(b))).toString('base64'),
});

export const series = (from: number, count: number, metric: string, values: number[]): SeriesResponse => ({
  sessionId: 'cc27db6b-508b-488e-a816-faae684491d8',
  pollIntervalSeconds: 1,
  startedUtc: at(from - 1),
  lastTickUtc: at(from + count - 1),
  tickCount: count,
  t: Array.from({ length: count }, (_, k) => at(from + k)),
  status: Array.from({ length: count }, () => 0),
  series: [{ metric, v: values }],
  events: [],
});
```

`web/src/contract/decode.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { decodeEnvelope } from './decode';
import { batch, envelope } from '../test/fixtures';

describe('decodeEnvelope', () => {
  it('inflates a gzip+base64 envelope into the batch and reports both sizes', async () => {
    const b = batch(0, { P: 1 });
    const env = envelope(b);

    const decoded = await decodeEnvelope(env);

    expect(decoded.batch).toEqual(b);
    expect(decoded.decodedBytes).toBe(new TextEncoder().encode(JSON.stringify(b)).length);
    expect(decoded.envelopeBytes).toBe(JSON.stringify(env).length);
  });

  it('rejects any other encoding', async () => {
    await expect(decodeEnvelope({ encoding: 'json', payload: '{}' })).rejects.toThrow('Unsupported envelope encoding: json');
  });
});
```

`web/src/contract/inspect.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { inspectBatch } from './inspect';
import { batch, ev } from '../test/fixtures';
import type { LiveBatch } from './types';

const clone = (b: LiveBatch): LiveBatch => JSON.parse(JSON.stringify(b));

describe('inspectBatch', () => {
  it('passes a well-formed batch', () => {
    expect(inspectBatch(batch(0, { P: 1 }, [{ P: 2 }]))).toEqual([]);
  });

  it('flags a missing key', () => {
    const b = clone(batch(0, { P: 1 }));
    delete (b.entries[1] as Partial<typeof b.entries[1]>).events;
    expect(inspectBatch(b)).toContain("entries[1]: missing key 'events'");
  });

  it('flags both or neither of snapshot and changes, and entry 0 without a snapshot', () => {
    const b = clone(batch(0, { P: 1 }));
    b.entries[1].snapshot = { P: 1 };
    b.entries[2].changes = null;
    b.entries[0] = { ...b.entries[0], snapshot: null, changes: {} };
    const v = inspectBatch(b);
    expect(v).toContain('entries[1]: both snapshot and changes are non-null');
    expect(v).toContain('entries[2]: neither snapshot nor changes is non-null');
    expect(v).toContain('entries[0]: snapshot is null');
  });

  it('flags a non-integer or out-of-range deviceStatus', () => {
    const b = clone(batch(0, { P: 1 }));
    (b.entries[1] as { deviceStatus: unknown }).deviceStatus = 'Warning';
    b.entries[2].deviceStatus = 1.5;
    b.entries[3].deviceStatus = 3;
    const v = inspectBatch(b);
    expect(v).toEqual(expect.arrayContaining([
      'entries[1]: deviceStatus "Warning" is not 0, 1 or 2',
      'entries[2]: deviceStatus 1.5 is not 0, 1 or 2',
      'entries[3]: deviceStatus 3 is not 0, 1 or 2',
    ]));
  });

  it('flags unknown enum values but accepts the documented "unknown"', () => {
    const b = clone(batch(0, { P: 1 }));
    b.entries[1].events = [ev('X', 1, 'raised'), ev('Y', 1, 'triggered', { eventType: 'unknown', domain: 'unknown', severity: 'unknown' })];
    expect(inspectBatch(b)).toEqual(['entries[1].events[0]: state "raised" is not a documented value']);
  });

  it('flags entries that are not ascending', () => {
    const b = clone(batch(0, { P: 1 }));
    b.entries[3].timestampUtc = b.entries[2].timestampUtc;
    expect(inspectBatch(b)).toContain('entries[3]: timestampUtc is not after entries[2]');
  });
});
```

`web/src/live/frames.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { appendBatch, batchToFrames, emptyTimeline } from './frames';
import { T0, batch, ev } from '../test/fixtures';

describe('batchToFrames', () => {
  it('starts from the snapshot and applies changes in order, heartbeats keeping values', () => {
    const frames = batchToFrames(batch(0, { P: 1, Q: 9 }, [{ P: 2 }, {}, { Q: 8 }, {}]));

    expect(frames.map(f => f.t)).toEqual([0, 1, 2, 3, 4].map(s => T0 + s * 1000));
    expect(frames.map(f => [f.values.P, f.values.Q])).toEqual([[1, 9], [2, 9], [2, 9], [2, 8], [2, 8]]);
  });
});

describe('appendBatch', () => {
  it('appends every frame of the first batch and records the session start', () => {
    const tl = appendBatch(emptyTimeline(), batch(0, { P: 1 }), 1000);
    expect(tl.frames).toHaveLength(5);
    expect(tl.lastT).toBe(T0 + 4000);
    expect(tl.sessionStartT).toBe(T0);
  });

  it('drops frames not newer than the last one shown (redelivery and overlap)', () => {
    let tl = appendBatch(emptyTimeline(), batch(0, { P: 1 }), 1000);
    tl = appendBatch(tl, batch(0, { P: 1 }), 1000);          // full redelivery
    expect(tl.frames).toHaveLength(5);

    tl = appendBatch(tl, batch(3, { P: 7 }), 1000);          // overlaps t3, t4
    expect(tl.frames.map(f => (f.t - T0) / 1000)).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(tl.frames[5].values.P).toBe(7);
  });

  it('marks a line break when consecutive frames are more than 1.5 intervals apart', () => {
    let tl = appendBatch(emptyTimeline(), batch(0, { P: 1 }), 1000);
    tl = appendBatch(tl, batch(10, { P: 2 }), 1000);          // a dropped batch: 4 → 10
    expect(tl.frames[5].gapBefore).toBe(true);
    expect(tl.frames[6].gapBefore).toBe(false);
    expect(tl.frames[0].gapBefore).toBe(false);
  });

  it('keeps events only for accepted frames, stamped with their entry time', () => {
    const b = batch(0, { P: 1 });
    b.entries[2].events = [ev('OperatingAlarmActive', 5, 'triggered', { label: 'Emergency Stop Active' })];
    let tl = appendBatch(emptyTimeline(), b, 1000);
    tl = appendBatch(tl, b, 1000);

    expect(tl.events).toHaveLength(1);
    expect(tl.events[0].t).toBe(T0 + 2000);
  });
});
```

`web/src/live/FrameQueue.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { FrameQueue } from './FrameQueue';

describe('FrameQueue', () => {
  it('releases one item per tick and nothing when empty', () => {
    const q = new FrameQueue<number>();
    q.push([1, 2, 3]);
    expect([q.tick(), q.tick(), q.tick(), q.tick()]).toEqual([[1], [2], [3], []]);
  });

  it('drains a backlog above 10 down to the newest 5 in one tick', () => {
    const q = new FrameQueue<number>();
    q.push(Array.from({ length: 12 }, (_, i) => i));

    expect(q.tick()).toEqual([0, 1, 2, 3, 4, 5, 6]);
    expect(q.size).toBe(5);
    expect(q.tick()).toEqual([7]);
  });

  it('does not drain at exactly 10', () => {
    const q = new FrameQueue<number>();
    q.push(Array.from({ length: 10 }, (_, i) => i));
    expect(q.tick()).toEqual([0]);
  });
});
```

`web/src/live/seriesSeed.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { timelineFromSeries } from './seriesSeed';
import { appendBatch } from './frames';
import { T0, at, batch, series } from '../test/fixtures';

describe('timelineFromSeries', () => {
  it('turns the grid into frames with values, status and the session start', () => {
    const s = series(1, 3, 'OperatingPressurePSI', [54000, 54100, 54100]);
    s.status = [0, 2, 2];
    s.series.push({ metric: 'StrokeRateLeft', v: [null, 90, 91] });
    s.events = [{ timestampUtc: at(2), parameter: 'OperatingAlarmActive', value: 5, state: 'triggered', eventType: 'alarm',
      domain: 'operational', severity: 'critical', label: 'Emergency Stop Active' }];

    const tl = timelineFromSeries(s);

    expect(tl.frames.map(f => f.status)).toEqual([0, 2, 2]);
    expect(tl.frames[0].values).toEqual({ OperatingPressurePSI: 54000 });     // null = not yet reported → absent
    expect(tl.frames[2].values).toEqual({ OperatingPressurePSI: 54100, StrokeRateLeft: 91 });
    expect(tl.events).toEqual([{ t: T0 + 2000, event: expect.objectContaining({ label: 'Emergency Stop Active' }) }]);
    expect(tl.sessionStartT).toBe(T0 + 1000);
    expect(tl.lastT).toBe(T0 + 3000);
  });

  it('is empty before the first tick', () => {
    const tl = timelineFromSeries(series(1, 0, 'P', []));
    expect(tl.frames).toEqual([]);
    expect(tl.lastT).toBeNull();
    expect(tl.sessionStartT).toBeNull();
  });

  it('merges a series seed with overlapping live frames without duplicates', () => {
    const seeded = timelineFromSeries(series(0, 5, 'P', [1, 2, 3, 4, 5]));     // t0..t4
    const merged = appendBatch(seeded, batch(3, { P: 40 }, [{ P: 50 }, { P: 60 }]), 1000);   // t3..t7

    const seconds = merged.frames.map(f => (f.t - T0) / 1000);
    expect(seconds).toEqual([0, 1, 2, 3, 4, 5, 6, 7]);
    expect(new Set(seconds).size).toBe(seconds.length);
    expect(merged.frames.map(f => f.values.P)).toEqual([1, 2, 3, 4, 5, 60, 60, 60]);
    expect(merged.frames.some(f => f.gapBefore)).toBe(false);
  });
});
```

`web/src/live/events.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { MARKER_STYLE, eventText, markerKind } from './events';
import { ev } from '../test/fixtures';

describe('markerKind', () => {
  it.each([
    [ev('MotorRunning', 1, 'triggered', { eventType: 'status', severity: 'minor' }), 'motor-on'],
    [ev('MotorRunning', 0, 'cleared', { eventType: 'status', severity: 'minor' }), 'motor-off'],
    [ev('OperatingWarningActive', 21, 'triggered', { eventType: 'warning' }), 'warning'],
    [ev('OperatingWarningActive', 0, 'cleared', { eventType: 'warning' }), 'warning-cleared'],
    [ev('OperatingAlarmActive', 6, 'triggered'), 'alarm'],
    [ev('OilFilterAlarm', 0, 'cleared', { domain: 'maintenance' }), 'alarm-cleared'],
    [ev('NewParam', 1, 'triggered', { eventType: 'unknown', domain: 'unknown', severity: 'unknown' }), 'unknown'],
    [ev('Odd', 1, 'triggered', { eventType: 'maintenance' }), 'unknown'],
  ])('maps %o to %s', (e, kind) => {
    expect(markerKind(e)).toBe(kind);
  });

  it('markerKind returns null for active events', () => {
    expect(markerKind(ev('OperatingAlarmActive', 5, 'active', { label: 'Emergency Stop Active' }))).toBeNull();
    expect(markerKind(ev('MotorRunning', 1, 'active', { eventType: 'status' }))).toBeNull();
  });

  it('has a style for every kind', () => {
    expect(Object.keys(MARKER_STYLE).sort()).toEqual(
      ['alarm', 'alarm-cleared', 'motor-off', 'motor-on', 'unknown', 'warning', 'warning-cleared']);
  });
});

describe('eventText', () => {
  it('uses the label when present', () => {
    expect(eventText(ev('OperatingAlarmActive', 6, 'triggered', { label: 'Overstroke Alarm' })))
      .toBe('Operational Alarm Triggered: Overstroke Alarm');
  });

  it('falls back to domain, event type and state when the label is null', () => {
    expect(eventText(ev('MaintenanceAlarmActive', 1, 'triggered', { domain: 'maintenance' }))).toBe('Maintenance Alarm Triggered');
    expect(eventText(ev('OperatingAlarmActive', 0, 'cleared'))).toBe('Operational Alarm Cleared');
  });
});
```

`web/src/live/units.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { axisFor, formatValue, unitFor } from './units';

describe('units', () => {
  it.each([
    ['OperatingPressurePSI', 'psi', 'psi'],
    ['OperatingPressureBar', 'bar', null],
    ['HydraulicOilTemperatureF', '°F', 'temp'],
    ['HydraulicOilTemperatureC', '°C', null],
    ['StrokeRateLeft', 'strokes/min', 'rate'],
    ['StrokeTimeRight', 'ms', null],
    ['OperatingHoursMotor', 'h', null],
    ['MotorRunning', '', null],
  ])('%s → unit %s, axis %s', (field, unit, axis) => {
    expect(unitFor(field)).toBe(unit);
    expect(axisFor(field)).toBe(axis);
  });

  it('formats values with their unit', () => {
    expect(formatValue('OperatingPressurePSI', 54372)).toBe('54,372 psi');
    expect(formatValue('MotorRunning', true)).toBe('true');
    expect(formatValue('X', null)).toBe('—');
  });
});
```

- [ ] **Step 4: Run tests — expect failure**

Run: `cd web; npx vitest run; cd ..`
Expected: FAIL — `Failed to resolve import "./decode"` (and the same for each module under test).

- [ ] **Step 5: Implement**

`web/src/contract/decode.ts`:
```ts
import type { LiveBatch, LiveEnvelope } from './types';

export interface DecodedBatch {
  batch: LiveBatch;
  envelopeBytes: number;
  decodedBytes: number;
}

/** base64 → gzip inflate → UTF-8 → JSON, with DecompressionStream as the integration doc prescribes. */
export async function decodeEnvelope(envelope: LiveEnvelope): Promise<DecodedBatch> {
  if (envelope.encoding !== 'gzip+base64') throw new Error(`Unsupported envelope encoding: ${envelope.encoding}`);
  const compressed = Uint8Array.from(atob(envelope.payload), c => c.charCodeAt(0));
  const stream = new Blob([compressed]).stream().pipeThrough(new DecompressionStream('gzip'));
  const bytes = new Uint8Array(await new Response(stream).arrayBuffer());
  return {
    batch: JSON.parse(new TextDecoder().decode(bytes)) as LiveBatch,
    envelopeBytes: JSON.stringify(envelope).length,
    decodedBytes: bytes.length,
  };
}
```

`web/src/contract/inspect.ts`:
```ts
import { DOMAINS, EVENT_STATES, EVENT_TYPES, SEVERITIES, type LiveBatch } from './types';

const ENTRY_KEYS = ['timestampUtc', 'deviceStatus', 'snapshot', 'changes', 'events'] as const;
type EnumField = 'state' | 'eventType' | 'domain' | 'severity';
const ENUMS: [EnumField, readonly string[]][] = [
  ['state', EVENT_STATES], ['eventType', EVENT_TYPES], ['domain', DOMAINS], ['severity', SEVERITIES],
];

/** Runtime checks of the SignalR contract (spec §8.2 contract inspector). Returns one line per violation. */
export function inspectBatch(batch: LiveBatch): string[] {
  const v: string[] = [];
  let previous: number | null = null;
  batch.entries.forEach((e, i) => {
    const at = `entries[${i}]`;
    for (const key of ENTRY_KEYS) if (!(key in e)) v.push(`${at}: missing key '${key}'`);

    const hasSnapshot = e.snapshot !== null && e.snapshot !== undefined;
    const hasChanges = e.changes !== null && e.changes !== undefined;
    if (hasSnapshot && hasChanges) v.push(`${at}: both snapshot and changes are non-null`);
    if (!hasSnapshot && !hasChanges) v.push(`${at}: neither snapshot nor changes is non-null`);
    if (i === 0 && !hasSnapshot) v.push(`${at}: snapshot is null`);

    const s: unknown = e.deviceStatus;
    if (!(Number.isInteger(s) && (s as number) >= 0 && (s as number) <= 2))
      v.push(`${at}: deviceStatus ${JSON.stringify(s)} is not 0, 1 or 2`);

    (e.events ?? []).forEach((evt, j) => {
      for (const [field, allowed] of ENUMS)
        if (!allowed.includes(evt[field])) v.push(`${at}.events[${j}]: ${field} "${evt[field]}" is not a documented value`);
    });

    const t = Date.parse(e.timestampUtc);
    if (previous !== null && !(t > previous)) v.push(`${at}: timestampUtc is not after entries[${i - 1}]`);
    previous = t;
  });
  return v;
}
```

`web/src/live/frames.ts`:
```ts
import type { FieldMap, LiveBatch, LiveEvent } from '../contract/types';

export const GAP_FACTOR = 1.5;

export interface Frame {
  t: number;
  values: FieldMap;
  status: number;
  /** True when the previous frame is more than GAP_FACTOR intervals earlier: draw a line break. */
  gapBefore: boolean;
}

export interface TimedEvent {
  t: number;
  event: LiveEvent;
}

export interface Timeline {
  frames: Frame[];
  events: TimedEvent[];
  lastT: number | null;
  sessionStartT: number | null;
}

export const emptyTimeline = (): Timeline => ({ frames: [], events: [], lastT: null, sessionStartT: null });

/** entries[0].snapshot is truth; entries 1..n apply changes in order (integration doc, "Decoded batch"). */
export function batchToFrames(batch: LiveBatch): Frame[] {
  let current: FieldMap = {};
  return batch.entries.map(e => {
    current = e.snapshot ? { ...e.snapshot } : { ...current, ...(e.changes ?? {}) };
    return { t: Date.parse(e.timestampUtc), values: current, status: e.deviceStatus, gapBefore: false };
  });
}

/** Every event of the batch, stamped with its entry's timestamp (events carry none of their own on SignalR). */
export function batchEvents(batch: LiveBatch): TimedEvent[] {
  return batch.entries.flatMap(e => (e.events ?? []).map((event: LiveEvent) => ({ t: Date.parse(e.timestampUtc), event })));
}

/** Appends frames newer than the last one shown; older or equal ones (overlap, redelivery) are dropped. */
export function appendFrames(tl: Timeline, frames: Frame[], events: TimedEvent[], intervalMs: number): Timeline {
  const accepted: Frame[] = [];
  let last = tl.lastT;
  for (const f of frames) {
    if (last !== null && f.t <= last) continue;
    accepted.push({ ...f, gapBefore: last !== null && f.t - last > GAP_FACTOR * intervalMs });
    last = f.t;
  }
  if (accepted.length === 0) return tl;
  const acceptedTimes = new Set(accepted.map(f => f.t));
  return {
    frames: [...tl.frames, ...accepted],
    events: [...tl.events, ...events.filter(e => acceptedTimes.has(e.t))],
    lastT: last,
    sessionStartT: tl.sessionStartT ?? accepted[0].t,
  };
}

export function appendBatch(tl: Timeline, batch: LiveBatch, intervalMs: number): Timeline {
  return appendFrames(tl, batchToFrames(batch), batchEvents(batch), intervalMs);
}
```

`web/src/live/FrameQueue.ts`:
```ts
/**
 * Paces playback: one item per tick. A backlog above maxBacklog (a background tab, a burst after reconnect) is
 * released at once down to the newest drainTo, so the chart catches up without replaying minutes of history.
 */
export class FrameQueue<T> {
  private items: T[] = [];

  constructor(private readonly maxBacklog = 10, private readonly drainTo = 5) {}

  get size(): number {
    return this.items.length;
  }

  push(items: T[]): void {
    this.items.push(...items);
  }

  tick(): T[] {
    if (this.items.length > this.maxBacklog) return this.items.splice(0, this.items.length - this.drainTo);
    return this.items.length > 0 ? this.items.splice(0, 1) : [];
  }

  clear(): void {
    this.items = [];
  }
}
```

`web/src/live/seriesSeed.ts`:
```ts
import type { FieldMap, SeriesResponse } from '../contract/types';
import type { Frame, Timeline } from './frames';

/** GET /series → the timeline the live feed continues (spec §8.1 step 1, history option C). */
export function timelineFromSeries(series: SeriesResponse): Timeline {
  const frames: Frame[] = series.t.map((ts, k) => {
    const values: FieldMap = {};
    for (const s of series.series) if (s.v[k] !== null && s.v[k] !== undefined) values[s.metric] = s.v[k];
    return { t: Date.parse(ts), values, status: series.status[k] ?? 0, gapBefore: false };
  });
  return {
    frames,
    events: series.events.map(({ timestampUtc, ...event }) => ({ t: Date.parse(timestampUtc), event })),
    lastT: frames.length > 0 ? frames[frames.length - 1].t : null,
    sessionStartT: frames.length > 0 ? frames[0].t : null,
  };
}
```

`web/src/live/events.ts`:
```ts
import type { LiveEvent } from '../contract/types';

export type MarkerKind = 'motor-on' | 'motor-off' | 'warning' | 'warning-cleared' | 'alarm' | 'alarm-cleared' | 'unknown';

export const MARKER_STYLE: Record<MarkerKind, { glyph: string; color: string; label: string }> = {
  'motor-on': { glyph: '▲', color: '#2e7d32', label: 'Motor On' },
  'motor-off': { glyph: '▼', color: '#616161', label: 'Motor Off' },
  warning: { glyph: '◆', color: '#f9a825', label: 'Warning' },
  'warning-cleared': { glyph: '◇', color: '#f9a825', label: 'Warning Cleared' },
  alarm: { glyph: '●', color: '#c62828', label: 'Alarm' },
  'alarm-cleared': { glyph: '○', color: '#c62828', label: 'Alarm Cleared' },
  unknown: { glyph: '?', color: '#6a1b9a', label: 'Unknown event' },
};

/**
 * `active` describes the condition the session opened in — no transition happened, so no marker (it goes in the
 * "Opened with" banner). Anything without a glyph of its own renders defensively as `unknown`.
 */
export function markerKind(e: LiveEvent): MarkerKind | null {
  if (e.state === 'active') return null;
  const cleared = e.state === 'cleared';
  if (e.parameter === 'MotorRunning') return cleared ? 'motor-off' : 'motor-on';
  if (e.eventType === 'warning') return cleared ? 'warning-cleared' : 'warning';
  if (e.eventType === 'alarm') return cleared ? 'alarm-cleared' : 'alarm';
  return 'unknown';
}

const title = (s: string) => (s.length === 0 ? s : s[0].toUpperCase() + s.slice(1));

/** Label when present, else "{domain} {eventType} {state}" (spec §8.2). */
export function eventText(e: LiveEvent): string {
  const base = `${title(e.domain)} ${title(e.eventType)} ${title(e.state)}`;
  return e.label ? `${base}: ${e.label}` : base;
}
```

`web/src/live/units.ts`:
```ts
import type { FieldValue } from '../contract/types';

const RULES: [RegExp, string, 'psi' | 'temp' | 'rate' | null][] = [
  [/PSI$/, 'psi', 'psi'],
  [/Bar$/, 'bar', null],
  [/TemperatureF$/, '°F', 'temp'],
  [/TemperatureC$/, '°C', null],
  [/^StrokeRate/, 'strokes/min', 'rate'],
  [/^StrokeTime/, 'ms', null],
  [/^OperatingHours/, 'h', null],
];

export const unitFor = (field: string): string => RULES.find(([re]) => re.test(field))?.[1] ?? '';

/** The graph's three Y axes: PSI (left), °F (left), strokes/min (right). Other fields are not charted. */
export const axisFor = (field: string): 'psi' | 'temp' | 'rate' | null => RULES.find(([re]) => re.test(field))?.[2] ?? null;

export function formatValue(field: string, value: FieldValue | undefined): string {
  if (value === null || value === undefined) return '—';
  if (typeof value !== 'number') return String(value);
  const unit = unitFor(field);
  return unit ? `${value.toLocaleString('en-US')} ${unit}` : value.toLocaleString('en-US');
}
```

- [ ] **Step 6: Run tests — expect pass**

Run: `cd web; npx vitest run; npx tsc --noEmit; cd ..`
Expected: `Test Files  7 passed (7)`, `Tests  40 passed (40)`; `tsc` prints nothing.

- [ ] **Step 7: Commit**

```powershell
git add web
git commit -m "feat(web): add contract decode and checks, frame timeline, pacing queue, series seed and event glyphs"
```

---


### Task 16: Web UI — API clients, live session hook, Live Mode components, headless check

**Files:**
- Create: `web/src/api/{backend.ts,simulator.ts}`
- Create: `web/src/live/{connection.ts,useLiveSession.ts,view.ts}`
- Create: `web/src/components/{Header,ErrorBanner,ParameterGraph,StatusStrip,OpenedWithBanner,HoursCards,LiveMetrics,SimPanel,ContractInspector}.tsx`
- Create: `web/src/{App.tsx,index.css}`, `web/scripts/check-ui.mjs`
- Modify: `web/src/main.tsx`, `web/src/config.ts`, `web/package.json`
- Test: `web/src/live/view.test.ts`

**Interfaces:**
- Consumes: everything Task 15 exports (`config`, contract types, `decodeEnvelope`, `inspectBatch`, `Timeline`, `Frame`, `TimedEvent`, `emptyTimeline`, `appendBatch`, `appendFrames`, `FrameQueue`, `timelineFromSeries`, `markerKind`, `eventText`, `MARKER_STYLE`, `unitFor`, `axisFor`, `formatValue`); REST + negotiator + simulator endpoints.
- Produces:
  - `backend.status() / start(body) / extend() / stop() / series(metrics?, since?)` → `Promise<ApiResult<T>>`; `ApiResult<T> = { ok: true; data: T; message: string | null } | ApiFailure`; `ApiFailure { ok: false; status: number; code: string; message: string; errors: string[] }`
  - `simulator.state() / setEvents(values) / clear()`
  - `openLiveConnection(onEnvelope, onState) → Promise<HubConnection>`; `ConnectionState`
  - `useLiveSession() → { status, timeline, inspector, connection, error, intervalMs, start, extend, stop, refresh, dismissError }`
  - `view.ts`: `chartRows(frames, keys)`, `formatDuration(ms)`, `clockTime(ms)`
  - `config.deviceName`

- [ ] **Step 1: Write the failing test**

`web/src/live/view.test.ts`:
```ts
import { describe, expect, it } from 'vitest';
import { chartRows, formatDuration } from './view';
import type { Frame } from './frames';

const f = (t: number, values: Frame['values'], gapBefore = false): Frame => ({ t, values, status: 0, gapBefore });

describe('chartRows', () => {
  it('keeps only charted keys, as numbers or null', () => {
    expect(chartRows([f(1000, { P: 5, Q: 'x', R: true })], ['P', 'Q', 'Missing'])).toEqual([{ t: 1000, P: 5, Q: null, Missing: null }]);
  });

  it('inserts an all-null row before a gap so the lines break', () => {
    const rows = chartRows([f(1000, { P: 1 }), f(9000, { P: 2 }, true)], ['P']);
    expect(rows).toEqual([{ t: 1000, P: 1 }, { t: 8999, P: null }, { t: 9000, P: 2 }]);
  });
});

describe('formatDuration', () => {
  it.each([[65_000, '01:05'], [600_000, '10:00'], [1_800_000, '30:00'], [-5, '00:00'], [59_999, '00:59']])('%d ms → %s', (ms, text) => {
    expect(formatDuration(ms)).toBe(text);
  });
});
```

Run: `cd web; npx vitest run src/live/view.test.ts; cd ..`
Expected: FAIL — `Failed to resolve import "./view"`.

- [ ] **Step 2: Implement the view helpers**

`web/src/live/view.ts`:
```ts
import type { Frame } from './frames';

export type ChartRow = { t: number } & Record<string, number | null>;

/** Frames → Recharts rows. A null row 1 ms before a gap makes every line break there (connectNulls is off). */
export function chartRows(frames: Frame[], keys: string[]): ChartRow[] {
  const rows: ChartRow[] = [];
  for (const frame of frames) {
    if (frame.gapBefore) rows.push(Object.assign({ t: frame.t - 1 }, Object.fromEntries(keys.map(k => [k, null]))));
    const row: ChartRow = { t: frame.t };
    for (const k of keys) {
      const v = frame.values[k];
      row[k] = typeof v === 'number' ? v : null;
    }
    rows.push(row);
  }
  return rows;
}

export function formatDuration(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000));
  return `${String(Math.floor(total / 60)).padStart(2, '0')}:${String(total % 60).padStart(2, '0')}`;
}

export const clockTime = (ms: number) => new Date(ms).toLocaleTimeString('en-GB', { hour12: false });
```

Run: `cd web; npx vitest run src/live/view.test.ts; cd ..`
Expected: `Tests  7 passed (7)`.

- [ ] **Step 3: Write the API clients and the connection**

In `web/src/config.ts` add the property (display only — the API returns no name):
```ts
  deviceName: env.VITE_DEVICE_NAME ?? 'PREC-NEO30-MARYL-07',
```
and append `VITE_DEVICE_NAME=PREC-NEO30-MARYL-07` to `web/.env.development`.

`web/src/api/backend.ts`:
```ts
import { config } from '../config';
import type { ApiResponse, LiveSessionDto, LiveSessionStatus, SeriesResponse } from '../contract/types';

export interface ApiFailure {
  ok: false;
  status: number;
  code: string;
  message: string;
  errors: string[];
}

export type ApiResult<T> = { ok: true; data: T; message: string | null } | ApiFailure;

const base = () => `${config.backendUrl}/api/v1/devices/${encodeURIComponent(config.deviceSerial)}/live-session`;

async function call<T>(method: string, path = '', body?: unknown): Promise<ApiResult<T>> {
  let res: Response;
  try {
    res = await fetch(base() + path, {
      method,
      headers: body === undefined ? undefined : { 'Content-Type': 'application/json' },
      body: body === undefined ? undefined : JSON.stringify(body),
    });
  } catch (e) {
    return { ok: false, status: 0, code: 'NETWORK_ERROR', message: `Backend unreachable: ${(e as Error).message}`, errors: [] };
  }
  const text = await res.text();
  let json: ApiResponse<T> | null = null;
  try {
    json = text ? (JSON.parse(text) as ApiResponse<T>) : null;
  } catch {
    // not an envelope (e.g. a proxy error page) — reported below with the raw text
  }
  if (res.ok && json?.succeeded) return { ok: true, data: json.data as T, message: json.message };
  return {
    ok: false,
    status: res.status,
    code: json?.errors?.[0] ?? `HTTP_${res.status}`,
    message: json?.message ?? (text || res.statusText),
    errors: json?.errors ?? [],
  };
}

export const backend = {
  status: () => call<LiveSessionStatus>('GET'),
  start: (body: { pollIntervalSeconds?: number; sessionDurationMinutes?: number }) => call<LiveSessionDto>('POST', '', body),
  extend: () => call<LiveSessionDto>('POST', '/extend'),
  stop: () => call<null>('DELETE'),
  series: (metrics: string[] = [], since?: string) => {
    const q = new URLSearchParams();
    for (const m of metrics) q.append('metric', m);      // repeated, never comma-separated
    if (since) q.set('since', since);
    const qs = q.toString();
    return call<SeriesResponse>('GET', `/series${qs ? `?${qs}` : ''}`);
  },
};
```

`web/src/api/simulator.ts`:
```ts
import { config } from '../config';

export interface SimState {
  active: boolean;
  sessionId: string | null;
  totalDurationMinutes: number | null;
  pollIntervalSeconds: number | null;
  events: Record<string, number>;
}

async function post(path: string, body?: unknown): Promise<void> {
  const res = await fetch(config.simulatorUrl + path, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  });
  if (!res.ok) throw new Error(`${path} → ${res.status} ${await res.text()}`);
}

export const simulator = {
  state: async (): Promise<SimState> => (await fetch(`${config.simulatorUrl}/sim/state`)).json(),
  setEvents: (values: Record<string, number>) => post('/sim/events', values),
  clear: () => post('/sim/events/clear'),
};
```

`web/src/live/connection.ts`:
```ts
import * as signalR from '@microsoft/signalr';
import { config } from '../config';
import type { LiveEnvelope } from '../contract/types';

export type ConnectionState = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';

/** No auth: the negotiator takes the caller's id from x-user-id (spec §5.3). */
export const USER_ID = 'local';

async function negotiator(path: string): Promise<Response> {
  const res = await fetch(`${config.negotiatorUrl}/api/${path}`, { method: 'POST', headers: { 'x-user-id': USER_ID } });
  if (!res.ok) throw new Error(`${path} → ${res.status}`);
  return res;
}

/** negotiate → connect → join-device(deviceGuid); re-joins after every automatic reconnect (spec §8.1 step 2). */
export async function openLiveConnection(
  onEnvelope: (envelope: LiveEnvelope) => void,
  onState: (state: ConnectionState) => void,
): Promise<signalR.HubConnection> {
  const info = (await (await negotiator('negotiate')).json()) as { url: string; accessToken: string };
  const connection = new signalR.HubConnectionBuilder()
    .withUrl(info.url, { accessTokenFactory: () => info.accessToken })
    .withAutomaticReconnect()
    .configureLogging(signalR.LogLevel.Warning)
    .build();
  const join = () => negotiator(`join-device?deviceId=${encodeURIComponent(config.deviceGuid)}`);

  connection.on('liveTelemetryUpdate', onEnvelope);
  connection.onreconnecting(() => onState('reconnecting'));
  connection.onreconnected(async () => {
    await join();
    onState('connected');
  });
  await connection.start();
  await join();
  onState('connected');
  return connection;
}
```

- [ ] **Step 4: Write the live session hook**

`web/src/live/useLiveSession.ts`:
```ts
import { useCallback, useEffect, useRef, useState } from 'react';
import type { HubConnection } from '@microsoft/signalr';
import { backend, type ApiFailure, type ApiResult } from '../api/backend';
import { decodeEnvelope, type DecodedBatch } from '../contract/decode';
import { inspectBatch } from '../contract/inspect';
import type { LiveEnvelope, LiveSessionStatus } from '../contract/types';
import { openLiveConnection, type ConnectionState } from './connection';
import { FrameQueue } from './FrameQueue';
import { appendBatch, appendFrames, emptyTimeline, type Frame, type TimedEvent, type Timeline } from './frames';
import { timelineFromSeries } from './seriesSeed';

export interface InspectorState {
  last: DecodedBatch | null;
  batches: number;
  violationCount: number;
  violations: string[];
}

export interface UiError {
  code: string;
  message: string;
}

type Queued = { frame: Frame; events: TimedEvent[] };

/**
 * Spec §8.1. `received` is everything accepted from /series and SignalR (dedupe point); the queue paces it into
 * `timeline`, the displayed state, one frame per interval.
 */
export function useLiveSession() {
  const [status, setStatus] = useState<LiveSessionStatus | null>(null);
  const [timeline, setTimeline] = useState<Timeline>(emptyTimeline);
  const [inspector, setInspector] = useState<InspectorState>({ last: null, batches: 0, violationCount: 0, violations: [] });
  const [connection, setConnection] = useState<ConnectionState>('disconnected');
  const [error, setError] = useState<UiError | null>(null);

  const statusRef = useRef<LiveSessionStatus | null>(null);
  const received = useRef<Timeline>(emptyTimeline());
  const queue = useRef(new FrameQueue<Queued>());
  const intervalMs = (status?.pollIntervalSeconds ?? 1) * 1000;
  const intervalRef = useRef(intervalMs);
  intervalRef.current = intervalMs;

  const fail = (r: ApiFailure) => setError({ code: r.code, message: r.message });

  /** GET /live-session; a session we have not seeded yet is seeded from GET /series (defaults). */
  const refresh = useCallback(async () => {
    const r = await backend.status();
    if (!r.ok) return fail(r);
    const isNewSession = r.data.active && r.data.sessionId !== statusRef.current?.sessionId;
    statusRef.current = r.data;
    setStatus(r.data);
    if (!isNewSession) return;
    const series = await backend.series();
    if (!series.ok) return fail(series);
    const seeded = timelineFromSeries(series.data);
    received.current = seeded;
    queue.current.clear();
    setTimeline(seeded);
  }, []);

  const onEnvelope = useCallback(async (envelope: LiveEnvelope) => {
    let decoded: DecodedBatch;
    try {
      decoded = await decodeEnvelope(envelope);
    } catch (e) {
      setInspector(i => ({ ...i, violationCount: i.violationCount + 1, violations: [`decode: ${(e as Error).message}`, ...i.violations].slice(0, 50) }));
      return;
    }
    const found = inspectBatch(decoded.batch).map(v => `${decoded.batch.batchSentUtc} ${v}`);
    setInspector(i => ({
      last: decoded,
      batches: i.batches + 1,
      violationCount: i.violationCount + found.length,
      violations: [...found, ...i.violations].slice(0, 50),
    }));

    const current = statusRef.current;
    if (!current?.active || decoded.batch.sessionId !== current.sessionId) {
      void refresh();   // a session started elsewhere (or one we have not loaded yet): reseed from /series
      return;
    }
    const before = received.current;
    const after = appendBatch(before, decoded.batch, intervalRef.current);
    if (after === before) return;   // redelivery or overlap with the seed
    received.current = after;
    const events = after.events.slice(before.events.length);
    queue.current.push(after.frames.slice(before.frames.length).map(frame => ({ frame, events: events.filter(e => e.t === frame.t) })));
  }, [refresh]);

  const onEnvelopeRef = useRef(onEnvelope);
  onEnvelopeRef.current = onEnvelope;

  const tick = useCallback(() => {
    const items = queue.current.tick();
    if (items.length === 0) return;
    setTimeline(tl => appendFrames(tl, items.map(i => i.frame), items.flatMap(i => i.events), intervalRef.current));
  }, []);

  // Pacing: one frame per interval; a hidden tab's throttled timers are caught up the moment it is visible again.
  useEffect(() => {
    const id = window.setInterval(tick, intervalMs);
    return () => window.clearInterval(id);
  }, [tick, intervalMs]);
  useEffect(() => {
    const onVisible = () => document.visibilityState === 'visible' && tick();
    document.addEventListener('visibilitychange', onVisible);
    return () => document.removeEventListener('visibilitychange', onVisible);
  }, [tick]);

  // Connection: automatic reconnect inside SignalR; when it gives up (onclose), start over (spec §8.1 step 5).
  useEffect(() => {
    let disposed = false;
    let hub: HubConnection | null = null;
    let retry: number | undefined;
    const connect = async () => {
      setConnection('connecting');
      try {
        hub = await openLiveConnection(e => void onEnvelopeRef.current(e), setConnection);
        hub.onclose(() => {
          if (disposed) return;
          setConnection('disconnected');
          retry = window.setTimeout(connect, 2000);
        });
      } catch {
        setConnection('disconnected');
        if (!disposed) retry = window.setTimeout(connect, 5000);
      }
    };
    void connect();
    return () => {
      disposed = true;
      window.clearTimeout(retry);
      void hub?.stop();
    };
  }, []);

  // Initial load, and a slow poll so lazy expiry shows up without a user action.
  useEffect(() => {
    void refresh();
    const id = window.setInterval(() => void refresh(), 30_000);
    return () => window.clearInterval(id);
  }, [refresh]);

  /** After every Start/Extend/Stop the UI re-reads GET /live-session (spec §8.1 step 6). */
  const act = useCallback(async <T,>(call: Promise<ApiResult<T>>) => {
    const r = await call;
    if (r.ok) setError(null);
    else fail(r);
    await refresh();
  }, [refresh]);

  return {
    status,
    timeline,
    inspector,
    connection,
    error,
    intervalMs,
    refresh,
    start: (body: { pollIntervalSeconds?: number; sessionDurationMinutes?: number }) => act(backend.start(body)),
    extend: () => act(backend.extend()),
    stop: () => act(backend.stop()),
    dismissError: () => setError(null),
  };
}
```

- [ ] **Step 5: Write the components**

`web/src/components/ErrorBanner.tsx`:
```tsx
import type { UiError } from '../live/useLiveSession';

export function ErrorBanner({ error, onDismiss }: { error: UiError; onDismiss: () => void }) {
  return (
    <div className="banner banner-error" role="alert">
      <strong>{error.code}</strong> {error.message}
      <button className="link" onClick={onDismiss}>Dismiss</button>
    </div>
  );
}
```

`web/src/components/Header.tsx`:
```tsx
import { useEffect, useState } from 'react';
import { config } from '../config';
import type { LiveSessionStatus } from '../contract/types';
import type { ConnectionState } from '../live/connection';
import { formatDuration } from '../live/view';

interface Props {
  status: LiveSessionStatus | null;
  connection: ConnectionState;
  onStart: (body: { pollIntervalSeconds?: number; sessionDurationMinutes?: number }) => Promise<void>;
  onExtend: () => Promise<void>;
  onStop: () => Promise<void>;
}

function useNow(): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);
  return now;
}

/** Empty input = omit the field, so the server-side default is exercised too. */
const optional = (v: string) => (v.trim() === '' ? undefined : Number(v));

export function Header({ status, connection, onStart, onExtend, onStop }: Props) {
  const now = useNow();
  const [intervalText, setIntervalText] = useState('1');
  const [durationText, setDurationText] = useState('10');
  const [busy, setBusy] = useState(false);
  const run = async (action: () => Promise<void>) => {
    setBusy(true);
    try {
      await action();
    } finally {
      setBusy(false);
    }
  };

  const active = status?.active === true;
  const expires = status?.expiresUtc ? Date.parse(status.expiresUtc) : 0;
  const started = expires - (status?.totalDurationMinutes ?? 0) * 60_000;

  return (
    <header className="header">
      <div>
        <h1>{config.deviceSerial} · {config.deviceName}</h1>
        <span className={`conn conn-${connection}`}>SignalR {connection}</span>
      </div>
      {active ? (
        <div className="controls">
          <span className="live-badge">● Live {formatDuration(now - started)}</span>
          <span>Remaining {formatDuration(expires - now)}</span>
          <span className="muted">{status?.totalDurationMinutes} min total</span>
          <button disabled={busy || !status?.canExtend} onClick={() => run(onExtend)}>+ Extend 10 min</button>
          <button className="danger" disabled={busy} onClick={() => run(onStop)}>Stop Live Mode</button>
        </div>
      ) : (
        <div className="controls">
          <label>Interval (s) <input type="number" value={intervalText} onChange={e => setIntervalText(e.target.value)} /></label>
          <label>Duration (min) <input type="number" value={durationText} onChange={e => setDurationText(e.target.value)} /></label>
          <button className="primary" disabled={busy}
            onClick={() => run(() => onStart({ pollIntervalSeconds: optional(intervalText), sessionDurationMinutes: optional(durationText) }))}>
            Start
          </button>
        </div>
      )}
    </header>
  );
}
```

`web/src/components/StatusStrip.tsx`:
```tsx
import type { Frame } from '../live/frames';
import { clockTime } from '../live/view';

const COLORS = ['#43a047', '#f9a825', '#e53935'];
const NAMES = ['Normal', 'Warning', 'Alarm'];

export function StatusStrip({ frames }: { frames: Frame[] }) {
  return (
    <div className="status-strip" aria-label="Device status per tick">
      {frames.map(f => (
        <div key={f.t} style={{ background: COLORS[f.status] ?? '#9e9e9e' }} title={`${clockTime(f.t)} ${NAMES[f.status] ?? f.status}`} />
      ))}
    </div>
  );
}
```

`web/src/components/OpenedWithBanner.tsx`:
```tsx
import type { TimedEvent } from '../live/frames';
import { eventText } from '../live/events';

/** `active` events: the condition the session opened in. Shown here, never as a transition marker. */
export function OpenedWithBanner({ events }: { events: TimedEvent[] }) {
  const active = events.filter(e => e.event.state === 'active');
  if (active.length === 0) return null;
  return (
    <div className="banner banner-info">
      <strong>Opened with:</strong> {active.map(e => eventText(e.event)).join(' · ')}
    </div>
  );
}
```

`web/src/components/ParameterGraph.tsx`:
```tsx
import { useMemo, useState } from 'react';
import { CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import type { Timeline, TimedEvent } from '../live/frames';
import { MARKER_STYLE, eventText, markerKind } from '../live/events';
import { formatValue } from '../live/units';
import { chartRows, clockTime } from '../live/view';
import { StatusStrip } from './StatusStrip';

const METRICS = [
  { key: 'OperatingPressurePSI', axis: 'psi', color: '#1565c0', label: 'Pressure' },
  { key: 'HydraulicOilTemperatureF', axis: 'temp', color: '#ef6c00', label: 'Oil Temp' },
  { key: 'StrokeRateLeft', axis: 'rate', color: '#2e7d32', label: 'Stroke Rate L' },
  { key: 'StrokeRateRight', axis: 'rate', color: '#6a1b9a', label: 'Stroke Rate R' },
] as const;

interface TooltipProps {
  active?: boolean;
  label?: number;
  payload?: { dataKey?: string | number; value?: number | null; color?: string }[];
  events: TimedEvent[];
  intervalMs: number;
}

function GraphTooltip({ active, label, payload, events, intervalMs }: TooltipProps) {
  if (!active || label === undefined) return null;
  const near = events.filter(e => Math.abs(e.t - label) <= intervalMs / 2);
  return (
    <div className="tooltip">
      <div className="muted">{clockTime(label)}</div>
      {(payload ?? []).map(p => (
        <div key={String(p.dataKey)} style={{ color: p.color }}>{String(p.dataKey)}: {formatValue(String(p.dataKey), p.value ?? null)}</div>
      ))}
      {near.map(e => <div key={`${e.event.parameter}-${e.event.state}`} className="tooltip-event">{eventText(e.event)}</div>)}
    </div>
  );
}

export function ParameterGraph({ timeline, intervalMs }: { timeline: Timeline; intervalMs: number }) {
  const [visible, setVisible] = useState<Set<string>>(() => new Set(METRICS.map(m => m.key)));
  const rows = useMemo(() => chartRows(timeline.frames, METRICS.map(m => m.key)), [timeline.frames]);
  const markers = timeline.events.flatMap(e => {
    const kind = markerKind(e.event);
    return kind ? [{ ...e, kind }] : [];
  });
  const toggle = (key: string) =>
    setVisible(v => {
      const next = new Set(v);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });

  return (
    <section className="card">
      <div className="card-head">
        <h2>Device Parameter Graph</h2>
        <div className="chips">
          {METRICS.map(m => (
            <button key={m.key} className={`chip ${visible.has(m.key) ? 'on' : ''}`} style={{ borderColor: m.color }} onClick={() => toggle(m.key)}>
              {m.label}
            </button>
          ))}
        </div>
      </div>
      {rows.length === 0 ? (
        <p className="muted empty">No ticks yet. Start a session, or wait for the first batch (about 5 s).</p>
      ) : (
        <ResponsiveContainer width="100%" height={340}>
          <ComposedChart data={rows} margin={{ top: 24, right: 8, left: 8, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']} tickFormatter={clockTime} />
            <YAxis yAxisId="psi" orientation="left" domain={['auto', 'auto']} label={{ value: 'PSI', angle: -90, position: 'insideLeft' }} />
            <YAxis yAxisId="temp" orientation="left" domain={['auto', 'auto']} label={{ value: '°F', angle: -90, position: 'insideLeft' }} />
            <YAxis yAxisId="rate" orientation="right" domain={['auto', 'auto']} label={{ value: 'strokes/min', angle: 90, position: 'insideRight' }} />
            <Tooltip content={<GraphTooltip events={timeline.events} intervalMs={intervalMs} />} />
            {timeline.sessionStartT !== null && (
              <ReferenceLine x={timeline.sessionStartT} yAxisId="psi" stroke="#757575" strokeDasharray="4 2"
                label={{ value: 'Session start', position: 'insideTopLeft', fontSize: 11 }} />
            )}
            {markers.map(m => (
              <ReferenceLine key={`${m.t}-${m.event.parameter}-${m.event.state}`} x={m.t} yAxisId="psi"
                stroke={MARKER_STYLE[m.kind].color} strokeOpacity={0.6}
                label={{ value: MARKER_STYLE[m.kind].glyph, position: 'top', fill: MARKER_STYLE[m.kind].color }} />
            ))}
            {METRICS.filter(m => visible.has(m.key)).map(m => (
              <Line key={m.key} yAxisId={m.axis} dataKey={m.key} stroke={m.color} dot={false} isAnimationActive={false} connectNulls={false} />
            ))}
          </ComposedChart>
        </ResponsiveContainer>
      )}
      <StatusStrip frames={timeline.frames} />
      <div className="legend">
        {Object.values(MARKER_STYLE).map(s => <span key={s.label} style={{ color: s.color }}>{s.glyph} {s.label}</span>)}
      </div>
    </section>
  );
}
```

`web/src/components/HoursCards.tsx`:
```tsx
import type { FieldMap } from '../contract/types';
import { formatValue } from '../live/units';

const CARDS = [
  { key: 'OperatingHoursMotor', title: 'Motor Operating Hours' },
  { key: 'OperatingHoursTW1', title: 'Intensifier Operating Hours' },
];

export function HoursCards({ latest }: { latest: FieldMap }) {
  return (
    <div className="hours">
      {CARDS.map(c => (
        <section key={c.key} className="card hours-card">
          <div className="muted">{c.title}</div>
          <div className="big">{formatValue(c.key, latest[c.key] ?? null)}</div>
        </section>
      ))}
    </div>
  );
}
```

`web/src/components/LiveMetrics.tsx`:
```tsx
import { useEffect, useMemo, useState } from 'react';
import { backend } from '../api/backend';
import type { FieldMap, FieldValue, SeriesResponse } from '../contract/types';
import type { Timeline } from '../live/frames';
import { formatValue, unitFor } from '../live/units';
import { clockTime } from '../live/view';

const STATUS = ['Normal', 'Warning', 'Alarm'];

interface Props {
  latest: FieldMap;
  timeline: Timeline;
  sessionId: string | null;
}

/** Every snapshot field; selecting one loads GET /series?metric=X and continues it from the live frames. */
export function LiveMetrics({ latest, timeline, sessionId }: Props) {
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState<string | null>(null);
  const [seed, setSeed] = useState<SeriesResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setSeed(null);
    setError(null);
    if (!selected || !sessionId) return;
    void backend.series([selected]).then(r => (r.ok ? setSeed(r.data) : setError(`${r.code}: ${r.message}`)));
  }, [selected, sessionId]);

  const fields = Object.keys(latest).filter(k => k.toLowerCase().includes(query.toLowerCase())).sort();

  const rows = useMemo(() => {
    if (!selected || !seed) return [];
    const seeded: { t: number; v: FieldValue; status: number }[] =
      seed.t.map((t, k) => ({ t: Date.parse(t), v: seed.series[0]?.v[k] ?? null, status: seed.status[k] ?? 0 }));
    const last = seeded.length > 0 ? seeded[seeded.length - 1].t : -Infinity;
    const live = timeline.frames.filter(f => f.t > last && selected in f.values).map(f => ({ t: f.t, v: f.values[selected], status: f.status }));
    return [...seeded, ...live].reverse().slice(0, 60);
  }, [selected, seed, timeline.frames]);

  return (
    <section className="card">
      <div className="card-head">
        <h2>Live Metrics</h2>
        <input placeholder="Search fields…" value={query} onChange={e => setQuery(e.target.value)} />
      </div>
      <div className="metrics">
        <ul className="field-list">
          {fields.map(k => (
            <li key={k} className={k === selected ? 'selected' : ''} onClick={() => setSelected(k)}>
              <span>{k}</span>
              <span className="muted">{formatValue(k, latest[k])}</span>
            </li>
          ))}
        </ul>
        <div className="metric-table">
          {selected === null ? (
            <p className="muted">Select a field to load its series.</p>
          ) : error ? (
            <p className="error">{error}</p>
          ) : (
            <table>
              <thead><tr><th>Time</th><th>{selected} {unitFor(selected) && `(${unitFor(selected)})`}</th><th>Status</th></tr></thead>
              <tbody>
                {rows.map(r => (
                  <tr key={r.t}><td>{clockTime(r.t)}</td><td>{formatValue(selected, r.v)}</td><td>{STATUS[r.status] ?? r.status}</td></tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </section>
  );
}
```

`web/src/components/SimPanel.tsx`:
```tsx
import { useEffect, useState } from 'react';
import { simulator, type SimState } from '../api/simulator';

const PRESETS: { label: string; values: Record<string, number> }[] = [
  { label: 'Emergency Stop (alarm 5)', values: { OperatingAlarmActive: 5 } },
  { label: 'Overstroke Alarm (6)', values: { OperatingAlarmActive: 6 } },
  { label: 'Clear operating alarm', values: { OperatingAlarmActive: 0 } },
  { label: 'Left Overstroke Warning (21)', values: { OperatingWarningActive: 21 } },
  { label: 'Clear operating warning', values: { OperatingWarningActive: 0 } },
  { label: 'Maintenance alarm', values: { MaintenanceAlarmActive: 1 } },
  { label: 'Oil filter warning', values: { OilFilterWarning: 1 } },
  { label: 'Motor off', values: { MotorRunning: 0 } },
  { label: 'Motor on', values: { MotorRunning: 1 } },
];

/** Dev panel: drives the simulator's event state (spec §4.5). */
export function SimPanel() {
  const [state, setState] = useState<SimState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const reload = () => simulator.state().then(setState, e => setError(String(e)));
  const send = (action: () => Promise<void>) => action().then(() => (setError(null), reload()), e => setError(String(e)));

  useEffect(() => {
    void reload();
    const id = window.setInterval(() => void reload(), 2000);
    return () => window.clearInterval(id);
  }, []);

  const nonZero = Object.entries(state?.events ?? {}).filter(([, v]) => v !== 0);
  return (
    <section className="card">
      <h2>Simulator</h2>
      <div className="muted">
        {state?.active ? `Session ${state.sessionId?.slice(0, 8)} · ${state.totalDurationMinutes} min · ${state.pollIntervalSeconds} s` : 'No module session'}
      </div>
      <div className="preset-grid">
        {PRESETS.map(p => <button key={p.label} onClick={() => send(() => simulator.setEvents(p.values))}>{p.label}</button>)}
        <button className="danger" onClick={() => send(simulator.clear)}>Clear all</button>
      </div>
      <div className="muted">Non-zero: {nonZero.length === 0 ? 'none' : nonZero.map(([k, v]) => `${k}=${v}`).join(', ')}</div>
      {error && <p className="error">{error}</p>}
    </section>
  );
}
```

`web/src/components/ContractInspector.tsx`:
```tsx
import type { InspectorState } from '../live/useLiveSession';

/** Dev panel: the last decoded batch, its sizes, and every contract violation seen (spec §8.2). */
export function ContractInspector({ inspector }: { inspector: InspectorState }) {
  const last = inspector.last;
  return (
    <section className="card">
      <h2>Contract inspector</h2>
      <dl className="facts">
        <dt>Batches</dt><dd>{inspector.batches}</dd>
        <dt>Violations</dt><dd data-testid="violation-count" className={inspector.violationCount ? 'error' : ''}>{inspector.violationCount}</dd>
        <dt>Envelope</dt><dd>{last ? `${last.envelopeBytes.toLocaleString()} B` : '—'}</dd>
        <dt>Decoded</dt><dd>{last ? `${last.decodedBytes.toLocaleString()} B` : '—'}</dd>
        <dt>Entries</dt><dd>{last ? last.batch.entries.length : '—'}</dd>
      </dl>
      {inspector.violations.length > 0 && (
        <ul className="violations">{inspector.violations.map((v, i) => <li key={i}>{v}</li>)}</ul>
      )}
      {last && (
        <details>
          <summary>Last decoded batch</summary>
          <pre>{JSON.stringify(last.batch, null, 2)}</pre>
        </details>
      )}
    </section>
  );
}
```

`web/src/App.tsx`:
```tsx
import { useMemo } from 'react';
import type { FieldMap } from './contract/types';
import { ContractInspector } from './components/ContractInspector';
import { ErrorBanner } from './components/ErrorBanner';
import { Header } from './components/Header';
import { HoursCards } from './components/HoursCards';
import { LiveMetrics } from './components/LiveMetrics';
import { OpenedWithBanner } from './components/OpenedWithBanner';
import { ParameterGraph } from './components/ParameterGraph';
import { SimPanel } from './components/SimPanel';
import { useLiveSession } from './live/useLiveSession';

export function App() {
  const live = useLiveSession();
  const lastFrame = live.timeline.frames[live.timeline.frames.length - 1];
  // The /series seed carries only the charted metrics; GET's snapshot fills the rest until live frames arrive.
  const latest: FieldMap = useMemo(() => ({ ...(live.status?.snapshot ?? {}), ...(lastFrame?.values ?? {}) }), [live.status, lastFrame]);

  return (
    <div className="page">
      <Header status={live.status} connection={live.connection} onStart={live.start} onExtend={live.extend} onStop={live.stop} />
      {live.error && <ErrorBanner error={live.error} onDismiss={live.dismissError} />}
      <OpenedWithBanner events={live.timeline.events} />
      <main className="layout">
        <div className="main-col">
          <ParameterGraph timeline={live.timeline} intervalMs={live.intervalMs} />
          <LiveMetrics latest={latest} timeline={live.timeline} sessionId={live.status?.active ? live.status.sessionId : null} />
        </div>
        <aside className="side-col">
          <HoursCards latest={latest} />
          <SimPanel />
          <ContractInspector inspector={live.inspector} />
        </aside>
      </main>
    </div>
  );
}
```

`web/src/main.tsx` (replace; no `StrictMode` — its double effects would open two SignalR connections):
```tsx
import { createRoot } from 'react-dom/client';
import { App } from './App';
import './index.css';

createRoot(document.getElementById('root')!).render(<App />);
```

`web/src/index.css`:
```css
:root {
  --bg: #f4f6f8;
  --card: #ffffff;
  --text: #1f2933;
  --muted: #6b7785;
  --border: #dde3ea;
  --primary: #1565c0;
  --danger: #c62828;
  --ok: #2e7d32;
  font-family: system-ui, 'Segoe UI', Roboto, sans-serif;
  color: var(--text);
  background: var(--bg);
}
* { box-sizing: border-box; }
body { margin: 0; }
h1 { font-size: 1.25rem; margin: 0; }
h2 { font-size: 1rem; margin: 0 0 8px; }
button { font: inherit; padding: 6px 12px; border: 1px solid var(--border); border-radius: 6px; background: var(--card); cursor: pointer; }
button:disabled { opacity: 0.5; cursor: default; }
button.primary { background: var(--primary); border-color: var(--primary); color: #fff; }
button.danger { color: var(--danger); border-color: var(--danger); }
button.link { border: none; background: none; text-decoration: underline; margin-left: 12px; }
input { font: inherit; padding: 5px 8px; border: 1px solid var(--border); border-radius: 6px; width: 7em; }
.page { max-width: 1440px; margin: 0 auto; padding: 16px; }
.header { display: flex; justify-content: space-between; align-items: center; gap: 16px; flex-wrap: wrap; background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 12px 16px; }
.controls { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
.live-badge { color: var(--danger); font-weight: 600; }
.conn { font-size: 0.8rem; color: var(--muted); }
.conn-connected { color: var(--ok); }
.muted { color: var(--muted); font-size: 0.85rem; }
.error { color: var(--danger); }
.banner { margin-top: 12px; padding: 10px 14px; border-radius: 8px; }
.banner-error { background: #fdecea; border: 1px solid #f5c2c0; }
.banner-info { background: #fff8e1; border: 1px solid #ffe082; }
.layout { display: grid; grid-template-columns: minmax(0, 1fr) 360px; gap: 16px; margin-top: 16px; }
@media (max-width: 1100px) { .layout { grid-template-columns: 1fr; } }
.main-col, .side-col { display: flex; flex-direction: column; gap: 16px; min-width: 0; }
.card { background: var(--card); border: 1px solid var(--border); border-radius: 10px; padding: 14px 16px; }
.card-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; margin-bottom: 8px; }
.chips { display: flex; gap: 6px; flex-wrap: wrap; }
.chip { border-width: 2px; border-radius: 999px; padding: 3px 10px; opacity: 0.45; }
.chip.on { opacity: 1; }
.empty { padding: 40px 0; text-align: center; }
.status-strip { display: flex; height: 10px; margin: 6px 0 0 60px; border-radius: 3px; overflow: hidden; }
.status-strip > div { flex: 1; min-width: 1px; }
.legend { display: flex; gap: 14px; flex-wrap: wrap; font-size: 0.8rem; margin-top: 8px; }
.tooltip { background: var(--card); border: 1px solid var(--border); border-radius: 6px; padding: 8px 10px; font-size: 0.85rem; }
.tooltip-event { font-weight: 600; margin-top: 4px; }
.hours { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.hours-card .big { font-size: 1.4rem; font-weight: 600; margin-top: 4px; }
.metrics { display: grid; grid-template-columns: 300px minmax(0, 1fr); gap: 12px; }
.field-list { list-style: none; margin: 0; padding: 0; max-height: 360px; overflow: auto; border: 1px solid var(--border); border-radius: 6px; }
.field-list li { display: flex; justify-content: space-between; gap: 8px; padding: 5px 8px; cursor: pointer; font-size: 0.85rem; }
.field-list li:hover, .field-list li.selected { background: #e3f2fd; }
.metric-table { max-height: 360px; overflow: auto; }
table { width: 100%; border-collapse: collapse; font-size: 0.85rem; }
th, td { text-align: left; padding: 4px 6px; border-bottom: 1px solid var(--border); }
.preset-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 6px; margin: 10px 0; }
.preset-grid button { font-size: 0.8rem; }
.facts { display: grid; grid-template-columns: auto 1fr; gap: 4px 12px; margin: 0 0 8px; font-size: 0.85rem; }
.facts dt { color: var(--muted); }
.facts dd { margin: 0; }
.violations { max-height: 160px; overflow: auto; font-size: 0.75rem; color: var(--danger); padding-left: 18px; }
pre { max-height: 300px; overflow: auto; font-size: 0.75rem; background: #f7f9fb; padding: 8px; border-radius: 6px; }
```

- [ ] **Step 6: Type-check, test and build**

Run: `cd web; npx tsc --noEmit; npx vitest run; npm run build; cd ..`
Expected: `tsc` silent; `Test Files  8 passed (8)`, `Tests  47 passed (47)`; `vite build` ends with `✓ built in`.

- [ ] **Step 7: Headless check in Edge**

```powershell
cd web; npm install -D playwright-core@^1.48.0; cd ..
```

`web/scripts/check-ui.mjs`:
```js
// Headless Edge against the running stack (scripts/run-local.ps1). Usage: node scripts/check-ui.mjs [screenshot.png]
import { chromium } from 'playwright-core';

const browser = await chromium.launch({ channel: 'msedge', headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1100 } });
const consoleErrors = [];
page.on('console', m => m.type() === 'error' && consoleErrors.push(m.text()));

await page.goto('http://localhost:5173');
const start = page.getByRole('button', { name: 'Start' });
if (await start.isVisible({ timeout: 5000 }).catch(() => false)) await start.click();
await page.getByText('● Live').waitFor({ timeout: 15000 });
await page.waitForTimeout(16000);    // three batches

const lines = await page.locator('.recharts-line path').count();
const violations = (await page.getByTestId('violation-count').textContent())?.trim();
const batches = (await page.locator('.facts dd').first().textContent())?.trim();
await page.screenshot({ path: process.argv[2] ?? 'live-mode.png', fullPage: true });
await browser.close();

console.log({ lines, batches, violations, consoleErrors });
if (lines === 0 || violations !== '0' || Number(batches) < 2 || consoleErrors.length > 0) process.exit(1);
```

Add to `web/package.json` `scripts`: `"check-ui": "node scripts/check-ui.mjs"`.

With the full stack running (`pwsh -NoProfile -File scripts/run-local.ps1`):
```powershell
cd web; npm run check-ui -- ../.superpowers/live-mode.png; cd ..
```
Expected: `{ lines: 4, batches: '3', violations: '0', consoleErrors: [] }` (batch count may be 2–4), exit code 0. Open the screenshot and compare against the Live Mode mock: header with serial/name, live timer, remaining, Extend/Stop; graph with four lines on three axes, session-start line, status strip; hours cards; metrics list; dev panels.

Then by hand in the browser at `http://localhost:5173`, check each and fix what fails:
1. **Simulator → Emergency Stop**: within ~6 s a red `●` marker, red status strip, tooltip "Operational Alarm Triggered: Emergency Stop Active". **Clear operating alarm**: `○` marker, strip back to green, tooltip "Operational Alarm Cleared".
2. **Reload the page mid-session**: the chart reappears from `/series` with the session-start line and continues without a duplicated or missing second (Review Focus 1).
3. **Hide the tab for ≥ 1 minute, return**: the chart catches up at once, not frame-by-frame over a minute.
4. **Extend** twice → button disabled at 30 min; the third attempt is impossible from the UI. **Stop** → header back to Start; an error-free banner area.
5. Restart the simulator with `--alarm-at-start`, Start → "Opened with: Operational Alarm Active: Emergency Stop Active" banner and a red strip from the first tick, but **no** marker for it (Review Focus 2).
6. Stop the simulator, Start → banner `GATEWAY_UNREACHABLE The gateway did not respond.` (after ~15 s with `--unreachable`, at once with the simulator down).
7. Select `OperatingHoursMotor` in Live Metrics → the table loads from `/series?metric=OperatingHoursMotor` and gains a row per tick.

- [ ] **Step 8: Commit**

```powershell
git add web
git commit -m "feat(web): add live mode validation ui with paced chart, event markers and contract inspector"
```

