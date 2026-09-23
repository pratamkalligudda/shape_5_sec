# 5-Second Batched Fast-Polling Telemetry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fully local simulation where a C# gateway simulator batches 1 s pump readings into 5 s snapshot/delta packets, a .NET 8 isolated Function folds them into full snapshots and pushes them through the Azure SignalR Emulator, and a React dashboard replays them one sample per second on live charts.

**Architecture:** Simulator (minimal API + background session runner) → HTTP POST in Event Grid schema to the Functions host's local `EventGridTrigger` webhook → `TelemetryIngestor` (pure `SnapshotFolder` + Azurite Table state) → `[SignalROutput]` → `asrs-emulator` → React `ReplayQueue` driving Recharts. All domain logic lives in plain class libraries so it is unit-testable without hosts.

**Tech Stack:** .NET 8 (C# 12), Azure Functions isolated worker v4, `Microsoft.Azure.Functions.Worker.Extensions.EventGrid`, `…Extensions.SignalRService`, `…Extensions.Http`, `Azure.Data.Tables`, xUnit, `Microsoft.Extensions.TimeProvider.Testing`, Azurite, Azure SignalR Emulator, `Microsoft.AspNetCore.SignalR.Client`, React + Vite + TypeScript, Recharts, `@microsoft/signalr`, Vitest.

**Spec:** `docs/superpowers/specs/2026-09-23-5sec-batch-telemetry-design.md`

**Deviation from spec layout (intentional):** folding, state storage and ingest orchestration live in a new class library `src/Shape.Ingest/` instead of inside `Shape.Functions`, so they can be tested without the Functions host. `Shape.Functions` becomes a thin binding layer.

## Global Constraints

- Target framework `net8.0` for every .NET project; `global.json` pins SDK `8.0.425` with `rollForward: latestFeature` (a .NET 10 SDK is also installed on this machine — do not let it build the solution).
- Node 20+ (installed: 20.20.2).
- Ports: simulator `5100`, Functions host `7071`, SignalR emulator `8888`, Azurite `10000–10002`, Vite `5173`.
- Event Grid delivery URL: `http://localhost:7071/runtime/webhooks/EventGrid?functionName=IngestTelemetry`, header `aeg-event-type: Notification`, body is a JSON **array** of events.
- Event envelope: `eventType = "Shape.Telemetry.FastPollingBatch"`, `subject = "pumps/pump-001"`, `dataVersion = "1.0"`.
- Wire JSON names verbatim: `Properties`, `session-id`, `telemetry-type` (= `"fast-polling"`), `batchSeq`, `batchStartUtc`, `Samples`, `snapshotTimestampUtc`, `Body`, `Changes`.
- Push JSON names verbatim: `sessionId`, `batchSeq`, `gap`, `partial`, `samples`, `ts`, `values`, `changed`.
- SignalR hub name `telemetry`, target `batch`; connection setting `AzureSignalRConnectionString`.
- Table name `sessionsnapshots`, PK = session id, RK = `state`, columns `SnapshotJson`, `LastBatchSeq`, `Partial`.
- Batch size 5; poll interval fixed at 1 s; session duration 10–30 min, default 10.
- Commit messages: Conventional Commits; **never** add `Co-Authored-By` lines or Claude's name (user rule).
- Shell: commands below are written for PowerShell 7 run from the repo root `D:\Work\Shape_5_sec`.

## Review Focus

1. **Duplicate delivery of an already-processed batch** (simulator retried after a timeout although the Function had saved state) → second delivery is classified stale, returns without a push, state unchanged. Pinned in Task 8 (`Duplicate_delivery_is_dropped_without_push`).
2. **Late batch from a superseded session reaching the UI** (old runner's final flush arrives after Disable → Enable started a new session) → the UI ignores it instead of drawing a second "session start" and interleaving points. Pinned in Task 10 (`ignores_late_batches_from_a_superseded_session`).
3. **Wrong JSON type in enable body** (`{"sessionDurationMinutes":"15"}`) → 400 listing the type error, not a silent default. Pinned in Task 4 (`Enable_rejects_non_integer_duration`).
4. **Final partial batch (1–4 samples)** → folds correctly and replays the exact sample count. Pinned in Task 7 (`Folds_partial_final_batch`) and Task 10 (`plays_partial_batches_sample_by_sample`).
5. **Disable immediately followed by Enable** while the old runner is still flushing → new session starts at once (200, new id); it is not blocked by 409. Pinned in Task 4 (`Enable_after_disable_starts_new_session_immediately`).

---

## File Structure

```
Shape_5_sec/
  global.json                          # SDK pin
  .gitignore
  Shape5Sec.sln
  README.md                            # how to run (Task 12)
  seed/pump-sample.json                # 73-field seed payload
  src/
    Shape.Contracts/
      TelemetryBatch.cs                # wire DTOs
      FoldedBatch.cs                   # push DTOs
      ShapeJson.cs                     # shared JsonSerializerOptions
      PumpSeed.cs                      # seed loader
    Shape.GatewaySimulator/
      Shape.GatewaySimulator.csproj    # Web SDK
      Program.cs                       # host + minimal API wiring
      SimulatorOptions.cs              # CLI parsing
      PumpDevice.cs                    # Modbus stand-in / generators
      DeltaEncoder.cs                  # Body/Changes encoding
      BatchBuffer.cs                   # 5-sample batching
      SessionController.cs             # enable/disable/status semantics
      IBatchPublisher.cs
      EventGridPublisher.cs            # HTTP POST + retry
      FaultInjector.cs                 # --drop-every / --reorder
      SessionRunner.cs                 # per-session loop
    Shape.Ingest/
      SessionState.cs
      SnapshotFolder.cs                # pure folding + classification
      ISnapshotStore.cs                # + SnapshotConflictException
      TableSnapshotStore.cs            # Azurite/Azure Table implementation
      TelemetryIngestor.cs             # load → fold → save orchestration
    Shape.Functions/
      Shape.Functions.csproj
      Program.cs
      IngestTelemetryFunction.cs
      NegotiateFunction.cs
      host.json
      local.settings.json
    Shape.SmokeClient/
      Program.cs                       # headless e2e assertion client
  tests/Shape.Tests/
    TestJson.cs                        # helpers
    Contracts/ContractsSerializationTests.cs
    Simulator/DeltaEncoderTests.cs
    Simulator/BatchBufferTests.cs
    Simulator/PumpDeviceTests.cs
    Simulator/SessionControllerTests.cs
    Simulator/SimulatorOptionsTests.cs
    Simulator/EventGridPublisherTests.cs
    Simulator/FaultInjectorTests.cs
    Simulator/SessionRunnerTests.cs
    Ingest/SnapshotFolderTests.cs
    Ingest/InMemorySnapshotStore.cs
    Ingest/TelemetryIngestorTests.cs
    Ingest/TableSnapshotStoreTests.cs  # [Trait("Category","Integration")]
    Functions/IngestTelemetryFunctionTests.cs
  web/
    src/types.ts
    src/ReplayQueue.ts
    src/ReplayQueue.test.ts
    src/rows.ts
    src/rows.test.ts
    src/useTelemetry.ts
    src/components/Header.tsx
    src/components/TrendChart.tsx
    src/components/StatusPanel.tsx
    src/components/SessionControls.tsx
    src/components/RawDrawer.tsx
    src/App.tsx
    src/main.tsx
    src/index.css
  scripts/run-local.ps1
  scripts/smoke.ps1
```

---

### Task 1: Solution scaffold, contracts, seed

**Files:**
- Create: `global.json`, `.gitignore`, `Shape5Sec.sln`, `seed/pump-sample.json`
- Create: `src/Shape.Contracts/{TelemetryBatch,FoldedBatch,ShapeJson,PumpSeed}.cs`
- Create: `tests/Shape.Tests/TestJson.cs`, `tests/Shape.Tests/Contracts/ContractsSerializationTests.cs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `Shape.Contracts.TelemetryBatch { BatchProperties? Properties; IReadOnlyList<Sample>? Samples }`
  - `Shape.Contracts.BatchProperties { string SessionId; string TelemetryType; long BatchSeq; DateTimeOffset BatchStartUtc }`
  - `Shape.Contracts.Sample { DateTimeOffset SnapshotTimestampUtc; Dictionary<string, JsonElement>? Body; Dictionary<string, JsonElement>? Changes }`
  - `Shape.Contracts.FoldedBatch { string SessionId; long BatchSeq; bool Gap; bool Partial; IReadOnlyList<FoldedSample> Samples }`
  - `Shape.Contracts.FoldedSample { DateTimeOffset Ts; Dictionary<string, JsonElement> Values; IReadOnlyList<string> Changed }`
  - `Shape.Contracts.ShapeJson.Options` (`JsonSerializerOptions`)
  - `Shape.Contracts.PumpSeed.Load(string path)`, `PumpSeed.LoadDefault()`, `PumpSeed.FileName` → `Dictionary<string, JsonElement>`
  - Test helpers `Shape.Tests.TestJson.J(object)`, `TestJson.Map(params (string, object)[])`, `TestJson.Seed()`

- [ ] **Step 1: Scaffold the solution**

```powershell
git init  # already done; skip if .git exists
dotnet new globaljson --sdk-version 8.0.425 --roll-forward latestFeature
dotnet new sln -n Shape5Sec
dotnet new classlib -n Shape.Contracts -o src/Shape.Contracts -f net8.0
dotnet new xunit -n Shape.Tests -o tests/Shape.Tests -f net8.0
Remove-Item src/Shape.Contracts/Class1.cs, tests/Shape.Tests/UnitTest1.cs
dotnet sln add src/Shape.Contracts tests/Shape.Tests
dotnet add tests/Shape.Tests reference src/Shape.Contracts
dotnet --version
```
Expected: last line prints `8.0.425` (or a later 8.0.x).

- [ ] **Step 2: Write `.gitignore`**

```gitignore
bin/
obj/
.vs/
*.user
node_modules/
web/dist/
.azurite/
__azurite_db_*
__blobstorage__/
__queuestorage__/
```

- [ ] **Step 3: Write `seed/pump-sample.json`** (exact content, one line)

```json
{"MotorRunning":true,"OperatingPressurePSI":50623,"OperatingPressureBar":3490,"RemainingMaintenanceTime":1,"OperatingAlarmActive":0,"OperatingWarningActive":0,"MaintenanceAlarmActive":false,"MaintenanceWarningActive":false,"PLCSoftwareVersion":0,"HighPressureSetpointPSI":59000,"HighPressureSetpointBar":4068,"LowPressureSetpointPSI":50834,"LowPressureSetpointBar":3505,"ActivateDumpValveOnHighLow":false,"DumpValveOpenTime":0,"ShutoffTime":0,"PLCYear":0,"PLCMonth":0,"PLCDay":0,"PLCWeekday":0,"PLCHour":0,"PLCMinute":0,"PLCSecond":0,"PLCNanosecond":0,"PLCReturnValue":0,"PLCWritySystemTime":false,"PLCReturnValueT":0,"FrontEndYear":0,"FrontEndMonth":0,"FrontEndDay":0,"FrontEndWeekday":0,"FrontEndHour":0,"FrontEndMinute":0,"FrontEndSecond":0,"FrontEndNanosecond":0,"PumpType":7,"PumpHP":125,"PumpBrandLabel":0,"PumpCoolingVersion":0,"PumpMotorFrequency":0,"PumpSerialNumber":78954293,"ENSoftStartFunctionality":false,"ENCheckvalveTemperature":false,"ENSealLifeTimeTracking":false,"ENRemoteNetworkControl":false,"ENProxSwitchIsNormallyClosed":false,"RemoteModbusIPChanged":false,"IPAddressRemoteStation":"0.0.103.9","OilChangeWarning":false,"OilChangeAlarm":false,"OilFilterWarning":false,"OilFilterAlarm":false,"OperatingHoursTW1":5706,"OperatingHoursCheckValveAssy_A":5706,"OperatingHoursCheckValveAssy_B":5706,"StrokeCounterTW1":5297,"OperatingHoursMotor":5706,"StrokeTimeLeft":5313,"StrokeTimeRight":5325,"StrokeRateLeft":89,"StrokeRateRight":84,"TemperatureCheckValveAlarmA":false,"TemperatureCheckValveAlarmB":false,"TemperatureCheckValveWarningA":false,"TemperatureCheckValveWarningB":false,"BoosterPressureFault":false,"WaterFilterAssyWarning":false,"WaterFilterAssyAlarm":false,"HydraulicOilTemperatureF":103,"HydraulicOilTemperatureC":39,"HydraulicFilterAssyWarning":false,"HydraulicFilterAssyAlarm":false,"ProportionalOutputValue":81}
```

- [ ] **Step 4: Link the seed into the test output.** Edit `tests/Shape.Tests/Shape.Tests.csproj`, add inside `<Project>`:

```xml
  <ItemGroup>
    <None Include="..\..\seed\pump-sample.json" Link="pump-sample.json" CopyToOutputDirectory="PreserveNewest" />
  </ItemGroup>
```

- [ ] **Step 5: Write test helpers** `tests/Shape.Tests/TestJson.cs`

```csharp
using System.Text.Json;
using Shape.Contracts;

namespace Shape.Tests;

public static class TestJson
{
    public static JsonElement J(object value) => JsonSerializer.SerializeToElement(value);

    public static Dictionary<string, JsonElement> Map(params (string Key, object Value)[] entries) =>
        entries.ToDictionary(e => e.Key, e => J(e.Value));

    public static Dictionary<string, JsonElement> Seed() => PumpSeed.LoadDefault();
}
```

- [ ] **Step 6: Write the failing tests** `tests/Shape.Tests/Contracts/ContractsSerializationTests.cs`

```csharp
using System.Text.Json;
using Shape.Contracts;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Contracts;

public class ContractsSerializationTests
{
    [Fact]
    public void Delta_sample_serializes_Changes_and_omits_Body()
    {
        var sample = new Sample
        {
            SnapshotTimestampUtc = DateTimeOffset.Parse("2026-09-23T12:00:01Z"),
            Changes = Map(("OperatingPressurePSI", 54360)),
        };

        var json = JsonSerializer.Serialize(sample, ShapeJson.Options);

        Assert.Contains("\"snapshotTimestampUtc\"", json);
        Assert.Contains("\"Changes\":{\"OperatingPressurePSI\":54360}", json);
        Assert.DoesNotContain("\"Body\"", json);
    }

    [Fact]
    public void Heartbeat_serializes_empty_Changes_object()
    {
        var sample = new Sample { SnapshotTimestampUtc = DateTimeOffset.UnixEpoch, Changes = new() };

        var json = JsonSerializer.Serialize(sample, ShapeJson.Options);

        Assert.Contains("\"Changes\":{}", json);
    }

    [Fact]
    public void Deserializes_wire_packet_from_spec()
    {
        const string json = """
        {
          "Properties": { "session-id": "cc27db6b-508b-488e-a816-faae684491d8", "telemetry-type": "fast-polling",
                          "batchSeq": 3, "batchStartUtc": "2026-09-23T12:00:00Z" },
          "Samples": [
            { "snapshotTimestampUtc": "2026-09-23T12:00:00Z", "Body": { "MotorRunning": true } },
            { "snapshotTimestampUtc": "2026-09-23T12:00:01Z", "Changes": {} }
          ]
        }
        """;

        var batch = JsonSerializer.Deserialize<TelemetryBatch>(json, ShapeJson.Options)!;

        Assert.Equal("cc27db6b-508b-488e-a816-faae684491d8", batch.Properties!.SessionId);
        Assert.Equal("fast-polling", batch.Properties.TelemetryType);
        Assert.Equal(3, batch.Properties.BatchSeq);
        Assert.Equal(2, batch.Samples!.Count);
        Assert.True(batch.Samples[0].Body!["MotorRunning"].GetBoolean());
        Assert.Null(batch.Samples[0].Changes);
        Assert.Empty(batch.Samples[1].Changes!);
    }

    [Fact]
    public void Folded_batch_uses_camelCase_push_names()
    {
        var folded = new FoldedBatch
        {
            SessionId = "s1", BatchSeq = 7, Gap = true, Partial = false,
            Samples = [new FoldedSample { Ts = DateTimeOffset.UnixEpoch, Values = Map(("A", 1)), Changed = ["A"] }],
        };

        var json = JsonSerializer.Serialize(folded, ShapeJson.Options);

        Assert.Contains("\"sessionId\":\"s1\"", json);
        Assert.Contains("\"batchSeq\":7", json);
        Assert.Contains("\"gap\":true", json);
        Assert.Contains("\"partial\":false", json);
        Assert.Contains("\"samples\":[{\"ts\":", json);
        Assert.Contains("\"values\":{\"A\":1}", json);
        Assert.Contains("\"changed\":[\"A\"]", json);
    }

    [Fact]
    public void Seed_loads_all_73_fields()
    {
        var seed = Seed();

        Assert.Equal(73, seed.Count);
        Assert.Equal(50623, seed["OperatingPressurePSI"].GetInt32());
        Assert.Equal("0.0.103.9", seed["IPAddressRemoteStation"].GetString());
    }
}
```

- [ ] **Step 7: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Tests`
Expected: FAIL — `The type or namespace name 'Sample' could not be found`.

- [ ] **Step 8: Implement contracts**

`src/Shape.Contracts/TelemetryBatch.cs`:
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.Contracts;

/// <summary>One 5-second packet sent by the gateway (the Event Grid event's data).</summary>
public sealed record TelemetryBatch
{
    [JsonPropertyName("Properties")] public BatchProperties? Properties { get; init; }
    [JsonPropertyName("Samples")] public IReadOnlyList<Sample>? Samples { get; init; }
}

public sealed record BatchProperties
{
    [JsonPropertyName("session-id")] public string SessionId { get; init; } = "";
    [JsonPropertyName("telemetry-type")] public string TelemetryType { get; init; } = "fast-polling";
    [JsonPropertyName("batchSeq")] public long BatchSeq { get; init; }
    [JsonPropertyName("batchStartUtc")] public DateTimeOffset BatchStartUtc { get; init; }
}

/// <summary>One 1-second reading. Exactly one of <see cref="Body"/> (full snapshot) or <see cref="Changes"/> (delta) is set.</summary>
public sealed record Sample
{
    [JsonPropertyName("snapshotTimestampUtc")] public DateTimeOffset SnapshotTimestampUtc { get; init; }
    [JsonPropertyName("Body")] public Dictionary<string, JsonElement>? Body { get; init; }
    [JsonPropertyName("Changes")] public Dictionary<string, JsonElement>? Changes { get; init; }
}
```

`src/Shape.Contracts/FoldedBatch.cs`:
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.Contracts;

/// <summary>What the Function pushes to SignalR: every sample carries the full folded snapshot.</summary>
public sealed record FoldedBatch
{
    [JsonPropertyName("sessionId")] public string SessionId { get; init; } = "";
    [JsonPropertyName("batchSeq")] public long BatchSeq { get; init; }
    [JsonPropertyName("gap")] public bool Gap { get; init; }
    [JsonPropertyName("partial")] public bool Partial { get; init; }
    [JsonPropertyName("samples")] public IReadOnlyList<FoldedSample> Samples { get; init; } = [];
}

public sealed record FoldedSample
{
    [JsonPropertyName("ts")] public DateTimeOffset Ts { get; init; }
    [JsonPropertyName("values")] public Dictionary<string, JsonElement> Values { get; init; } = new();
    [JsonPropertyName("changed")] public IReadOnlyList<string> Changed { get; init; } = [];
}
```

`src/Shape.Contracts/ShapeJson.cs`:
```csharp
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.Contracts;

public static class ShapeJson
{
    public static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
}
```

`src/Shape.Contracts/PumpSeed.cs`:
```csharp
using System.Text.Json;

namespace Shape.Contracts;

public static class PumpSeed
{
    public const string FileName = "pump-sample.json";

    public static Dictionary<string, JsonElement> Load(string path)
    {
        using var stream = File.OpenRead(path);
        return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(stream)
            ?? throw new InvalidDataException($"Seed file '{path}' is empty.");
    }

    public static Dictionary<string, JsonElement> LoadDefault() =>
        Load(Path.Combine(AppContext.BaseDirectory, FileName));
}
```

- [ ] **Step 9: Run tests — expect pass**

Run: `dotnet test tests/Shape.Tests`
Expected: `Passed!  - Failed: 0, Passed: 5`.

- [ ] **Step 10: Commit**

```powershell
git add global.json .gitignore Shape5Sec.sln seed src/Shape.Contracts tests/Shape.Tests
git commit -m "feat(contracts): add wire and push DTOs, seed payload, solution scaffold"
```

---

### Task 2: Gateway encoding — DeltaEncoder and BatchBuffer

**Files:**
- Create: `src/Shape.GatewaySimulator/Shape.GatewaySimulator.csproj` (via template), `src/Shape.GatewaySimulator/DeltaEncoder.cs`, `src/Shape.GatewaySimulator/BatchBuffer.cs`
- Test: `tests/Shape.Tests/Simulator/DeltaEncoderTests.cs`, `tests/Shape.Tests/Simulator/BatchBufferTests.cs`

**Interfaces:**
- Consumes: `Sample`, `TelemetryBatch`, `BatchProperties` (Task 1).
- Produces:
  - `Shape.GatewaySimulator.DeltaEncoder.Encode(IReadOnlyDictionary<string, JsonElement> reading, DateTimeOffset ts) → Sample`
  - `Shape.GatewaySimulator.BatchBuffer(string sessionId, int size = 5)`; `Add(Sample) → TelemetryBatch?`; `Flush() → TelemetryBatch?`; `const int DefaultSize = 5`

- [ ] **Step 1: Create the simulator project**

```powershell
dotnet new web -n Shape.GatewaySimulator -o src/Shape.GatewaySimulator -f net8.0
dotnet sln add src/Shape.GatewaySimulator
dotnet add src/Shape.GatewaySimulator reference src/Shape.Contracts
dotnet add tests/Shape.Tests reference src/Shape.GatewaySimulator
```

Replace `src/Shape.GatewaySimulator/Program.cs` with a placeholder host so it compiles (Task 6 replaces it):
```csharp
var app = WebApplication.CreateBuilder(args).Build();
app.Run();
```

Add inside `<Project>` of `src/Shape.GatewaySimulator/Shape.GatewaySimulator.csproj`:
```xml
  <ItemGroup>
    <None Include="..\..\seed\pump-sample.json" Link="pump-sample.json" CopyToOutputDirectory="PreserveNewest" />
    <InternalsVisibleTo Include="Shape.Tests" />
  </ItemGroup>
```

Add to `tests/Shape.Tests/Shape.Tests.csproj` (test project references a Web SDK project):
```xml
  <ItemGroup>
    <FrameworkReference Include="Microsoft.AspNetCore.App" />
  </ItemGroup>
```

- [ ] **Step 2: Write failing tests** `tests/Shape.Tests/Simulator/DeltaEncoderTests.cs`

```csharp
using Shape.GatewaySimulator;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Simulator;

public class DeltaEncoderTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-09-23T12:00:00Z");

    [Fact]
    public void First_reading_is_full_Body()
    {
        var encoder = new DeltaEncoder();
        var reading = Map(("A", 1), ("B", true), ("C", "x"));

        var sample = encoder.Encode(reading, T0);

        Assert.Equal(T0, sample.SnapshotTimestampUtc);
        Assert.Null(sample.Changes);
        Assert.Equal(3, sample.Body!.Count);
    }

    [Fact]
    public void Identical_reading_is_empty_Changes_heartbeat()
    {
        var encoder = new DeltaEncoder();
        encoder.Encode(Map(("A", 1)), T0);

        var sample = encoder.Encode(Map(("A", 1)), T0.AddSeconds(1));

        Assert.Null(sample.Body);
        Assert.NotNull(sample.Changes);
        Assert.Empty(sample.Changes!);
    }

    [Fact]
    public void Only_changed_keys_carry_new_values()
    {
        var encoder = new DeltaEncoder();
        encoder.Encode(Map(("A", 1), ("B", 2), ("C", 3)), T0);

        var sample = encoder.Encode(Map(("A", 1), ("B", 20), ("C", 3)), T0.AddSeconds(1));

        Assert.Equal(new[] { "B" }, sample.Changes!.Keys);
        Assert.Equal(20, sample.Changes["B"].GetInt32());
    }

    [Fact]
    public void Delta_is_chained_against_previous_tick_not_baseline()
    {
        var encoder = new DeltaEncoder();
        encoder.Encode(Map(("A", 1)), T0);
        encoder.Encode(Map(("A", 2)), T0.AddSeconds(1));

        var sample = encoder.Encode(Map(("A", 2)), T0.AddSeconds(2));

        Assert.Empty(sample.Changes!);
    }

    [Fact]
    public void Bool_flip_and_string_change_are_detected()
    {
        var encoder = new DeltaEncoder();
        encoder.Encode(Map(("Flag", false), ("Ip", "0.0.103.9")), T0);

        var sample = encoder.Encode(Map(("Flag", true), ("Ip", "0.0.103.10")), T0.AddSeconds(1));

        Assert.True(sample.Changes!["Flag"].GetBoolean());
        Assert.Equal("0.0.103.10", sample.Changes["Ip"].GetString());
    }

    [Fact]
    public void Mutating_the_input_after_encode_does_not_affect_lastSent()
    {
        var encoder = new DeltaEncoder();
        var reading = Map(("A", 1));
        encoder.Encode(reading, T0);
        reading["A"] = J(99);

        var sample = encoder.Encode(Map(("A", 1)), T0.AddSeconds(1));

        Assert.Empty(sample.Changes!);
    }
}
```

`tests/Shape.Tests/Simulator/BatchBufferTests.cs`:
```csharp
using Shape.Contracts;
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class BatchBufferTests
{
    private static Sample S(int second) => new()
    {
        SnapshotTimestampUtc = DateTimeOffset.Parse("2026-09-23T12:00:00Z").AddSeconds(second),
        Changes = new(),
    };

    [Fact]
    public void Emits_batch_on_fifth_sample_only()
    {
        var buffer = new BatchBuffer("s1");

        for (var i = 0; i < 4; i++) Assert.Null(buffer.Add(S(i)));
        var batch = buffer.Add(S(4));

        Assert.NotNull(batch);
        Assert.Equal(5, batch!.Samples!.Count);
        Assert.Equal("s1", batch.Properties!.SessionId);
        Assert.Equal("fast-polling", batch.Properties.TelemetryType);
        Assert.Equal(S(0).SnapshotTimestampUtc, batch.Properties.BatchStartUtc);
    }

    [Fact]
    public void BatchSeq_starts_at_zero_and_increments()
    {
        var buffer = new BatchBuffer("s1");
        var seqs = new List<long>();

        for (var i = 0; i < 15; i++)
            if (buffer.Add(S(i)) is { } b) seqs.Add(b.Properties!.BatchSeq);

        Assert.Equal(new[] { 0L, 1L, 2L }, seqs);
    }

    [Fact]
    public void Flush_emits_partial_batch_then_nothing()
    {
        var buffer = new BatchBuffer("s1");
        buffer.Add(S(0));
        buffer.Add(S(1));

        var partial = buffer.Flush();

        Assert.Equal(2, partial!.Samples!.Count);
        Assert.Null(buffer.Flush());
    }

    [Fact]
    public void Flush_on_empty_buffer_returns_null()
    {
        Assert.Null(new BatchBuffer("s1").Flush());
    }

    [Fact]
    public void New_buffer_for_new_session_restarts_seq()
    {
        var first = new BatchBuffer("s1");
        for (var i = 0; i < 10; i++) first.Add(S(i));

        var second = new BatchBuffer("s2");
        for (var i = 0; i < 4; i++) second.Add(S(i));
        var batch = second.Add(S(4))!;

        Assert.Equal(0, batch.Properties!.BatchSeq);
        Assert.Equal("s2", batch.Properties.SessionId);
    }
}
```

- [ ] **Step 3: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~Simulator"`
Expected: FAIL — `The type or namespace name 'DeltaEncoder' could not be found`.

- [ ] **Step 4: Implement** `src/Shape.GatewaySimulator/DeltaEncoder.cs`

```csharp
using System.Text.Json;
using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>
/// Turns full readings into Body (first reading of a session) or Changes (fields whose value differs from the
/// previous reading, carrying the new value). Create one instance per session.
/// </summary>
public sealed class DeltaEncoder
{
    private Dictionary<string, JsonElement>? _lastSent;

    public Sample Encode(IReadOnlyDictionary<string, JsonElement> reading, DateTimeOffset ts)
    {
        if (_lastSent is null)
        {
            _lastSent = new Dictionary<string, JsonElement>(reading);
            return new Sample { SnapshotTimestampUtc = ts, Body = new Dictionary<string, JsonElement>(reading) };
        }

        var changes = new Dictionary<string, JsonElement>();
        foreach (var (key, value) in reading)
        {
            if (!_lastSent.TryGetValue(key, out var previous) || !JsonValueEquals(previous, value))
                changes[key] = value;
        }

        _lastSent = new Dictionary<string, JsonElement>(reading);
        return new Sample { SnapshotTimestampUtc = ts, Changes = changes };
    }

    // .NET 8 has no JsonElement.DeepEquals; raw-text comparison is exact for the scalar values the device produces.
    private static bool JsonValueEquals(JsonElement a, JsonElement b) =>
        a.ValueKind == b.ValueKind && a.GetRawText() == b.GetRawText();
}
```

`src/Shape.GatewaySimulator/BatchBuffer.cs`:
```csharp
using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>Collects samples into fixed-size batches with a per-session sequence number. One instance per session.</summary>
public sealed class BatchBuffer
{
    public const int DefaultSize = 5;

    private readonly string _sessionId;
    private readonly int _size;
    private readonly List<Sample> _pending = new();
    private long _nextSeq;

    public BatchBuffer(string sessionId, int size = DefaultSize)
    {
        _sessionId = sessionId;
        _size = size;
    }

    public TelemetryBatch? Add(Sample sample)
    {
        _pending.Add(sample);
        return _pending.Count >= _size ? Emit() : null;
    }

    public TelemetryBatch? Flush() => _pending.Count == 0 ? null : Emit();

    private TelemetryBatch Emit()
    {
        var batch = new TelemetryBatch
        {
            Properties = new BatchProperties
            {
                SessionId = _sessionId,
                BatchSeq = _nextSeq++,
                BatchStartUtc = _pending[0].SnapshotTimestampUtc,
            },
            Samples = _pending.ToList(),
        };
        _pending.Clear();
        return batch;
    }
}
```

- [ ] **Step 5: Run tests — expect pass**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~Simulator"`
Expected: `Passed!  - Failed: 0, Passed: 11`.

- [ ] **Step 6: Commit**

```powershell
git add src/Shape.GatewaySimulator tests/Shape.Tests Shape5Sec.sln
git commit -m "feat(simulator): add delta encoder and 5-sample batch buffer"
```

---

### Task 3: PumpDevice (Modbus stand-in)

**Files:**
- Create: `src/Shape.GatewaySimulator/PumpDevice.cs`
- Test: `tests/Shape.Tests/Simulator/PumpDeviceTests.cs`

**Interfaces:**
- Consumes: `TestJson.Seed()` (Task 1).
- Produces: `Shape.GatewaySimulator.PumpDevice(IReadOnlyDictionary<string, JsonElement> seed, int? randomSeed = null, double flagProbability = 0.002)`; `Read() → Dictionary<string, JsonElement>` (advances one tick); `static IReadOnlyList<string> FlagKeys(IEnumerable<string> keys)`.

- [ ] **Step 1: Write failing tests** `tests/Shape.Tests/Simulator/PumpDeviceTests.cs`

```csharp
using System.Text.Json;
using Shape.GatewaySimulator;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Simulator;

public class PumpDeviceTests
{
    private static List<Dictionary<string, JsonElement>> ReadMany(PumpDevice device, int n) =>
        Enumerable.Range(0, n).Select(_ => device.Read()).ToList();

    [Fact]
    public void Every_reading_has_exactly_the_seed_keys()
    {
        var seed = Seed();
        var device = new PumpDevice(seed, randomSeed: 1);

        foreach (var reading in ReadMany(device, 50))
            Assert.True(reading.Keys.ToHashSet().SetEquals(seed.Keys));
    }

    [Fact]
    public void Same_random_seed_gives_identical_sequences()
    {
        var a = ReadMany(new PumpDevice(Seed(), randomSeed: 42), 200);
        var b = ReadMany(new PumpDevice(Seed(), randomSeed: 42), 200);

        for (var i = 0; i < a.Count; i++)
            Assert.Equal(JsonSerializer.Serialize(a[i]), JsonSerializer.Serialize(b[i]));
    }

    [Fact]
    public void Generated_values_stay_within_declared_ranges()
    {
        foreach (var r in ReadMany(new PumpDevice(Seed(), randomSeed: 7), 2000))
        {
            var psi = r["OperatingPressurePSI"].GetInt32();
            Assert.InRange(psi, 54000 - 3000 - 150, 54000 + 3000 + 150);
            Assert.Equal((int)Math.Round(psi / 14.5038), r["OperatingPressureBar"].GetInt32());
            Assert.InRange(r["StrokeRateLeft"].GetInt32(), 80, 100);
            Assert.InRange(r["StrokeRateRight"].GetInt32(), 80, 100);
            Assert.InRange(r["StrokeTimeLeft"].GetInt32(), 5200, 5400);
            Assert.InRange(r["StrokeTimeRight"].GetInt32(), 5200, 5400);
            var f = r["HydraulicOilTemperatureF"].GetInt32();
            Assert.InRange(f, 100, 115);
            Assert.Equal((int)Math.Round((f - 32) * 5 / 9.0), r["HydraulicOilTemperatureC"].GetInt32());
            Assert.InRange(r["ProportionalOutputValue"].GetInt32(), 70, 90);
            Assert.True(r["MotorRunning"].GetBoolean());
        }
    }

    [Fact]
    public void Oil_temperature_changes_only_on_every_tenth_tick()
    {
        var readings = ReadMany(new PumpDevice(Seed(), randomSeed: 3), 100);

        for (var t = 1; t < readings.Count; t++)
        {
            if (t % 10 == 0) continue;
            Assert.Equal(readings[t - 1]["HydraulicOilTemperatureF"].GetInt32(), readings[t]["HydraulicOilTemperatureF"].GetInt32());
        }
    }

    [Fact]
    public void Stroke_counter_is_monotonic_and_static_fields_never_change()
    {
        var seed = Seed();
        var readings = ReadMany(new PumpDevice(seed, randomSeed: 5), 300);

        for (var t = 1; t < readings.Count; t++)
            Assert.True(readings[t]["StrokeCounterTW1"].GetInt64() >= readings[t - 1]["StrokeCounterTW1"].GetInt64());
        Assert.True(readings[^1]["StrokeCounterTW1"].GetInt64() > seed["StrokeCounterTW1"].GetInt64());

        foreach (var r in readings)
        {
            Assert.Equal(seed["PumpSerialNumber"].GetRawText(), r["PumpSerialNumber"].GetRawText());
            Assert.Equal(seed["IPAddressRemoteStation"].GetRawText(), r["IPAddressRemoteStation"].GetRawText());
            Assert.Equal(seed["HighPressureSetpointPSI"].GetRawText(), r["HighPressureSetpointPSI"].GetRawText());
        }
    }

    [Fact]
    public void Flags_are_detected_by_name()
    {
        var flags = PumpDevice.FlagKeys(Seed().Keys);

        Assert.Equal(17, flags.Count);
        Assert.Contains("BoosterPressureFault", flags);
        Assert.Contains("OperatingAlarmActive", flags);
        Assert.DoesNotContain("MotorRunning", flags);
    }

    [Fact]
    public void Flag_probability_zero_keeps_all_flags_off_and_one_turns_them_on()
    {
        var off = new PumpDevice(Seed(), randomSeed: 1, flagProbability: 0).Read();
        var on = new PumpDevice(Seed(), randomSeed: 1, flagProbability: 1).Read();

        Assert.False(off["OilFilterWarning"].GetBoolean());
        Assert.Equal(0, off["OperatingAlarmActive"].GetInt32());
        Assert.True(on["OilFilterWarning"].GetBoolean());
        Assert.Equal(1, on["OperatingAlarmActive"].GetInt32());   // int flag toggles 0↔1, stays an int
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~PumpDevice"`
Expected: FAIL — `The type or namespace name 'PumpDevice' could not be found`.

- [ ] **Step 3: Implement** `src/Shape.GatewaySimulator/PumpDevice.cs`

```csharp
using System.Text.Json;

namespace Shape.GatewaySimulator;

/// <summary>
/// Stand-in for the Modbus pump. Each <see cref="Read"/> is one 1-second poll. Fields without a generator
/// keep their seed value (spec §5.1).
/// </summary>
public sealed class PumpDevice
{
    private const int FlagHoldTicks = 20;
    private static readonly string[] HourKeys =
        ["OperatingHoursTW1", "OperatingHoursCheckValveAssy_A", "OperatingHoursCheckValveAssy_B", "OperatingHoursMotor"];

    private readonly Dictionary<string, JsonElement> _seed;
    private readonly Random _rng;
    private readonly double _flagProbability;
    private readonly IReadOnlyList<string> _flagKeys;
    private readonly Dictionary<string, int> _flagRemaining = new();
    private long _tick;
    private int _strokeRateLeft, _strokeRateRight, _strokeTimeLeft, _strokeTimeRight, _oilTempF;
    private double _strokeCounter;

    public PumpDevice(IReadOnlyDictionary<string, JsonElement> seed, int? randomSeed = null, double flagProbability = 0.002)
    {
        _seed = new Dictionary<string, JsonElement>(seed);
        _rng = randomSeed is { } s ? new Random(s) : new Random();
        _flagProbability = flagProbability;
        _flagKeys = FlagKeys(_seed.Keys);
        _strokeRateLeft = Math.Clamp(SeedInt("StrokeRateLeft"), 80, 100);
        _strokeRateRight = Math.Clamp(SeedInt("StrokeRateRight"), 80, 100);
        _strokeTimeLeft = Math.Clamp(SeedInt("StrokeTimeLeft"), 5200, 5400);
        _strokeTimeRight = Math.Clamp(SeedInt("StrokeTimeRight"), 5200, 5400);
        _oilTempF = Math.Clamp(SeedInt("HydraulicOilTemperatureF"), 100, 115);
        _strokeCounter = SeedInt("StrokeCounterTW1");
    }

    public static IReadOnlyList<string> FlagKeys(IEnumerable<string> keys) =>
        keys.Where(k => k.Contains("Alarm") || k.Contains("Warning") || k == "BoosterPressureFault").ToList();

    public Dictionary<string, JsonElement> Read()
    {
        var t = _tick++;
        var generated = new Dictionary<string, object>();

        var psi = (int)Math.Round(54000 + 3000 * Math.Sin(2 * Math.PI * t / 30.0) + (_rng.NextDouble() * 300 - 150));
        generated["OperatingPressurePSI"] = psi;
        generated["OperatingPressureBar"] = (int)Math.Round(psi / 14.5038);

        _strokeRateLeft = Walk(_strokeRateLeft, 2, 80, 100);
        _strokeRateRight = Walk(_strokeRateRight, 2, 80, 100);
        _strokeTimeLeft = Walk(_strokeTimeLeft, 10, 5200, 5400);
        _strokeTimeRight = Walk(_strokeTimeRight, 10, 5200, 5400);
        generated["StrokeRateLeft"] = _strokeRateLeft;
        generated["StrokeRateRight"] = _strokeRateRight;
        generated["StrokeTimeLeft"] = _strokeTimeLeft;
        generated["StrokeTimeRight"] = _strokeTimeRight;

        _strokeCounter += (_strokeRateLeft + _strokeRateRight) / 2.0 / 60.0;
        generated["StrokeCounterTW1"] = (long)_strokeCounter;

        if (t > 0 && t % 10 == 0) _oilTempF = Walk(_oilTempF, 1, 100, 115);
        generated["HydraulicOilTemperatureF"] = _oilTempF;
        generated["HydraulicOilTemperatureC"] = (int)Math.Round((_oilTempF - 32) * 5 / 9.0);

        generated["ProportionalOutputValue"] = (int)Math.Round(70 + Math.Clamp((psi - 51000) / 6000.0, 0, 1) * 20);

        foreach (var key in HourKeys)
            if (_seed.ContainsKey(key)) generated[key] = SeedInt(key) + t / 3600;

        foreach (var key in _flagKeys)
        {
            var active = NextFlagState(key);
            generated[key] = _seed[key].ValueKind is JsonValueKind.True or JsonValueKind.False ? active : (active ? 1 : 0);
        }

        generated["MotorRunning"] = true;

        var reading = new Dictionary<string, JsonElement>(_seed);
        foreach (var (key, value) in generated)
            if (reading.ContainsKey(key)) reading[key] = JsonSerializer.SerializeToElement(value);
        return reading;
    }

    private bool NextFlagState(string key)
    {
        var remaining = _flagRemaining.GetValueOrDefault(key);
        if (remaining > 0)
        {
            _flagRemaining[key] = remaining - 1;
            return true;
        }
        if (_rng.NextDouble() < _flagProbability)
        {
            _flagRemaining[key] = FlagHoldTicks - 1;
            return true;
        }
        return false;
    }

    private int Walk(int value, int step, int min, int max) => Math.Clamp(value + _rng.Next(-step, step + 1), min, max);

    private int SeedInt(string key) => _seed[key].GetInt32();
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~PumpDevice"`
Expected: `Passed!  - Failed: 0, Passed: 7`.

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.GatewaySimulator/PumpDevice.cs tests/Shape.Tests/Simulator/PumpDeviceTests.cs
git commit -m "feat(simulator): add seeded pump device with field generators"
```

---

### Task 4: SessionController and SimulatorOptions

**Files:**
- Create: `src/Shape.GatewaySimulator/SessionController.cs`, `src/Shape.GatewaySimulator/SimulatorOptions.cs`
- Test: `tests/Shape.Tests/Simulator/SessionControllerTests.cs`, `tests/Shape.Tests/Simulator/SimulatorOptionsTests.cs`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `FastPollingSession(Guid SessionId, int DurationMinutes, DateTimeOffset StartedUtc, CancellationTokenSource Cts)` (record)
  - `ControlResult(int StatusCode, object Body, FastPollingSession? Session = null)` (record)
  - `SessionController(TimeProvider? time = null)`; `Enable(string? rawBody) → ControlResult`; `Disable() → ControlResult`; `Status() → object`; `Clear(FastPollingSession) → void`; `Active → FastPollingSession?`
  - `SimulatorOptions { int? RandomSeed; int DropEvery; bool Reorder; Uri FunctionsUrl; string Urls }`; `static SimulatorOptions Parse(string[] args)`; `static readonly Uri DefaultFunctionsUrl`

- [ ] **Step 1: Write failing tests** `tests/Shape.Tests/Simulator/SessionControllerTests.cs`

```csharp
using System.Text.Json;
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class SessionControllerTests
{
    private static JsonElement Body(ControlResult r) => JsonSerializer.SerializeToElement(r.Body);

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("{}")]
    [InlineData("not json")]
    public void Enable_with_missing_or_unparseable_body_uses_defaults(string? raw)
    {
        var result = new SessionController().Enable(raw);

        Assert.Equal(200, result.StatusCode);
        var body = Body(result);
        Assert.Equal("accepted", body.GetProperty("result").GetString());
        Assert.Equal(10, body.GetProperty("durationMinutes").GetInt32());
        Assert.Equal(1, body.GetProperty("intervalSeconds").GetInt32());
        Assert.Equal(result.Session!.SessionId.ToString(), body.GetProperty("sessionId").GetString());
        Assert.Equal(new[] { "sessionDurationMinutes", "pollIntervalSeconds" },
            body.GetProperty("defaultsApplied").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void Enable_uses_supplied_duration_case_insensitively()
    {
        var result = new SessionController().Enable("""{"SESSIONDURATIONMINUTES": 15, "pollIntervalSeconds": 1}""");

        Assert.Equal(200, result.StatusCode);
        Assert.Equal(15, result.Session!.DurationMinutes);
        Assert.Empty(Body(result).GetProperty("defaultsApplied").EnumerateArray());
    }

    [Fact]
    public void Enable_rejects_out_of_range_values_listing_every_error()
    {
        var result = new SessionController().Enable("""{"sessionDurationMinutes": 45, "pollIntervalSeconds": 5}""");

        Assert.Equal(400, result.StatusCode);
        Assert.Null(result.Session);
        var body = Body(result);
        Assert.Equal("rejected", body.GetProperty("result").GetString());
        Assert.Equal("Invalid request.", body.GetProperty("reason").GetString());
        var errors = body.GetProperty("errors").EnumerateArray().Select(e => e.GetString()).ToList();
        Assert.Equal(2, errors.Count);
        Assert.Contains("'sessionDurationMinutes'=45 is outside the allowed range (10-30).", errors);
        Assert.Contains("'pollIntervalSeconds'=5 is not supported; only 1 is allowed.", errors);
    }

    [Fact]
    public void Enable_rejects_non_integer_duration()
    {
        var result = new SessionController().Enable("""{"sessionDurationMinutes": "15"}""");

        Assert.Equal(400, result.StatusCode);
        Assert.Contains("'sessionDurationMinutes' must be an integer.",
            Body(result).GetProperty("errors").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void Enable_while_active_is_409_but_validation_runs_first()
    {
        var controller = new SessionController();
        controller.Enable(null);

        var conflict = controller.Enable(null);
        var invalid = controller.Enable("""{"sessionDurationMinutes": 5}""");

        Assert.Equal(409, conflict.StatusCode);
        Assert.Equal("A fast polling session is already active.", Body(conflict).GetProperty("reason").GetString());
        Assert.Equal(400, invalid.StatusCode);
    }

    [Fact]
    public void Disable_when_idle_is_409()
    {
        var result = new SessionController().Disable();

        Assert.Equal(409, result.StatusCode);
        Assert.Equal("No fast polling session is active.", Body(result).GetProperty("reason").GetString());
    }

    [Fact]
    public void Disable_cancels_and_clears_the_active_session()
    {
        var controller = new SessionController();
        var session = controller.Enable(null).Session!;

        var result = controller.Disable();

        Assert.Equal(200, result.StatusCode);
        Assert.Equal(session.SessionId.ToString(), Body(result).GetProperty("sessionId").GetString());
        Assert.True(session.Cts.IsCancellationRequested);
        Assert.Null(controller.Active);
    }

    [Fact]
    public void Enable_after_disable_starts_new_session_immediately()
    {
        var controller = new SessionController();
        var first = controller.Enable(null).Session!;
        controller.Disable();

        var second = controller.Enable(null);

        Assert.Equal(200, second.StatusCode);
        Assert.NotEqual(first.SessionId, second.Session!.SessionId);
    }

    [Fact]
    public void Clear_only_clears_the_matching_session()
    {
        var controller = new SessionController();
        var first = controller.Enable(null).Session!;
        controller.Disable();
        var second = controller.Enable(null).Session!;

        controller.Clear(first);

        Assert.Same(second, controller.Active);
    }

    [Fact]
    public void Status_reports_active_session()
    {
        var controller = new SessionController();
        Assert.False(JsonSerializer.SerializeToElement(controller.Status()).GetProperty("active").GetBoolean());

        var session = controller.Enable(null).Session!;
        var status = JsonSerializer.SerializeToElement(controller.Status());

        Assert.True(status.GetProperty("active").GetBoolean());
        Assert.Equal(session.SessionId.ToString(), status.GetProperty("sessionId").GetString());
        Assert.Equal(10, status.GetProperty("durationMinutes").GetInt32());
    }
}
```

`tests/Shape.Tests/Simulator/SimulatorOptionsTests.cs`:
```csharp
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class SimulatorOptionsTests
{
    [Fact]
    public void Defaults_when_no_args()
    {
        var o = SimulatorOptions.Parse([]);

        Assert.Null(o.RandomSeed);
        Assert.Equal(0, o.DropEvery);
        Assert.False(o.Reorder);
        Assert.Equal(SimulatorOptions.DefaultFunctionsUrl, o.FunctionsUrl);
        Assert.Equal("http://localhost:5100", o.Urls);
    }

    [Fact]
    public void Parses_all_flags()
    {
        var o = SimulatorOptions.Parse(["--seed", "42", "--drop-every", "3", "--reorder", "--functions-url", "http://x:1/hook"]);

        Assert.Equal(42, o.RandomSeed);
        Assert.Equal(3, o.DropEvery);
        Assert.True(o.Reorder);
        Assert.Equal(new Uri("http://x:1/hook"), o.FunctionsUrl);
    }

    [Theory]
    [InlineData("--drop-every", "1")]
    [InlineData("--drop-every", "abc")]
    [InlineData("--seed", "x")]
    [InlineData("--bogus", "1")]
    public void Rejects_invalid_args(string flag, string value)
    {
        Assert.Throws<ArgumentException>(() => SimulatorOptions.Parse([flag, value]));
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~SessionController|FullyQualifiedName~SimulatorOptions"`
Expected: FAIL — `The type or namespace name 'SessionController' could not be found`.

- [ ] **Step 3: Implement** `src/Shape.GatewaySimulator/SessionController.cs`

```csharp
using System.Text.Json;

namespace Shape.GatewaySimulator;

public sealed record FastPollingSession(Guid SessionId, int DurationMinutes, DateTimeOffset StartedUtc, CancellationTokenSource Cts);

public sealed record ControlResult(int StatusCode, object Body, FastPollingSession? Session = null);

/// <summary>
/// Mirrors the FastPollingEnable/FastPollingDisable direct methods (spec §5.2). Only one session at a time.
/// Disable clears the slot immediately so a new session can start while the old runner is still flushing.
/// </summary>
public sealed class SessionController
{
    public const int DefaultDurationMinutes = 10;
    public const int MinDurationMinutes = 10;
    public const int MaxDurationMinutes = 30;
    public const int IntervalSeconds = 1;

    private readonly TimeProvider _time;
    private FastPollingSession? _active;

    public SessionController(TimeProvider? time = null) => _time = time ?? TimeProvider.System;

    public FastPollingSession? Active => Volatile.Read(ref _active);

    public ControlResult Enable(string? rawBody)
    {
        var (duration, defaultsApplied, errors) = Validate(rawBody);
        if (errors.Count > 0)
            return new(400, new { result = "rejected", reason = "Invalid request.", errors });

        var session = new FastPollingSession(Guid.NewGuid(), duration, _time.GetUtcNow(), new CancellationTokenSource());
        if (Interlocked.CompareExchange(ref _active, session, null) is not null)
        {
            session.Cts.Dispose();
            return new(409, new { result = "rejected", reason = "A fast polling session is already active." });
        }

        return new(200, new
        {
            result = "accepted",
            durationMinutes = duration,
            intervalSeconds = IntervalSeconds,
            sessionId = session.SessionId,
            defaultsApplied,
        }, session);
    }

    public ControlResult Disable()
    {
        var session = Volatile.Read(ref _active);
        if (session is null || Interlocked.CompareExchange(ref _active, null, session) != session)
            return new(409, new { result = "rejected", reason = "No fast polling session is active." });

        session.Cts.Cancel();
        return new(200, new { result = "accepted", sessionId = session.SessionId });
    }

    public void Clear(FastPollingSession session) => Interlocked.CompareExchange(ref _active, null, session);

    public object Status() => Active is { } s
        ? new { active = true, sessionId = s.SessionId, startedUtc = s.StartedUtc, durationMinutes = s.DurationMinutes }
        : new { active = false };

    private static (int Duration, List<string> DefaultsApplied, List<string> Errors) Validate(string? raw)
    {
        var errors = new List<string>();
        int? duration = null, interval = null;
        bool durationSupplied = false, intervalSupplied = false;

        if (!string.IsNullOrWhiteSpace(raw))
        {
            try
            {
                using var doc = JsonDocument.Parse(raw);
                if (doc.RootElement.ValueKind == JsonValueKind.Object)
                {
                    foreach (var p in doc.RootElement.EnumerateObject())
                    {
                        if (p.Name.Equals("sessionDurationMinutes", StringComparison.OrdinalIgnoreCase))
                        {
                            durationSupplied = true;
                            duration = ReadInt(p, errors);
                        }
                        else if (p.Name.Equals("pollIntervalSeconds", StringComparison.OrdinalIgnoreCase))
                        {
                            intervalSupplied = true;
                            interval = ReadInt(p, errors);
                        }
                    }
                }
            }
            catch (JsonException)
            {
                // Unparseable body is treated as "no parameters supplied" (same as the direct-method contract).
            }
        }

        if (duration is { } d && (d < MinDurationMinutes || d > MaxDurationMinutes))
            errors.Add($"'sessionDurationMinutes'={d} is outside the allowed range ({MinDurationMinutes}-{MaxDurationMinutes}).");
        if (interval is { } i && i != IntervalSeconds)
            errors.Add($"'pollIntervalSeconds'={i} is not supported; only {IntervalSeconds} is allowed.");

        var defaultsApplied = new List<string>();
        if (!durationSupplied) defaultsApplied.Add("sessionDurationMinutes");
        if (!intervalSupplied) defaultsApplied.Add("pollIntervalSeconds");

        return (duration ?? DefaultDurationMinutes, defaultsApplied, errors);
    }

    private static int? ReadInt(JsonProperty p, List<string> errors)
    {
        if (p.Value.ValueKind == JsonValueKind.Number && p.Value.TryGetInt32(out var value)) return value;
        errors.Add($"'{p.Name}' must be an integer.");
        return null;
    }
}
```

Note: the type error message uses the property name as sent (`p.Name`), so the test sends the canonical casing.

`src/Shape.GatewaySimulator/SimulatorOptions.cs`:
```csharp
namespace Shape.GatewaySimulator;

public sealed record SimulatorOptions
{
    public static readonly Uri DefaultFunctionsUrl =
        new("http://localhost:7071/runtime/webhooks/EventGrid?functionName=IngestTelemetry");

    public int? RandomSeed { get; init; }
    /// <summary>0 = off; N ≥ 2 = drop every Nth batch.</summary>
    public int DropEvery { get; init; }
    public bool Reorder { get; init; }
    public Uri FunctionsUrl { get; init; } = DefaultFunctionsUrl;
    public string Urls { get; init; } = "http://localhost:5100";

    public static SimulatorOptions Parse(string[] args)
    {
        var o = new SimulatorOptions();
        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--seed": o = o with { RandomSeed = Int(args, ++i, "--seed") }; break;
                case "--drop-every":
                    var n = Int(args, ++i, "--drop-every");
                    if (n < 2) throw new ArgumentException("--drop-every must be >= 2.");
                    o = o with { DropEvery = n };
                    break;
                case "--reorder": o = o with { Reorder = true }; break;
                case "--functions-url": o = o with { FunctionsUrl = new Uri(Value(args, ++i, "--functions-url")) }; break;
                case "--urls": o = o with { Urls = Value(args, ++i, "--urls") }; break;
                default: throw new ArgumentException($"Unknown argument '{args[i]}'.");
            }
        }
        return o;
    }

    private static string Value(string[] args, int i, string flag) =>
        i < args.Length ? args[i] : throw new ArgumentException($"{flag} needs a value.");

    private static int Int(string[] args, int i, string flag) =>
        int.TryParse(Value(args, i, flag), out var v) ? v : throw new ArgumentException($"{flag} needs an integer.");
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~SessionController|FullyQualifiedName~SimulatorOptions"`
Expected: `Passed!  - Failed: 0, Passed: 19`.

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.GatewaySimulator tests/Shape.Tests/Simulator
git commit -m "feat(simulator): add fast-polling session controller and CLI options"
```

---

### Task 5: EventGridPublisher and FaultInjector

**Files:**
- Create: `src/Shape.GatewaySimulator/IBatchPublisher.cs`, `src/Shape.GatewaySimulator/EventGridPublisher.cs`, `src/Shape.GatewaySimulator/FaultInjector.cs`
- Test: `tests/Shape.Tests/Simulator/EventGridPublisherTests.cs`, `tests/Shape.Tests/Simulator/FaultInjectorTests.cs`

**Interfaces:**
- Consumes: `TelemetryBatch`, `ShapeJson.Options` (Task 1); `BatchBuffer` (Task 2, in tests).
- Produces:
  - `IBatchPublisher.PublishAsync(TelemetryBatch batch, CancellationToken ct) → Task<bool>`
  - `EventGridPublisher(HttpClient http, Uri endpoint, IReadOnlyList<TimeSpan> retryDelays, ILogger<EventGridPublisher> logger) : IBatchPublisher`; constants `EventType`, `Subject`; `static readonly TimeSpan[] DefaultRetryDelays`
  - `FaultInjector(int dropEvery, bool reorder)`; `Process(TelemetryBatch) → IReadOnlyList<TelemetryBatch>`; `Drain() → IReadOnlyList<TelemetryBatch>`

- [ ] **Step 1: Write failing tests** `tests/Shape.Tests/Simulator/EventGridPublisherTests.cs`

```csharp
using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.Contracts;
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class EventGridPublisherTests
{
    // Captures everything at send time: the publisher disposes the request afterwards.
    public sealed record Captured(HttpMethod Method, Uri? Uri, string? AegEventType, string? MediaType, string Body);

    private sealed class StubHandler(params object[] outcomes) : HttpMessageHandler
    {
        private int _call;
        public List<Captured> Requests { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add(new Captured(
                request.Method,
                request.RequestUri,
                request.Headers.TryGetValues("aeg-event-type", out var v) ? v.Single() : null,
                request.Content!.Headers.ContentType?.MediaType,
                await request.Content.ReadAsStringAsync(ct)));
            var outcome = outcomes[Math.Min(_call++, outcomes.Length - 1)];
            if (outcome is Exception ex) throw ex;
            return new HttpResponseMessage((HttpStatusCode)outcome);
        }
    }

    private static readonly Uri Endpoint = new("http://localhost:7071/runtime/webhooks/EventGrid?functionName=IngestTelemetry");

    private static TelemetryBatch Batch(long seq = 0) => new()
    {
        Properties = new BatchProperties { SessionId = "s1", BatchSeq = seq, BatchStartUtc = DateTimeOffset.UnixEpoch },
        Samples = [new Sample { SnapshotTimestampUtc = DateTimeOffset.UnixEpoch, Changes = new() }],
    };

    private static EventGridPublisher Publisher(StubHandler handler, int retries = 3) =>
        new(new HttpClient(handler), Endpoint, Enumerable.Repeat(TimeSpan.Zero, retries).ToArray(),
            NullLogger<EventGridPublisher>.Instance);

    [Fact]
    public async Task Posts_event_grid_array_with_notification_header()
    {
        var handler = new StubHandler(HttpStatusCode.OK);

        var ok = await Publisher(handler).PublishAsync(Batch(4), CancellationToken.None);

        Assert.True(ok);
        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal(Endpoint, request.Uri);
        Assert.Equal("Notification", request.AegEventType);
        Assert.Equal("application/json", request.MediaType);

        using var doc = JsonDocument.Parse(request.Body);
        var ev = Assert.Single(doc.RootElement.EnumerateArray());
        Assert.Equal("Shape.Telemetry.FastPollingBatch", ev.GetProperty("eventType").GetString());
        Assert.Equal("pumps/pump-001", ev.GetProperty("subject").GetString());
        Assert.Equal("1.0", ev.GetProperty("dataVersion").GetString());
        Assert.False(string.IsNullOrEmpty(ev.GetProperty("id").GetString()));
        Assert.True(ev.TryGetProperty("eventTime", out _));
        Assert.Equal(4, ev.GetProperty("data").GetProperty("Properties").GetProperty("batchSeq").GetInt64());
        Assert.Equal("s1", ev.GetProperty("data").GetProperty("Properties").GetProperty("session-id").GetString());
    }

    [Fact]
    public async Task Retries_on_failure_status_then_succeeds()
    {
        var handler = new StubHandler(HttpStatusCode.InternalServerError, HttpStatusCode.OK);

        Assert.True(await Publisher(handler).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Retries_on_connection_error()
    {
        var handler = new StubHandler(new HttpRequestException("refused"), HttpStatusCode.OK);

        Assert.True(await Publisher(handler).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Gives_up_after_all_retries_and_returns_false()
    {
        var handler = new StubHandler(HttpStatusCode.ServiceUnavailable);

        Assert.False(await Publisher(handler, retries: 3).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(4, handler.Requests.Count); // 1 attempt + 3 retries
    }

    [Fact]
    public void Default_retry_delays_are_1_2_4_seconds()
    {
        Assert.Equal(new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4) },
            EventGridPublisher.DefaultRetryDelays);
    }
}
```

`tests/Shape.Tests/Simulator/FaultInjectorTests.cs`:
```csharp
using Shape.Contracts;
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class FaultInjectorTests
{
    private static TelemetryBatch B(long seq) => new()
    {
        Properties = new BatchProperties { SessionId = "s1", BatchSeq = seq },
        Samples = [new Sample { Changes = new() }],
    };

    private static List<long> Run(FaultInjector f, int count)
    {
        var sent = new List<long>();
        for (var i = 0; i < count; i++) sent.AddRange(f.Process(B(i)).Select(b => b.Properties!.BatchSeq));
        sent.AddRange(f.Drain().Select(b => b.Properties!.BatchSeq));
        return sent;
    }

    [Fact]
    public void No_faults_passes_everything_through_in_order()
    {
        Assert.Equal(new[] { 0L, 1, 2, 3 }, Run(new FaultInjector(0, false), 4));
    }

    [Fact]
    public void Drop_every_2_keeps_baseline_and_drops_odd_batches()
    {
        Assert.Equal(new[] { 0L, 2, 4 }, Run(new FaultInjector(2, false), 6));
    }

    [Fact]
    public void Reorder_swaps_adjacent_pairs_and_drains_leftover()
    {
        Assert.Equal(new[] { 1L, 0, 3, 2, 4 }, Run(new FaultInjector(0, true), 5));
    }

    [Fact]
    public void Drop_applies_before_reorder()
    {
        // dropped: 2,5 → survivors 0,1,3,4 → swapped pairs
        Assert.Equal(new[] { 1L, 0, 4, 3 }, Run(new FaultInjector(3, true), 6));
    }
}
```

- [ ] **Step 2: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~EventGridPublisher|FullyQualifiedName~FaultInjector"`
Expected: FAIL — `The type or namespace name 'EventGridPublisher' could not be found`.

- [ ] **Step 3: Implement**

`src/Shape.GatewaySimulator/IBatchPublisher.cs`:
```csharp
using Shape.Contracts;

namespace Shape.GatewaySimulator;

public interface IBatchPublisher
{
    /// <returns>true if delivered; false if dropped after all retries.</returns>
    Task<bool> PublishAsync(TelemetryBatch batch, CancellationToken ct);
}
```

`src/Shape.GatewaySimulator/EventGridPublisher.cs`:
```csharp
using System.Text;
using System.Text.Json;
using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>Delivers a batch the way Event Grid would: POST an EventGridEvent array to the Function's webhook.</summary>
public sealed class EventGridPublisher(
    HttpClient http, Uri endpoint, IReadOnlyList<TimeSpan> retryDelays, ILogger<EventGridPublisher> logger) : IBatchPublisher
{
    public const string EventType = "Shape.Telemetry.FastPollingBatch";
    public const string Subject = "pumps/pump-001";
    public static readonly TimeSpan[] DefaultRetryDelays =
        [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4)];

    public async Task<bool> PublishAsync(TelemetryBatch batch, CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new[]
        {
            new
            {
                id = Guid.NewGuid().ToString(),
                eventType = EventType,
                subject = Subject,
                eventTime = DateTimeOffset.UtcNow,
                dataVersion = "1.0",
                data = batch,
            },
        }, ShapeJson.Options);

        var seq = batch.Properties?.BatchSeq;
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
                {
                    Content = new StringContent(payload, Encoding.UTF8, "application/json"),
                };
                request.Headers.Add("aeg-event-type", "Notification");
                using var response = await http.SendAsync(request, ct);
                if (response.IsSuccessStatusCode)
                {
                    logger.LogInformation("Delivered batch {BatchSeq} ({Count} samples)", seq, batch.Samples?.Count);
                    return true;
                }
                logger.LogWarning("Batch {BatchSeq} attempt {Attempt} got HTTP {Status}", seq, attempt + 1, (int)response.StatusCode);
            }
            catch (HttpRequestException ex)
            {
                logger.LogWarning("Batch {BatchSeq} attempt {Attempt} failed: {Error}", seq, attempt + 1, ex.Message);
            }

            if (attempt >= retryDelays.Count)
            {
                logger.LogError("Dropping batch {BatchSeq} after {Attempts} attempts", seq, attempt + 1);
                return false;
            }
            await Task.Delay(retryDelays[attempt], ct);
        }
    }
}
```

`src/Shape.GatewaySimulator/FaultInjector.cs`:
```csharp
using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>Test-only faults: drop every Nth batch, and/or send adjacent batches swapped. One instance per session.</summary>
public sealed class FaultInjector(int dropEvery, bool reorder)
{
    private long _count;
    private TelemetryBatch? _held;

    public IReadOnlyList<TelemetryBatch> Process(TelemetryBatch batch)
    {
        _count++;
        if (dropEvery >= 2 && _count % dropEvery == 0) return [];
        if (!reorder) return [batch];
        if (_held is null)
        {
            _held = batch;
            return [];
        }
        var pair = new[] { batch, _held };
        _held = null;
        return pair;
    }

    public IReadOnlyList<TelemetryBatch> Drain()
    {
        if (_held is null) return [];
        var held = _held;
        _held = null;
        return [held];
    }
}
```

- [ ] **Step 4: Run tests — expect pass**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~EventGridPublisher|FullyQualifiedName~FaultInjector"`
Expected: `Passed!  - Failed: 0, Passed: 9`.

- [ ] **Step 5: Commit**

```powershell
git add src/Shape.GatewaySimulator tests/Shape.Tests/Simulator
git commit -m "feat(simulator): add Event Grid webhook publisher with retry and fault injector"
```

---

### Task 6: SessionRunner and simulator host

**Files:**
- Create: `src/Shape.GatewaySimulator/SessionRunner.cs`
- Modify: `src/Shape.GatewaySimulator/Program.cs` (replace placeholder)
- Test: `tests/Shape.Tests/Simulator/SessionRunnerTests.cs`

**Interfaces:**
- Consumes: `PumpDevice`, `DeltaEncoder`, `BatchBuffer`, `FaultInjector`, `IBatchPublisher`, `SessionController`, `FastPollingSession`, `SimulatorOptions`, `EventGridPublisher` (Tasks 2–5).
- Produces: `SessionRunner(Func<PumpDevice> deviceFactory, IBatchPublisher publisher, SimulatorOptions options, SessionController controller, TimeProvider time, ILogger<SessionRunner> logger)`; `RunAsync(FastPollingSession session, CancellationToken hostStopping) → Task`. HTTP: `POST /fast-polling/enable`, `POST /fast-polling/disable`, `GET /fast-polling/status` on `http://localhost:5100`, CORS for `http://localhost:5173`.

- [ ] **Step 1: Add the fake-time package**

```powershell
dotnet add tests/Shape.Tests package Microsoft.Extensions.TimeProvider.Testing
```

- [ ] **Step 2: Write failing tests** `tests/Shape.Tests/Simulator/SessionRunnerTests.cs`

```csharp
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.Contracts;
using Shape.GatewaySimulator;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Simulator;

public class SessionRunnerTests
{
    private sealed class RecordingPublisher : IBatchPublisher
    {
        private readonly object _gate = new();
        private readonly List<TelemetryBatch> _batches = new();

        public IReadOnlyList<TelemetryBatch> Batches { get { lock (_gate) return _batches.ToList(); } }

        public Task<bool> PublishAsync(TelemetryBatch batch, CancellationToken ct)
        {
            lock (_gate) _batches.Add(batch);
            return Task.FromResult(true);
        }
    }

    private static async Task Until(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Condition not met");
            await Task.Delay(10);
        }
    }

    private static async Task AdvanceSeconds(FakeTimeProvider time, int seconds)
    {
        for (var i = 0; i < seconds; i++)
        {
            time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(20); // let the timer continuation run
        }
    }

    private static (SessionRunner Runner, SessionController Controller, RecordingPublisher Publisher, FakeTimeProvider Time)
        Create(SimulatorOptions? options = null)
    {
        var time = new FakeTimeProvider(DateTimeOffset.Parse("2026-09-23T12:00:00Z"));
        var controller = new SessionController(time);
        var publisher = new RecordingPublisher();
        var seed = Seed();
        var runner = new SessionRunner(() => new PumpDevice(seed, randomSeed: 1), publisher, options ?? new SimulatorOptions(),
            controller, time, NullLogger<SessionRunner>.Instance);
        return (runner, controller, publisher, time);
    }

    [Fact]
    public async Task Publishes_baseline_batch_then_delta_batches_every_five_ticks()
    {
        var (runner, controller, publisher, time) = Create();
        var session = controller.Enable(null).Session!;
        var run = runner.RunAsync(session, CancellationToken.None);

        await AdvanceSeconds(time, 9); // ticks at t=0..9 → 10 samples → 2 batches
        await Until(() => publisher.Batches.Count == 2);

        var batches = publisher.Batches;
        Assert.Equal(new[] { 0L, 1L }, batches.Select(b => b.Properties!.BatchSeq));
        Assert.All(batches, b => Assert.Equal(session.SessionId.ToString(), b.Properties!.SessionId));
        Assert.NotNull(batches[0].Samples![0].Body);
        Assert.All(batches[0].Samples!.Skip(1), s => Assert.NotNull(s.Changes));
        Assert.All(batches[1].Samples!, s => Assert.NotNull(s.Changes));

        controller.Disable();
        await run;
    }

    [Fact]
    public async Task Disable_flushes_partial_batch_and_ends_run()
    {
        var (runner, controller, publisher, time) = Create();
        var session = controller.Enable(null).Session!;
        var run = runner.RunAsync(session, CancellationToken.None);

        await AdvanceSeconds(time, 6); // 7 samples: batch 0 (5) + 2 pending
        await Until(() => publisher.Batches.Count == 1);
        controller.Disable();
        await run;

        var last = publisher.Batches[^1];
        Assert.Equal(1, last.Properties!.BatchSeq);
        Assert.Equal(2, last.Samples!.Count);
    }

    [Fact]
    public async Task Session_expires_after_duration_and_clears_controller()
    {
        var (runner, controller, _, time) = Create();
        var session = controller.Enable("""{"sessionDurationMinutes": 10}""").Session!;
        var run = runner.RunAsync(session, CancellationToken.None);

        time.Advance(TimeSpan.FromMinutes(10) + TimeSpan.FromSeconds(1));
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Null(controller.Active);
    }

    [Fact]
    public async Task Drop_every_fault_is_applied()
    {
        var (runner, controller, publisher, time) = Create(new SimulatorOptions { DropEvery = 2 });
        var session = controller.Enable(null).Session!;
        var run = runner.RunAsync(session, CancellationToken.None);

        await AdvanceSeconds(time, 19); // 20 samples → batches 0..3, 1 and 3 dropped
        await Until(() => publisher.Batches.Count == 2);
        controller.Disable();
        await run;

        Assert.Equal(new[] { 0L, 2L }, publisher.Batches.Select(b => b.Properties!.BatchSeq));
    }
}
```

- [ ] **Step 3: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~SessionRunner"`
Expected: FAIL — `The type or namespace name 'SessionRunner' could not be found`.

- [ ] **Step 4: Implement** `src/Shape.GatewaySimulator/SessionRunner.cs`

```csharp
using System.Threading.Channels;
using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>
/// Runs one fast-polling session: poll every second, encode, batch, publish. Publishing happens on a separate
/// consumer so slow retries never delay the 1-second sampling.
/// </summary>
public sealed class SessionRunner(
    Func<PumpDevice> deviceFactory,
    IBatchPublisher publisher,
    SimulatorOptions options,
    SessionController controller,
    TimeProvider time,
    ILogger<SessionRunner> logger)
{
    public async Task RunAsync(FastPollingSession session, CancellationToken hostStopping)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(session.Cts.Token, hostStopping);
        using var expiry = new CancellationTokenSource(TimeSpan.FromMinutes(session.DurationMinutes), time);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(linked.Token, expiry.Token);

        var device = deviceFactory();
        var encoder = new DeltaEncoder();
        var buffer = new BatchBuffer(session.SessionId.ToString());
        var faults = new FaultInjector(options.DropEvery, options.Reorder);
        var outbox = Channel.CreateUnbounded<TelemetryBatch>(new UnboundedChannelOptions { SingleReader = true });
        var consumer = Task.Run(async () =>
        {
            await foreach (var batch in outbox.Reader.ReadAllAsync())
                await publisher.PublishAsync(batch, CancellationToken.None);
        });

        logger.LogInformation("Session {SessionId} started ({Minutes} min)", session.SessionId, session.DurationMinutes);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1), time);
        try
        {
            do
            {
                var sample = encoder.Encode(device.Read(), time.GetUtcNow());
                if (buffer.Add(sample) is { } batch)
                    foreach (var b in faults.Process(batch)) outbox.Writer.TryWrite(b);
            }
            while (await timer.WaitForNextTickAsync(stop.Token));
        }
        catch (OperationCanceledException)
        {
            // disable, expiry, or host shutdown
        }
        finally
        {
            controller.Clear(session);
            if (buffer.Flush() is { } last)
                foreach (var b in faults.Process(last)) outbox.Writer.TryWrite(b);
            foreach (var b in faults.Drain()) outbox.Writer.TryWrite(b);
            outbox.Writer.Complete();
            await consumer;

            var reason = session.Cts.IsCancellationRequested ? "disabled"
                : expiry.IsCancellationRequested ? "expired"
                : "host stopping";
            logger.LogInformation("Session {SessionId} ended: {Reason}", session.SessionId, reason);
        }
    }
}
```

- [ ] **Step 5: Run tests — expect pass**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~SessionRunner"`
Expected: `Passed!  - Failed: 0, Passed: 4`.

- [ ] **Step 6: Replace** `src/Shape.GatewaySimulator/Program.cs`

```csharp
using Shape.Contracts;
using Shape.GatewaySimulator;

var options = SimulatorOptions.Parse(args);

var builder = WebApplication.CreateBuilder();
builder.WebHost.UseUrls(options.Urls);
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.WithOrigins("http://localhost:5173").AllowAnyHeader().AllowAnyMethod()));
builder.Services.AddHttpClient();
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<SessionController>();
builder.Services.AddSingleton<IBatchPublisher>(sp => new EventGridPublisher(
    sp.GetRequiredService<IHttpClientFactory>().CreateClient(),
    options.FunctionsUrl,
    EventGridPublisher.DefaultRetryDelays,
    sp.GetRequiredService<ILogger<EventGridPublisher>>()));
var seed = PumpSeed.LoadDefault();
builder.Services.AddSingleton<Func<PumpDevice>>(() => new PumpDevice(seed, options.RandomSeed));
builder.Services.AddSingleton<SessionRunner>();

var app = builder.Build();
app.UseCors();

app.MapPost("/fast-polling/enable", async (HttpRequest request, SessionController controller, SessionRunner runner,
    IHostApplicationLifetime lifetime) =>
{
    using var reader = new StreamReader(request.Body);
    var result = controller.Enable(await reader.ReadToEndAsync());
    if (result.Session is { } session)
        _ = Task.Run(() => runner.RunAsync(session, lifetime.ApplicationStopping));
    return Results.Json(result.Body, statusCode: result.StatusCode);
});

app.MapPost("/fast-polling/disable", (SessionController controller) =>
{
    var result = controller.Disable();
    return Results.Json(result.Body, statusCode: result.StatusCode);
});

app.MapGet("/fast-polling/status", (SessionController controller) => Results.Json(controller.Status()));

app.Run();
```

- [ ] **Step 7: Manual check of the control API** (Functions host not running yet — publish failures are expected and logged)

```powershell
dotnet build src/Shape.GatewaySimulator
# run the built dll (not `dotnet run`) so Stop-Process kills the server itself, freeing port 5100
$sim = Start-Process dotnet -ArgumentList 'src/Shape.GatewaySimulator/bin/Debug/net8.0/Shape.GatewaySimulator.dll','--seed','42' -PassThru
Start-Sleep 3
Invoke-RestMethod -Method Post http://localhost:5100/fast-polling/enable -Body '{"sessionDurationMinutes":10}' -ContentType application/json
Invoke-RestMethod http://localhost:5100/fast-polling/status
try { Invoke-RestMethod -Method Post http://localhost:5100/fast-polling/enable } catch { $_.Exception.Response.StatusCode }
Invoke-RestMethod -Method Post http://localhost:5100/fast-polling/disable
Stop-Process $sim.Id
```
Expected: enable → `result: accepted`, `durationMinutes: 10`; status → `active: True`; second enable → `Conflict`; disable → `result: accepted`. Simulator console shows `Batch 0 attempt 1 failed: ... refused` warnings.

- [ ] **Step 8: Run full unit suite and commit**

Run: `dotnet test tests/Shape.Tests`
Expected: all pass.

```powershell
git add src/Shape.GatewaySimulator tests/Shape.Tests
git commit -m "feat(simulator): add session runner and control API host"
```

---

### Task 7: SnapshotFolder (pure folding)

**Files:**
- Create: `src/Shape.Ingest/Shape.Ingest.csproj` (template), `src/Shape.Ingest/SessionState.cs`, `src/Shape.Ingest/SnapshotFolder.cs`
- Test: `tests/Shape.Tests/Ingest/SnapshotFolderTests.cs`

**Interfaces:**
- Consumes: `TelemetryBatch`, `Sample`, `FoldedBatch`, `FoldedSample` (Task 1).
- Produces:
  - `Shape.Ingest.SessionState { string SessionId; Dictionary<string, JsonElement> Snapshot; long LastBatchSeq; bool Partial; string? ETag }`
  - `Shape.Ingest.FoldOutcome { Normal, Gap, Stale, NewSession, Orphan, Invalid }`
  - `Shape.Ingest.FoldResult(FoldOutcome Outcome, SessionState? NewState, FoldedBatch? Folded)`
  - `Shape.Ingest.SnapshotFolder.Fold(SessionState? state, TelemetryBatch batch) → FoldResult`

- [ ] **Step 1: Create project**

```powershell
dotnet new classlib -n Shape.Ingest -o src/Shape.Ingest -f net8.0
Remove-Item src/Shape.Ingest/Class1.cs
dotnet sln add src/Shape.Ingest
dotnet add src/Shape.Ingest reference src/Shape.Contracts
dotnet add tests/Shape.Tests reference src/Shape.Ingest
```

- [ ] **Step 2: Write failing tests** `tests/Shape.Tests/Ingest/SnapshotFolderTests.cs`

```csharp
using System.Text.Json;
using Shape.Contracts;
using Shape.Ingest;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Ingest;

public class SnapshotFolderTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-09-23T12:00:00Z");

    private static Sample BodySample(int sec, params (string, object)[] fields) =>
        new() { SnapshotTimestampUtc = T0.AddSeconds(sec), Body = Map(fields) };

    private static Sample Delta(int sec, params (string, object)[] fields) =>
        new() { SnapshotTimestampUtc = T0.AddSeconds(sec), Changes = Map(fields) };

    private static TelemetryBatch Batch(long seq, params Sample[] samples) => new()
    {
        Properties = new BatchProperties { SessionId = "s1", BatchSeq = seq, BatchStartUtc = samples[0].SnapshotTimestampUtc },
        Samples = samples,
    };

    private static SessionState State(long lastSeq, params (string, object)[] snapshot) => new()
    {
        SessionId = "s1", LastBatchSeq = lastSeq, Snapshot = Map(snapshot), ETag = "etag-1",
    };

    private static int Int(FoldedSample s, string key) => s.Values[key].GetInt32();

    [Fact]
    public void New_session_folds_body_then_deltas_into_full_samples()
    {
        var batch = Batch(0,
            BodySample(0, ("A", 1), ("B", 10)),
            Delta(1, ("A", 2)),
            Delta(2),
            Delta(3, ("B", 11)),
            Delta(4, ("A", 3), ("B", 12)));

        var result = SnapshotFolder.Fold(null, batch);

        Assert.Equal(FoldOutcome.NewSession, result.Outcome);
        var f = result.Folded!;
        Assert.Equal("s1", f.SessionId);
        Assert.Equal(0, f.BatchSeq);
        Assert.False(f.Gap);
        Assert.False(f.Partial);
        Assert.Equal(new[] { 1, 2, 2, 2, 3 }, f.Samples.Select(s => Int(s, "A")));
        Assert.Equal(new[] { 10, 10, 10, 11, 12 }, f.Samples.Select(s => Int(s, "B")));
        Assert.Equal(new[] { "A", "B" }, f.Samples[0].Changed);
        Assert.Equal(new[] { "A" }, f.Samples[1].Changed);
        Assert.Empty(f.Samples[2].Changed);
        Assert.Equal(T0.AddSeconds(3), f.Samples[3].Ts);
        Assert.Equal(0, result.NewState!.LastBatchSeq);
        Assert.Equal(3, result.NewState.Snapshot["A"].GetInt32());
        Assert.Null(result.NewState.ETag);
    }

    [Fact]
    public void Next_batch_in_sequence_is_Normal_and_folds_onto_stored_snapshot()
    {
        var result = SnapshotFolder.Fold(State(4, ("A", 1), ("B", 10)), Batch(5, Delta(0, ("A", 5))));

        Assert.Equal(FoldOutcome.Normal, result.Outcome);
        Assert.False(result.Folded!.Gap);
        Assert.Equal(5, Int(result.Folded.Samples[0], "A"));
        Assert.Equal(10, Int(result.Folded.Samples[0], "B"));
        Assert.Equal(5, result.NewState!.LastBatchSeq);
        Assert.Equal("etag-1", result.NewState.ETag);
    }

    [Fact]
    public void Skipped_seq_is_Gap_but_still_folds()
    {
        var result = SnapshotFolder.Fold(State(4, ("A", 1)), Batch(7, Delta(0, ("A", 9))));

        Assert.Equal(FoldOutcome.Gap, result.Outcome);
        Assert.True(result.Folded!.Gap);
        Assert.Equal(9, Int(result.Folded.Samples[0], "A"));
        Assert.Equal(7, result.NewState!.LastBatchSeq);
    }

    [Theory]
    [InlineData(4)]
    [InlineData(2)]
    public void Old_or_duplicate_seq_is_Stale_with_no_output(long seq)
    {
        var result = SnapshotFolder.Fold(State(4, ("A", 1)), Batch(seq, Delta(0, ("A", 9))));

        Assert.Equal(FoldOutcome.Stale, result.Outcome);
        Assert.Null(result.Folded);
        Assert.Null(result.NewState);
    }

    [Fact]
    public void Unknown_session_without_body_is_Orphan_partial_gap()
    {
        var result = SnapshotFolder.Fold(null, Batch(12, Delta(0, ("A", 1)), Delta(1, ("B", 2))));

        Assert.Equal(FoldOutcome.Orphan, result.Outcome);
        Assert.True(result.Folded!.Gap);
        Assert.True(result.Folded.Partial);
        Assert.Single(result.Folded.Samples[0].Values);
        Assert.Equal(2, result.Folded.Samples[1].Values.Count);
        Assert.True(result.NewState!.Partial);
    }

    [Fact]
    public void Partial_flag_persists_on_later_batches()
    {
        var state = State(12, ("A", 1));
        state.Partial = true;

        var result = SnapshotFolder.Fold(state, Batch(13, Delta(0)));

        Assert.Equal(FoldOutcome.Normal, result.Outcome);
        Assert.True(result.Folded!.Partial);
        Assert.True(result.NewState!.Partial);
    }

    [Fact]
    public void Folds_partial_final_batch()
    {
        var result = SnapshotFolder.Fold(State(4, ("A", 1)), Batch(5, Delta(0, ("A", 2)), Delta(1)));

        Assert.Equal(2, result.Folded!.Samples.Count);
        Assert.Equal(new[] { 2, 2 }, result.Folded.Samples.Select(s => Int(s, "A")));
    }

    [Fact]
    public void Output_samples_are_independent_copies()
    {
        var state = State(0, ("A", 1));
        var result = SnapshotFolder.Fold(state, Batch(1, Delta(0, ("A", 2)), Delta(1, ("A", 3))));

        result.Folded!.Samples[0].Values["A"] = J(999);

        Assert.Equal(3, Int(result.Folded.Samples[1], "A"));
        Assert.Equal(3, result.NewState!.Snapshot["A"].GetInt32());
        Assert.Equal(1, state.Snapshot["A"].GetInt32()); // input state not mutated
    }

    [Fact]
    public void Invalid_batches_are_rejected()
    {
        var noProps = new TelemetryBatch { Samples = [Delta(0)] };
        var noSamples = new TelemetryBatch { Properties = new BatchProperties { SessionId = "s1" }, Samples = [] };
        var both = Batch(0, new Sample { Body = Map(("A", 1)), Changes = Map(("A", 1)) });
        var neither = Batch(0, new Sample { SnapshotTimestampUtc = T0 });
        var noSession = new TelemetryBatch { Properties = new BatchProperties { SessionId = "" }, Samples = [Delta(0)] };

        foreach (var b in new[] { noProps, noSamples, both, neither, noSession })
            Assert.Equal(FoldOutcome.Invalid, SnapshotFolder.Fold(null, b).Outcome);
    }
}
```

- [ ] **Step 3: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~SnapshotFolder"`
Expected: FAIL — `The type or namespace name 'Ingest' does not exist`.

- [ ] **Step 4: Implement**

`src/Shape.Ingest/SessionState.cs`:
```csharp
using System.Text.Json;

namespace Shape.Ingest;

/// <summary>Per-session folded state persisted between batches.</summary>
public sealed class SessionState
{
    public required string SessionId { get; init; }
    public Dictionary<string, JsonElement> Snapshot { get; init; } = new();
    public long LastBatchSeq { get; init; }
    /// <summary>True when the session started without a Body (orphan); stays true for the session's lifetime.</summary>
    public bool Partial { get; set; }
    /// <summary>Storage concurrency token; null means "not yet stored".</summary>
    public string? ETag { get; set; }
}
```

`src/Shape.Ingest/SnapshotFolder.cs`:
```csharp
using System.Text.Json;
using Shape.Contracts;

namespace Shape.Ingest;

public enum FoldOutcome { Normal, Gap, Stale, NewSession, Orphan, Invalid }

public sealed record FoldResult(FoldOutcome Outcome, SessionState? NewState, FoldedBatch? Folded);

/// <summary>Pure fold of one batch onto the stored session state (spec §4.2–4.3). Never mutates its inputs.</summary>
public static class SnapshotFolder
{
    public static FoldResult Fold(SessionState? state, TelemetryBatch batch)
    {
        if (!IsValid(batch)) return new(FoldOutcome.Invalid, null, null);

        var props = batch.Properties!;
        var samples = batch.Samples!;
        FoldOutcome outcome;
        bool gap, partial;
        Dictionary<string, JsonElement> snapshot;

        if (state is null)
        {
            var hasBody = samples[0].Body is not null;
            outcome = hasBody ? FoldOutcome.NewSession : FoldOutcome.Orphan;
            gap = partial = !hasBody;
            snapshot = new();
        }
        else
        {
            if (props.BatchSeq <= state.LastBatchSeq) return new(FoldOutcome.Stale, null, null);
            gap = props.BatchSeq > state.LastBatchSeq + 1;
            outcome = gap ? FoldOutcome.Gap : FoldOutcome.Normal;
            partial = state.Partial;
            snapshot = new(state.Snapshot);
        }

        var folded = new List<FoldedSample>(samples.Count);
        foreach (var sample in samples)
        {
            IReadOnlyList<string> changed;
            if (sample.Body is not null)
            {
                snapshot = new(sample.Body);
                changed = sample.Body.Keys.ToList();
            }
            else
            {
                foreach (var (key, value) in sample.Changes!) snapshot[key] = value;
                changed = sample.Changes.Keys.ToList();
            }
            folded.Add(new FoldedSample { Ts = sample.SnapshotTimestampUtc, Values = new(snapshot), Changed = changed });
        }

        var newState = new SessionState
        {
            SessionId = props.SessionId,
            Snapshot = snapshot,
            LastBatchSeq = props.BatchSeq,
            Partial = partial,
            ETag = state?.ETag,
        };
        var result = new FoldedBatch
        {
            SessionId = props.SessionId,
            BatchSeq = props.BatchSeq,
            Gap = gap,
            Partial = partial,
            Samples = folded,
        };
        return new(outcome, newState, result);
    }

    private static bool IsValid(TelemetryBatch batch) =>
        batch.Properties is { SessionId.Length: > 0 }
        && batch.Samples is { Count: > 0 }
        && batch.Samples.All(s => (s.Body is null) != (s.Changes is null));
}
```

- [ ] **Step 5: Run tests — expect pass**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~SnapshotFolder"`
Expected: `Passed!  - Failed: 0, Passed: 10`.

- [ ] **Step 6: Commit**

```powershell
git add src/Shape.Ingest tests/Shape.Tests Shape5Sec.sln
git commit -m "feat(ingest): add pure snapshot folder with gap/stale/orphan classification"
```

---

### Task 8: Snapshot store (Azurite) and TelemetryIngestor

**Files:**
- Create: `src/Shape.Ingest/ISnapshotStore.cs`, `src/Shape.Ingest/TableSnapshotStore.cs`, `src/Shape.Ingest/TelemetryIngestor.cs`
- Test: `tests/Shape.Tests/Ingest/InMemorySnapshotStore.cs`, `tests/Shape.Tests/Ingest/TelemetryIngestorTests.cs`, `tests/Shape.Tests/Ingest/TableSnapshotStoreTests.cs`

**Interfaces:**
- Consumes: `SnapshotFolder`, `SessionState`, `FoldOutcome` (Task 7); `TelemetryBatch`, `ShapeJson`, `FoldedBatch` (Task 1).
- Produces:
  - `ISnapshotStore.LoadAsync(string sessionId, CancellationToken ct) → Task<SessionState?>`; `SaveAsync(SessionState state, CancellationToken ct) → Task` (sets `state.ETag`; throws `SnapshotConflictException` on concurrency conflict)
  - `SnapshotConflictException : Exception`
  - `TableSnapshotStore(TableClient table) : ISnapshotStore`; `const string DefaultTableName = "sessionsnapshots"`
  - `TelemetryIngestor(ISnapshotStore store, ILogger<TelemetryIngestor> logger)`; `IngestAsync(string dataJson, CancellationToken ct) → Task<FoldedBatch?>`; `const int MaxSaveAttempts = 3`

- [ ] **Step 1: Install Azurite and add packages**

```powershell
npm install -g azurite
dotnet add src/Shape.Ingest package Azure.Data.Tables
dotnet add src/Shape.Ingest package Microsoft.Extensions.Logging.Abstractions
```

- [ ] **Step 2: Write the in-memory fake** `tests/Shape.Tests/Ingest/InMemorySnapshotStore.cs`

```csharp
using System.Text.Json;
using Shape.Ingest;

namespace Shape.Tests.Ingest;

/// <summary>Store fake with real ETag semantics and injectable conflicts.</summary>
public sealed class InMemorySnapshotStore : ISnapshotStore
{
    private readonly Dictionary<string, (string Json, long Seq, bool Partial, string ETag)> _rows = new();
    private int _version;

    public int ConflictsToInject { get; set; }
    public int SaveCalls { get; private set; }

    public Task<SessionState?> LoadAsync(string sessionId, CancellationToken ct)
    {
        if (!_rows.TryGetValue(sessionId, out var row)) return Task.FromResult<SessionState?>(null);
        return Task.FromResult<SessionState?>(new SessionState
        {
            SessionId = sessionId,
            Snapshot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(row.Json)!,
            LastBatchSeq = row.Seq,
            Partial = row.Partial,
            ETag = row.ETag,
        });
    }

    public Task SaveAsync(SessionState state, CancellationToken ct)
    {
        SaveCalls++;
        if (ConflictsToInject > 0)
        {
            ConflictsToInject--;
            throw new SnapshotConflictException(state.SessionId);
        }
        var exists = _rows.TryGetValue(state.SessionId, out var current);
        if (exists ? current.ETag != state.ETag : state.ETag is not null)
            throw new SnapshotConflictException(state.SessionId);

        var etag = $"v{++_version}";
        _rows[state.SessionId] = (JsonSerializer.Serialize(state.Snapshot), state.LastBatchSeq, state.Partial, etag);
        state.ETag = etag;
        return Task.CompletedTask;
    }
}
```

- [ ] **Step 3: Write failing ingestor tests** `tests/Shape.Tests/Ingest/TelemetryIngestorTests.cs`

```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.Contracts;
using Shape.Ingest;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Ingest;

public class TelemetryIngestorTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-09-23T12:00:00Z");

    private static string Json(long seq, bool withBody, string session = "s1")
    {
        var first = withBody
            ? new Sample { SnapshotTimestampUtc = T0, Body = Map(("A", 1), ("B", 2)) }
            : new Sample { SnapshotTimestampUtc = T0, Changes = Map(("A", (int)seq)) };
        var samples = new List<Sample> { first };
        for (var i = 1; i < 5; i++) samples.Add(new Sample { SnapshotTimestampUtc = T0.AddSeconds(i), Changes = new() });
        return JsonSerializer.Serialize(new TelemetryBatch
        {
            Properties = new BatchProperties { SessionId = session, BatchSeq = seq, BatchStartUtc = T0 },
            Samples = samples,
        }, ShapeJson.Options);
    }

    private static TelemetryIngestor Ingestor(ISnapshotStore store) => new(store, NullLogger<TelemetryIngestor>.Instance);

    [Fact]
    public async Task First_batch_is_folded_saved_and_returned()
    {
        var store = new InMemorySnapshotStore();

        var folded = await Ingestor(store).IngestAsync(Json(0, withBody: true), CancellationToken.None);

        Assert.NotNull(folded);
        Assert.Equal(5, folded!.Samples.Count);
        Assert.Equal(0, (await store.LoadAsync("s1", CancellationToken.None))!.LastBatchSeq);
    }

    [Fact]
    public async Task State_survives_across_ingestor_instances()
    {
        var store = new InMemorySnapshotStore();
        await Ingestor(store).IngestAsync(Json(0, withBody: true), CancellationToken.None);

        var folded = await Ingestor(store).IngestAsync(Json(1, withBody: false), CancellationToken.None);

        Assert.False(folded!.Gap);
        Assert.Equal(2, folded.Samples[0].Values["B"].GetInt32()); // B carried over from the Body
    }

    [Fact]
    public async Task Duplicate_delivery_is_dropped_without_push()
    {
        var store = new InMemorySnapshotStore();
        var ingestor = Ingestor(store);
        await ingestor.IngestAsync(Json(0, withBody: true), CancellationToken.None);
        await ingestor.IngestAsync(Json(1, withBody: false), CancellationToken.None);
        var savesBefore = store.SaveCalls;

        var again = await ingestor.IngestAsync(Json(1, withBody: false), CancellationToken.None);

        Assert.Null(again);
        Assert.Equal(savesBefore, store.SaveCalls);
        Assert.Equal(1, (await store.LoadAsync("s1", CancellationToken.None))!.LastBatchSeq);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("null")]
    [InlineData("{}")]
    [InlineData("""{"Properties":{"session-id":"s1","batchSeq":0},"Samples":[]}""")]
    public async Task Malformed_data_returns_null_without_throwing(string json)
    {
        var store = new InMemorySnapshotStore();

        Assert.Null(await Ingestor(store).IngestAsync(json, CancellationToken.None));
        Assert.Equal(0, store.SaveCalls);
    }

    [Fact]
    public async Task Conflict_is_retried_and_then_succeeds()
    {
        var store = new InMemorySnapshotStore { ConflictsToInject = 2 };

        var folded = await Ingestor(store).IngestAsync(Json(0, withBody: true), CancellationToken.None);

        Assert.NotNull(folded);
        Assert.Equal(3, store.SaveCalls);
    }

    [Fact]
    public async Task Third_conflict_propagates()
    {
        var store = new InMemorySnapshotStore { ConflictsToInject = 3 };

        await Assert.ThrowsAsync<SnapshotConflictException>(
            () => Ingestor(store).IngestAsync(Json(0, withBody: true), CancellationToken.None));
    }
}
```

- [ ] **Step 4: Write failing integration tests** `tests/Shape.Tests/Ingest/TableSnapshotStoreTests.cs`

```csharp
using Azure.Data.Tables;
using Shape.Ingest;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Ingest;

/// <summary>Requires Azurite: run `azurite --silent --location .azurite` first.</summary>
[Trait("Category", "Integration")]
public class TableSnapshotStoreTests
{
    private static TableSnapshotStore NewStore() =>
        new(new TableServiceClient("UseDevelopmentStorage=true").GetTableClient($"t{Guid.NewGuid():N}"));

    [Fact]
    public async Task Load_returns_null_for_unknown_session()
    {
        Assert.Null(await NewStore().LoadAsync("nope", CancellationToken.None));
    }

    [Fact]
    public async Task Insert_load_update_round_trip()
    {
        var store = NewStore();
        var state = new SessionState { SessionId = "s1", Snapshot = Map(("A", 1), ("Ip", "0.0.103.9")), LastBatchSeq = 0 };

        await store.SaveAsync(state, CancellationToken.None);
        Assert.NotNull(state.ETag);

        var loaded = (await store.LoadAsync("s1", CancellationToken.None))!;
        Assert.Equal(0, loaded.LastBatchSeq);
        Assert.Equal("0.0.103.9", loaded.Snapshot["Ip"].GetString());
        Assert.False(loaded.Partial);

        var next = new SessionState { SessionId = "s1", Snapshot = Map(("A", 2)), LastBatchSeq = 1, Partial = true, ETag = loaded.ETag };
        await store.SaveAsync(next, CancellationToken.None);

        var reloaded = (await store.LoadAsync("s1", CancellationToken.None))!;
        Assert.Equal(1, reloaded.LastBatchSeq);
        Assert.True(reloaded.Partial);
        Assert.Equal(2, reloaded.Snapshot["A"].GetInt32());
    }

    [Fact]
    public async Task Stale_etag_throws_conflict()
    {
        var store = NewStore();
        var state = new SessionState { SessionId = "s1", Snapshot = Map(("A", 1)) };
        await store.SaveAsync(state, CancellationToken.None);
        var staleEtag = state.ETag;
        await store.SaveAsync(new SessionState { SessionId = "s1", Snapshot = Map(("A", 2)), LastBatchSeq = 1, ETag = staleEtag }, CancellationToken.None);

        await Assert.ThrowsAsync<SnapshotConflictException>(() => store.SaveAsync(
            new SessionState { SessionId = "s1", Snapshot = Map(("A", 3)), LastBatchSeq = 2, ETag = staleEtag }, CancellationToken.None));
    }

    [Fact]
    public async Task Concurrent_first_insert_throws_conflict()
    {
        var store = NewStore();
        await store.SaveAsync(new SessionState { SessionId = "s1", Snapshot = Map(("A", 1)) }, CancellationToken.None);

        await Assert.ThrowsAsync<SnapshotConflictException>(() => store.SaveAsync(
            new SessionState { SessionId = "s1", Snapshot = Map(("A", 9)) }, CancellationToken.None));
    }
}
```

- [ ] **Step 5: Run tests — expect compile failure**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~Ingest"`
Expected: FAIL — `The type or namespace name 'ISnapshotStore' could not be found`.

- [ ] **Step 6: Implement**

`src/Shape.Ingest/ISnapshotStore.cs`:
```csharp
namespace Shape.Ingest;

public interface ISnapshotStore
{
    Task<SessionState?> LoadAsync(string sessionId, CancellationToken ct);

    /// <summary>
    /// Inserts when <c>state.ETag</c> is null, otherwise replaces only if the stored ETag still matches.
    /// Sets <c>state.ETag</c> to the new value. Throws <see cref="SnapshotConflictException"/> on conflict.
    /// </summary>
    Task SaveAsync(SessionState state, CancellationToken ct);
}

public sealed class SnapshotConflictException(string sessionId, Exception? inner = null)
    : Exception($"Snapshot for session '{sessionId}' was modified concurrently.", inner);
```

`src/Shape.Ingest/TableSnapshotStore.cs`:
```csharp
using System.Text.Json;
using Azure;
using Azure.Data.Tables;

namespace Shape.Ingest;

/// <summary>Table storage: PK = session id, RK = "state". Works against Azurite and Azure.</summary>
public sealed class TableSnapshotStore(TableClient table) : ISnapshotStore
{
    public const string DefaultTableName = "sessionsnapshots";
    private const string RowKey = "state";
    private int _tableReady;

    public async Task<SessionState?> LoadAsync(string sessionId, CancellationToken ct)
    {
        await EnsureTableAsync(ct);
        var response = await table.GetEntityIfExistsAsync<TableEntity>(sessionId, RowKey, cancellationToken: ct);
        if (!response.HasValue) return null;
        var e = response.Value!;
        return new SessionState
        {
            SessionId = sessionId,
            Snapshot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(e.GetString("SnapshotJson"))!,
            LastBatchSeq = e.GetInt64("LastBatchSeq") ?? 0,
            Partial = e.GetBoolean("Partial") ?? false,
            ETag = e.ETag.ToString(),
        };
    }

    public async Task SaveAsync(SessionState state, CancellationToken ct)
    {
        await EnsureTableAsync(ct);
        var entity = new TableEntity(state.SessionId, RowKey)
        {
            ["SnapshotJson"] = JsonSerializer.Serialize(state.Snapshot),
            ["LastBatchSeq"] = state.LastBatchSeq,
            ["Partial"] = state.Partial,
        };
        try
        {
            var response = state.ETag is null
                ? await table.AddEntityAsync(entity, ct)
                : await table.UpdateEntityAsync(entity, new ETag(state.ETag), TableUpdateMode.Replace, ct);
            state.ETag = response.Headers.ETag?.ToString();
        }
        catch (RequestFailedException ex) when (ex.Status is 409 or 412)
        {
            throw new SnapshotConflictException(state.SessionId, ex);
        }
    }

    private async Task EnsureTableAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _tableReady) == 1) return;
        await table.CreateIfNotExistsAsync(ct);
        Volatile.Write(ref _tableReady, 1);
    }
}
```

`src/Shape.Ingest/TelemetryIngestor.cs`:
```csharp
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Shape.Contracts;

namespace Shape.Ingest;

/// <summary>Load state → fold → save (optimistic, retried) → return what to push. Null means "push nothing".</summary>
public sealed class TelemetryIngestor(ISnapshotStore store, ILogger<TelemetryIngestor> logger)
{
    public const int MaxSaveAttempts = 3;

    public async Task<FoldedBatch?> IngestAsync(string dataJson, CancellationToken ct)
    {
        TelemetryBatch? batch;
        try
        {
            batch = JsonSerializer.Deserialize<TelemetryBatch>(dataJson, ShapeJson.Options);
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Discarding telemetry batch: data is not valid JSON");
            return null;
        }

        var sessionId = batch?.Properties?.SessionId;
        if (batch is null || string.IsNullOrEmpty(sessionId))
        {
            logger.LogError("Discarding telemetry batch: missing Properties/session-id");
            return null;
        }

        for (var attempt = 1; ; attempt++)
        {
            var state = await store.LoadAsync(sessionId, ct);
            var result = SnapshotFolder.Fold(state, batch);
            var seq = batch.Properties!.BatchSeq;

            switch (result.Outcome)
            {
                case FoldOutcome.Invalid:
                    logger.LogError("Discarding invalid batch {BatchSeq} for session {SessionId}", seq, sessionId);
                    return null;
                case FoldOutcome.Stale:
                    logger.LogWarning("Dropping stale batch {BatchSeq} for session {SessionId} (last {LastSeq})",
                        seq, sessionId, state?.LastBatchSeq);
                    return null;
                case FoldOutcome.Gap:
                    logger.LogWarning("Gap before batch {BatchSeq} for session {SessionId} (last {LastSeq})",
                        seq, sessionId, state?.LastBatchSeq);
                    break;
                case FoldOutcome.Orphan:
                    logger.LogWarning("Batch {BatchSeq} for unknown session {SessionId} has no Body; folding partial",
                        seq, sessionId);
                    break;
            }

            try
            {
                await store.SaveAsync(result.NewState!, ct);
                return result.Folded;
            }
            catch (SnapshotConflictException) when (attempt < MaxSaveAttempts)
            {
                logger.LogWarning("Snapshot conflict for session {SessionId}, attempt {Attempt}; retrying", sessionId, attempt);
            }
        }
    }
}
```

- [ ] **Step 7: Run unit tests — expect pass**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~TelemetryIngestor"`
Expected: `Passed!  - Failed: 0, Passed: 9`.

- [ ] **Step 8: Run integration tests against Azurite**

In a separate terminal: `azurite --silent --location .azurite` (leave running; Ctrl+C when done). Then:
```powershell
dotnet test tests/Shape.Tests --filter "Category=Integration"
```
Expected: `Passed!  - Failed: 0, Passed: 4`. Without Azurite running these fail with a connection-refused `RequestFailedException` — that is the expected signal, not a code bug.

- [ ] **Step 9: Commit**

```powershell
git add src/Shape.Ingest tests/Shape.Tests
git commit -m "feat(ingest): add Table snapshot store and ingest orchestration with conflict retry"
```

---

### Task 9: Function App (Event Grid trigger, SignalR output, negotiate)

**Files:**
- Create: `src/Shape.Functions/Shape.Functions.csproj`, `Program.cs`, `IngestTelemetryFunction.cs`, `NegotiateFunction.cs`, `host.json`, `local.settings.json`
- Test: `tests/Shape.Tests/Functions/IngestTelemetryFunctionTests.cs`

**Interfaces:**
- Consumes: `TelemetryIngestor`, `TableSnapshotStore`, `ISnapshotStore` (Task 8); `FoldedBatch` (Task 1); `InMemorySnapshotStore` (Task 8 tests).
- Produces:
  - Function `IngestTelemetry`: `IngestTelemetryFunction(TelemetryIngestor ingestor)`; `Run(EventGridEvent ev, CancellationToken ct) → Task<SignalRMessageAction?>`; `const string HubName = "telemetry"`, `const string Target = "batch"`
  - Function `negotiate`: `POST http://localhost:7071/api/negotiate` → SignalR connection info JSON

- [ ] **Step 1: Install the SignalR emulator**

```powershell
dotnet tool install -g Microsoft.Azure.SignalR.Emulator
asrs-emulator --help
```
Expected: help text listing `start` and `upstream` commands.

- [ ] **Step 2: Create the Functions project.** Write `src/Shape.Functions/Shape.Functions.csproj`:

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
    <None Update="host.json" CopyToOutputDirectory="PreserveNewest" />
    <None Update="local.settings.json" CopyToOutputDirectory="PreserveNewest" CopyToPublishDirectory="Never" />
  </ItemGroup>
</Project>
```

Then:
```powershell
dotnet add src/Shape.Functions package Microsoft.Azure.Functions.Worker
dotnet add src/Shape.Functions package Microsoft.Azure.Functions.Worker.Sdk
dotnet add src/Shape.Functions package Microsoft.Azure.Functions.Worker.Extensions.EventGrid
dotnet add src/Shape.Functions package Microsoft.Azure.Functions.Worker.Extensions.SignalRService
dotnet add src/Shape.Functions package Microsoft.Azure.Functions.Worker.Extensions.Http
dotnet add src/Shape.Functions reference src/Shape.Ingest
dotnet sln add src/Shape.Functions
dotnet add tests/Shape.Tests reference src/Shape.Functions
```

`src/Shape.Functions/host.json`:
```json
{
  "version": "2.0",
  "logging": {
    "logLevel": { "default": "Information", "Host.Results": "Error", "Function": "Information" }
  }
}
```

`src/Shape.Functions/local.settings.json` (committed on purpose — it only holds local emulator values; the AccessKey is the emulator's fixed public dev key):
```json
{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "dotnet-isolated",
    "SnapshotStorage": "UseDevelopmentStorage=true",
    "AzureSignalRConnectionString": "Endpoint=http://localhost;Port=8888;AccessKey=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGH;Version=1.0;"
  },
  "Host": {
    "LocalHttpPort": 7071,
    "CORS": "http://localhost:5173",
    "CORSCredentials": true
  }
}
```

- [ ] **Step 3: Write failing test** `tests/Shape.Tests/Functions/IngestTelemetryFunctionTests.cs`

```csharp
using Azure.Messaging.EventGrid;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.Contracts;
using Shape.Functions;
using Shape.Ingest;
using Shape.Tests.Ingest;

namespace Shape.Tests.Functions;

public class IngestTelemetryFunctionTests
{
    private const string EventJson = """
    {
      "id": "1", "eventType": "Shape.Telemetry.FastPollingBatch", "subject": "pumps/pump-001",
      "eventTime": "2026-09-23T12:00:05Z", "dataVersion": "1.0",
      "data": {
        "Properties": { "session-id": "s1", "telemetry-type": "fast-polling", "batchSeq": 0, "batchStartUtc": "2026-09-23T12:00:00Z" },
        "Samples": [
          { "snapshotTimestampUtc": "2026-09-23T12:00:00Z", "Body": { "OperatingPressurePSI": 54000, "MotorRunning": true } },
          { "snapshotTimestampUtc": "2026-09-23T12:00:01Z", "Changes": { "OperatingPressurePSI": 54360 } }
        ]
      }
    }
    """;

    private static IngestTelemetryFunction Function(ISnapshotStore store) =>
        new(new TelemetryIngestor(store, NullLogger<TelemetryIngestor>.Instance));

    [Fact]
    public async Task Real_event_grid_event_produces_batch_message()
    {
        var ev = EventGridEvent.Parse(BinaryData.FromString(EventJson));

        var action = await Function(new InMemorySnapshotStore()).Run(ev, CancellationToken.None);

        Assert.NotNull(action);
        Assert.Equal("batch", action!.Target);
        var folded = Assert.IsType<FoldedBatch>(Assert.Single(action.Arguments));
        Assert.Equal("s1", folded.SessionId);
        Assert.Equal(2, folded.Samples.Count);
        Assert.Equal(54360, folded.Samples[1].Values["OperatingPressurePSI"].GetInt32());
        Assert.True(folded.Samples[1].Values["MotorRunning"].GetBoolean());
    }

    [Fact]
    public async Task Stale_redelivery_produces_no_message()
    {
        var store = new InMemorySnapshotStore();
        var function = Function(store);
        await function.Run(EventGridEvent.Parse(BinaryData.FromString(EventJson)), CancellationToken.None);

        var second = await function.Run(EventGridEvent.Parse(BinaryData.FromString(EventJson)), CancellationToken.None);

        Assert.Null(second);
    }
}
```

- [ ] **Step 4: Run test — expect compile failure**

Run: `dotnet test tests/Shape.Tests --filter "FullyQualifiedName~IngestTelemetryFunction"`
Expected: FAIL — `The type or namespace name 'IngestTelemetryFunction' could not be found`.

- [ ] **Step 5: Implement**

`src/Shape.Functions/IngestTelemetryFunction.cs`:
```csharp
using Azure.Messaging.EventGrid;
using Microsoft.Azure.Functions.Worker;
using Shape.Ingest;

namespace Shape.Functions;

public sealed class IngestTelemetryFunction(TelemetryIngestor ingestor)
{
    public const string HubName = "telemetry";
    public const string Target = "batch";

    [Function("IngestTelemetry")]
    [SignalROutput(HubName = HubName, ConnectionStringSetting = "AzureSignalRConnectionString")]
    public async Task<SignalRMessageAction?> Run([EventGridTrigger] EventGridEvent ev, CancellationToken ct)
    {
        var folded = await ingestor.IngestAsync(ev.Data.ToString(), ct);
        return folded is null ? null : new SignalRMessageAction(Target, new object[] { folded });
    }
}
```

`src/Shape.Functions/NegotiateFunction.cs`:
```csharp
using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace Shape.Functions;

public sealed class NegotiateFunction
{
    [Function("negotiate")]
    public static HttpResponseData Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData request,
        [SignalRConnectionInfoInput(HubName = IngestTelemetryFunction.HubName, ConnectionStringSetting = "AzureSignalRConnectionString")]
        string connectionInfo)
    {
        var response = request.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/json");
        response.WriteString(connectionInfo);
        return response;
    }
}
```

`src/Shape.Functions/Program.cs`:
```csharp
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shape.Ingest;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        var connection = context.Configuration["SnapshotStorage"] ?? "UseDevelopmentStorage=true";
        services.AddSingleton<ISnapshotStore>(_ =>
            new TableSnapshotStore(new TableServiceClient(connection).GetTableClient(TableSnapshotStore.DefaultTableName)));
        services.AddSingleton<TelemetryIngestor>();
    })
    .Build();

host.Run();
```

- [ ] **Step 6: Run tests — expect pass**

Run: `dotnet test tests/Shape.Tests --filter "Category!=Integration"`
Expected: all pass, including the 2 new function tests.

If the `SignalRMessageAction(string, object[])` constructor does not exist in the installed package version, use `new SignalRMessageAction(Target) { Arguments = new object[] { folded } }` and re-run.

- [ ] **Step 7: Manual pipeline check with curl-style POST**

Open three terminals:
```powershell
azurite --silent --location .azurite           # terminal 1
asrs-emulator start                             # terminal 2 — note the printed connection string
cd src/Shape.Functions; func start              # terminal 3
```
If the emulator prints a connection string different from `local.settings.json`, copy it into `AzureSignalRConnectionString`.

Then in a fourth terminal:
```powershell
$event = '[{"id":"1","eventType":"Shape.Telemetry.FastPollingBatch","subject":"pumps/pump-001","eventTime":"2026-09-23T12:00:05Z","dataVersion":"1.0","data":{"Properties":{"session-id":"manual-1","telemetry-type":"fast-polling","batchSeq":0,"batchStartUtc":"2026-09-23T12:00:00Z"},"Samples":[{"snapshotTimestampUtc":"2026-09-23T12:00:00Z","Body":{"A":1}}]}}]'
Invoke-WebRequest -Method Post 'http://localhost:7071/runtime/webhooks/EventGrid?functionName=IngestTelemetry' -Headers @{ 'aeg-event-type' = 'Notification' } -ContentType 'application/json' -Body $event
Invoke-RestMethod -Method Post http://localhost:7071/api/negotiate
```
Expected: first call `StatusCode 202` (or 200); Functions log shows `Executed 'Functions.IngestTelemetry' (Succeeded`. Negotiate returns `url` pointing at `http://localhost:8888/client/?hub=telemetry` and an `accessToken`. Re-running the first POST logs `Dropping stale batch 0`.

- [ ] **Step 8: Commit**

```powershell
git add src/Shape.Functions tests/Shape.Tests Shape5Sec.sln
git commit -m "feat(functions): add Event Grid ingest function with SignalR output and negotiate"
```

---

### Task 10: Web scaffold and ReplayQueue

**Files:**
- Create: `web/` (Vite React TS template), `web/src/types.ts`, `web/src/ReplayQueue.ts`, `web/src/ReplayQueue.test.ts`, `web/src/rows.ts`, `web/src/rows.test.ts`
- Modify: `web/vite.config.ts`, `web/package.json` (scripts)

**Interfaces:**
- Consumes: push payload shape (spec §4.4).
- Produces:
  - `types.ts`: `FoldedSample { ts: string; values: Record<string, unknown>; changed: string[] }`, `FoldedBatch { sessionId; batchSeq; gap; partial; samples }`
  - `ReplayQueue(windowSeconds = 300, maxBacklog = 10, keepOnCatchUp = 5)`: `push(batch)`, `tick()`, `setWindow(seconds)`, getters `points: Point[]`, `sessionStarts: number[]`, `latest: FoldedSample | null`, `state: QueueState`, `queueDepth: number`, `sessionId: string | null`, `partial: boolean`, `windowSeconds: number`
  - `Point { t: number; sample: FoldedSample | null }`; `QueueState = 'waiting' | 'playing' | 'buffering'`
  - `rows.ts`: `toRows(points: Point[], keys: string[]) → Row[]` where `Row = { t: number } & Record<string, number | null>`

- [ ] **Step 1: Scaffold**

```powershell
npm create vite@latest web -- --template react-ts --no-interactive
cd web
npm install
npm install recharts @microsoft/signalr
npm install -D vitest
cd ..
```
If the create command prompts anyway, answer: no rolldown, do not install/start.

Replace `web/vite.config.ts`:
```ts
/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
  server: { port: 5173, strictPort: true },
  test: { environment: 'node' },
})
```

In `web/package.json` `"scripts"`, add `"test": "vitest run"`.

Delete template leftovers: `Remove-Item web/src/App.css, web/src/assets -Recurse`.

- [ ] **Step 2: Write types** `web/src/types.ts`

```ts
export type FoldedSample = {
  ts: string
  values: Record<string, unknown>
  changed: string[]
}

export type FoldedBatch = {
  sessionId: string
  batchSeq: number
  gap: boolean
  partial: boolean
  samples: FoldedSample[]
}
```

- [ ] **Step 3: Write failing tests** `web/src/ReplayQueue.test.ts`

```ts
import { describe, expect, it } from 'vitest'
import { ReplayQueue } from './ReplayQueue'
import type { FoldedBatch } from './types'

const T0 = Date.parse('2026-09-23T12:00:00Z')

function batch(seq: number, opts: { session?: string; n?: number; gap?: boolean; startSec?: number } = {}): FoldedBatch {
  const n = opts.n ?? 5
  const start = opts.startSec ?? seq * 5
  return {
    sessionId: opts.session ?? 's1',
    batchSeq: seq,
    gap: opts.gap ?? false,
    partial: false,
    samples: Array.from({ length: n }, (_, i) => ({
      ts: new Date(T0 + (start + i) * 1000).toISOString(),
      values: { P: start + i },
      changed: ['P'],
    })),
  }
}

const values = (q: ReplayQueue) => q.points.map(p => (p.sample ? (p.sample.values.P as number) : null))

describe('ReplayQueue', () => {
  it('waits before the first batch', () => {
    const q = new ReplayQueue()
    q.tick()
    expect(q.state).toBe('waiting')
    expect(q.points).toHaveLength(0)
  })

  it('plays one sample per tick', () => {
    const q = new ReplayQueue()
    q.push(batch(0))
    expect(q.queueDepth).toBe(5)
    q.tick()
    q.tick()
    expect(values(q)).toEqual([0, 1])
    expect(q.queueDepth).toBe(3)
    expect(q.state).toBe('playing')
    expect(q.latest?.values.P).toBe(1)
  })

  it('goes buffering when starved and resumes', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 1 }))
    q.tick()
    q.tick()
    expect(q.state).toBe('buffering')
    expect(q.points).toHaveLength(1)
    q.push(batch(1, { n: 1, startSec: 1 }))
    q.tick()
    expect(q.state).toBe('playing')
    expect(values(q)).toEqual([0, 1])
  })

  it('dedupes by session and batchSeq', () => {
    const q = new ReplayQueue()
    q.push(batch(0))
    q.push(batch(0))
    expect(q.queueDepth).toBe(5)
  })

  it('catches up on backlog, keeping the newest 5 queued', () => {
    const q = new ReplayQueue()
    q.push(batch(0))
    q.push(batch(1))
    q.push(batch(2)) // 15 queued > 10
    q.tick()
    expect(q.points).toHaveLength(11) // 10 drained + 1 played
    expect(q.queueDepth).toBe(4)
  })

  it('inserts a null break before a gap batch', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 1 }))
    q.push(batch(3, { n: 1, startSec: 15, gap: true }))
    q.tick()
    q.tick()
    expect(values(q)).toEqual([0, null, 15])
    expect(q.points[1].t).toBe(q.points[2].t - 1)
  })

  it('does not insert a leading null for a gap on the very first batch', () => {
    const q = new ReplayQueue()
    q.push(batch(7, { n: 1, gap: true }))
    q.tick()
    expect(values(q)).toEqual([35])
  })

  it('records a session start marker only for batchSeq 0 and breaks the line between sessions', () => {
    const q = new ReplayQueue()
    q.push(batch(4, { n: 1, session: 'old' })) // joined mid-session: no marker
    q.push(batch(0, { n: 1, session: 'new', startSec: 100 }))
    q.tick()
    q.tick()
    expect(q.sessionStarts).toEqual([T0 + 100_000])
    expect(values(q)).toEqual([20, null, 100])
    expect(q.sessionId).toBe('new')
  })

  it('ignores late batches from a superseded session', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 1, session: 'a' }))
    q.push(batch(0, { n: 1, session: 'b', startSec: 50 }))
    q.push(batch(1, { n: 1, session: 'a', startSec: 5 }))
    expect(q.queueDepth).toBe(2)
    expect(q.sessionId).toBe('b')
  })

  it('plays partial batches sample by sample', () => {
    const q = new ReplayQueue()
    q.push(batch(0, { n: 2 }))
    q.tick()
    q.tick()
    q.tick()
    expect(values(q)).toEqual([0, 1])
    expect(q.state).toBe('buffering')
  })

  it('trims points and markers outside the window', () => {
    const q = new ReplayQueue(10)
    q.push(batch(0))
    q.push(batch(1))
    q.push(batch(2, { n: 5 }))
    for (let i = 0; i < 15; i++) q.tick()
    const ts = q.points.map(p => p.t)
    const latest = Date.parse(q.latest!.ts)
    expect(Math.min(...ts)).toBeGreaterThanOrEqual(latest - 10_000)
    expect(q.sessionStarts).toEqual([])
  })

  it('exposes partial flag of the current session', () => {
    const q = new ReplayQueue()
    q.push({ ...batch(3), partial: true })
    expect(q.partial).toBe(true)
  })
})
```

`web/src/rows.test.ts`:
```ts
import { describe, expect, it } from 'vitest'
import { toRows } from './rows'
import type { Point } from './ReplayQueue'

describe('toRows', () => {
  it('maps numeric values and turns gaps and non-numbers into null', () => {
    const points: Point[] = [
      { t: 1, sample: { ts: '', values: { A: 5, B: true }, changed: [] } },
      { t: 2, sample: null },
    ]
    expect(toRows(points, ['A', 'B', 'C'])).toEqual([
      { t: 1, A: 5, B: null, C: null },
      { t: 2, A: null, B: null, C: null },
    ])
  })
})
```

- [ ] **Step 4: Run tests — expect failure**

Run: `cd web; npm test; cd ..`
Expected: FAIL — `Failed to resolve import "./ReplayQueue"`.

- [ ] **Step 5: Implement** `web/src/ReplayQueue.ts`

```ts
import type { FoldedBatch, FoldedSample } from './types'

export type Point = { t: number; sample: FoldedSample | null } // sample null = line break
export type QueueState = 'waiting' | 'playing' | 'buffering'

type Item = { sample: FoldedSample; breakBefore: boolean; sessionStart: boolean }

/**
 * Buffers 5-sample batches and releases one sample per tick (1 s), so the chart moves second by second.
 * Pure: the caller owns the timer.
 */
export class ReplayQueue {
  private items: Item[] = []
  private seen = new Set<string>()
  private retired = new Set<string>()
  private current: string | null = null
  private currentPartial = false
  private _points: Point[] = []
  private _sessionStarts: number[] = []
  private _latest: FoldedSample | null = null
  private _state: QueueState = 'waiting'
  private started = false
  private _windowSeconds: number

  private readonly maxBacklog: number
  private readonly keepOnCatchUp: number

  // No TS parameter properties: the Vite template enables `erasableSyntaxOnly`.
  constructor(windowSeconds = 300, maxBacklog = 10, keepOnCatchUp = 5) {
    this._windowSeconds = windowSeconds
    this.maxBacklog = maxBacklog
    this.keepOnCatchUp = keepOnCatchUp
  }

  push(batch: FoldedBatch): void {
    const key = `${batch.sessionId}:${batch.batchSeq}`
    if (this.seen.has(key) || this.retired.has(batch.sessionId) || batch.samples.length === 0) return
    this.seen.add(key)

    const newSession = batch.sessionId !== this.current
    if (newSession) {
      if (this.current) this.retired.add(this.current)
      this.current = batch.sessionId
    }
    this.currentPartial = batch.partial

    batch.samples.forEach((sample, i) =>
      this.items.push({
        sample,
        breakBefore: i === 0 && (batch.gap || newSession),
        sessionStart: i === 0 && newSession && batch.batchSeq === 0,
      }),
    )
    this.started = true
  }

  tick(): void {
    if (!this.started) return
    if (this.items.length === 0) {
      this._state = 'buffering'
      return
    }
    if (this.items.length > this.maxBacklog) {
      while (this.items.length > this.keepOnCatchUp) this.play(this.items.shift()!)
    }
    this.play(this.items.shift()!)
    this._state = 'playing'
    this.trim()
  }

  setWindow(seconds: number): void {
    this._windowSeconds = seconds
    this.trim()
  }

  get points(): Point[] { return this._points }
  get sessionStarts(): number[] { return this._sessionStarts }
  get latest(): FoldedSample | null { return this._latest }
  get state(): QueueState { return this._state }
  get queueDepth(): number { return this.items.length }
  get sessionId(): string | null { return this.current }
  get partial(): boolean { return this.currentPartial }
  get windowSeconds(): number { return this._windowSeconds }

  private play(item: Item): void {
    const t = Date.parse(item.sample.ts)
    if (item.breakBefore && this._points.length > 0) this._points.push({ t: t - 1, sample: null })
    if (item.sessionStart) this._sessionStarts.push(t)
    this._points.push({ t, sample: item.sample })
    this._latest = item.sample
  }

  private trim(): void {
    if (!this._latest) return
    const cutoff = Date.parse(this._latest.ts) - this._windowSeconds * 1000
    const firstKept = this._points.findIndex(p => p.t >= cutoff)
    this._points = firstKept === -1 ? [] : this._points.slice(firstKept)
    this._sessionStarts = this._sessionStarts.filter(t => t >= cutoff)
  }
}
```

`web/src/rows.ts`:
```ts
import type { Point } from './ReplayQueue'

export type Row = { t: number } & Record<string, number | null>

/** Flattens replayed points into Recharts rows; gaps and non-numeric values become null (line breaks). */
export function toRows(points: Point[], keys: string[]): Row[] {
  return points.map(p => {
    const row: Row = { t: p.t } as Row
    for (const k of keys) {
      const v = p.sample?.values[k]
      row[k] = typeof v === 'number' ? v : null
    }
    return row
  })
}
```

- [ ] **Step 6: Run tests — expect pass**

Run: `cd web; npm test; cd ..`
Expected: `Test Files  2 passed`, `Tests  13 passed`.

- [ ] **Step 7: Commit**

```powershell
git add web
git commit -m "feat(web): scaffold Vite app and add 1-second replay queue"
```

---

### Task 11: Web UI — hub connection and dashboard

**Files:**
- Create: `web/src/useTelemetry.ts`, `web/src/components/{Header,TrendChart,StatusPanel,SessionControls,RawDrawer}.tsx`
- Modify (replace): `web/src/App.tsx`, `web/src/main.tsx`, `web/src/index.css`
- Test: `npm run build` (type check) + manual browser check

**Interfaces:**
- Consumes: `ReplayQueue`, `Point`, `QueueState` (Task 10); `toRows`, `Row`; `FoldedBatch`, `FoldedSample`; Functions `POST /api/negotiate` (Task 9); simulator control API (Task 6).
- Produces: `useTelemetry(functionsBaseUrl: string) → { queue: ReplayQueue; conn: ConnState }`; `ConnState = 'connecting' | 'live' | 'reconnecting' | 'down'`.

- [ ] **Step 1: Hub hook** `web/src/useTelemetry.ts`

```ts
import { useEffect, useRef, useState } from 'react'
import { HubConnectionBuilder, LogLevel } from '@microsoft/signalr'
import { ReplayQueue } from './ReplayQueue'
import type { FoldedBatch } from './types'

export type ConnState = 'connecting' | 'live' | 'reconnecting' | 'down'

/** Connects to the SignalR hub via the Functions negotiate endpoint and ticks the replay queue every second. */
export function useTelemetry(functionsBaseUrl: string) {
  const queueRef = useRef(new ReplayQueue())
  const [conn, setConn] = useState<ConnState>('connecting')
  const [, setFrame] = useState(0)

  useEffect(() => {
    const hub = new HubConnectionBuilder()
      .withUrl(`${functionsBaseUrl}/api`)
      .withAutomaticReconnect([0, 2000, 5000, 10000, 30000])
      .configureLogging(LogLevel.Warning)
      .build()

    hub.on('batch', (batch: FoldedBatch) => queueRef.current.push(batch))
    hub.onreconnecting(() => setConn('reconnecting'))
    hub.onreconnected(() => setConn('live'))
    hub.onclose(() => setConn('down'))

    let disposed = false
    let retry: ReturnType<typeof setTimeout> | undefined
    const start = () => {
      setConn('connecting')
      hub.start()
        .then(() => !disposed && setConn('live'))
        .catch(() => {
          if (disposed) return
          setConn('down')
          retry = setTimeout(start, 5000) // automatic reconnect only covers drops after a first connect
        })
    }
    start()

    const timer = setInterval(() => {
      queueRef.current.tick()
      setFrame(f => f + 1)
    }, 1000)

    return () => {
      disposed = true
      clearInterval(timer)
      clearTimeout(retry)
      void hub.stop()
    }
  }, [functionsBaseUrl])

  return { queue: queueRef.current, conn }
}
```

- [ ] **Step 2: Components**

`web/src/components/Header.tsx`:
```tsx
import type { ConnState } from '../useTelemetry'
import type { QueueState } from '../ReplayQueue'

type Props = {
  conn: ConnState
  sessionId: string | null
  partial: boolean
  lagSeconds: number | null
  queueDepth: number
  state: QueueState
  windowSeconds: number
  onWindowChange: (seconds: number) => void
}

const WINDOWS = [60, 300, 900]

export function Header(p: Props) {
  return (
    <header className="header">
      <h1>Pump 001 · fast polling</h1>
      <span className={`dot dot-${p.conn}`} title={p.conn} /> <span>{p.conn}</span>
      <span>session <code>{p.sessionId?.slice(0, 8) ?? '—'}</code>{p.partial && <em className="warn"> partial</em>}</span>
      <span>lag {p.lagSeconds === null ? '—' : `${p.lagSeconds.toFixed(1)} s`}</span>
      <span>queue {p.queueDepth}</span>
      {p.state === 'buffering' && <span className="badge">buffering</span>}
      <label>
        window{' '}
        <select value={p.windowSeconds} onChange={e => p.onWindowChange(Number(e.target.value))}>
          {WINDOWS.map(w => <option key={w} value={w}>{w / 60} min</option>)}
        </select>
      </label>
    </header>
  )
}
```

`web/src/components/TrendChart.tsx`:
```tsx
import { CartesianGrid, Legend, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts'
import type { Row } from '../rows'

export type Series = { key: string; name: string; color: string; axis?: 'left' | 'right' }

type Props = {
  title: string
  rows: Row[]
  series: Series[]
  domain: [number, number]
  sessionStarts: number[]
  refLines?: { y: number; label: string }[]
}

const time = (t: number) => new Date(t).toLocaleTimeString()

export function TrendChart({ title, rows, series, domain, sessionStarts, refLines = [] }: Props) {
  const hasRight = series.some(s => s.axis === 'right')
  return (
    <section className="card">
      <h2>{title}</h2>
      <ResponsiveContainer width="100%" height={220}>
        <LineChart data={rows} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--grid)" />
          <XAxis dataKey="t" type="number" domain={domain} tickFormatter={time} allowDataOverflow />
          <YAxis yAxisId="left" domain={['auto', 'auto']} width={60} />
          {hasRight && <YAxis yAxisId="right" orientation="right" domain={['auto', 'auto']} width={40} />}
          <Tooltip labelFormatter={v => time(Number(v))} />
          <Legend />
          {series.map(s => (
            <Line key={s.key} yAxisId={s.axis ?? 'left'} dataKey={s.key} name={s.name} stroke={s.color}
              dot={false} isAnimationActive={false} connectNulls={false} />
          ))}
          {refLines.map(r => (
            <ReferenceLine key={r.label} yAxisId="left" y={r.y} stroke="var(--muted)" strokeDasharray="6 4"
              label={{ value: r.label, position: 'insideTopRight', fill: 'var(--muted)', fontSize: 11 }} />
          ))}
          {sessionStarts.map(t => (
            <ReferenceLine key={t} yAxisId="left" x={t} stroke="var(--accent)" strokeDasharray="3 3"
              label={{ value: 'session start', position: 'insideTopLeft', fill: 'var(--accent)', fontSize: 11 }} />
          ))}
        </LineChart>
      </ResponsiveContainer>
    </section>
  )
}
```

`web/src/components/StatusPanel.tsx`:
```tsx
import type { FoldedSample } from '../types'

const COUNTERS = ['StrokeCounterTW1', 'OperatingHoursMotor', 'OperatingHoursTW1']
const isFlag = (k: string) => k.includes('Alarm') || k.includes('Warning') || k === 'BoosterPressureFault'
const active = (v: unknown) => v === true || (typeof v === 'number' && v !== 0)

export function StatusPanel({ latest }: { latest: FoldedSample | null }) {
  if (!latest) return <section className="card"><h2>Status</h2><p className="muted">waiting for data…</p></section>
  const changed = new Set(latest.changed)
  const flash = (k: string) => (changed.has(k) ? 'flash' : '')
  const flags = Object.keys(latest.values).filter(isFlag)
  return (
    <section className="card">
      <h2>Status</h2>
      <div className="chips">
        <span key={`MotorRunning-${changed.has('MotorRunning') ? latest.ts : ''}`}
          className={`chip ${active(latest.values.MotorRunning) ? 'ok' : 'off'} ${flash('MotorRunning')}`}>MotorRunning</span>
        {flags.map(k => {
          const level = active(latest.values[k]) ? (k.includes('Warning') ? 'warn' : 'alarm') : 'off'
          return <span key={`${k}-${changed.has(k) ? latest.ts : ''}`} className={`chip ${level} ${flash(k)}`}>{k}</span>
        })}
      </div>
      <dl className="counters">
        {COUNTERS.map(k => (
          <div key={`${k}-${changed.has(k) ? latest.ts : ''}`} className={flash(k)}>
            <dt>{k}</dt><dd>{String(latest.values[k] ?? '—')}</dd>
          </div>
        ))}
      </dl>
    </section>
  )
}
```

`web/src/components/SessionControls.tsx`:
```tsx
import { useEffect, useState } from 'react'

type Response = { status: number; body: string } | { error: string }

export function SessionControls({ simulatorUrl }: { simulatorUrl: string }) {
  const [duration, setDuration] = useState(10)
  const [response, setResponse] = useState<Response | null>(null)
  const [active, setActive] = useState<boolean | null>(null)

  const refresh = async () => {
    try {
      const res = await fetch(`${simulatorUrl}/fast-polling/status`)
      setActive((await res.json()).active)
    } catch {
      setActive(null)
    }
  }

  const call = async (path: string, body?: unknown) => {
    try {
      const res = await fetch(`${simulatorUrl}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
      })
      setResponse({ status: res.status, body: await res.text() })
    } catch (e) {
      setResponse({ error: `simulator unreachable: ${String(e)}` })
    }
    void refresh()
  }

  useEffect(() => { void refresh() }, [])

  return (
    <section className="card">
      <h2>Session {active === null ? '(simulator offline)' : active ? '(active)' : '(idle)'}</h2>
      <label>
        duration (min){' '}
        <input type="number" min={10} max={30} value={duration} onChange={e => setDuration(Number(e.target.value))} />
      </label>{' '}
      <button onClick={() => call('/fast-polling/enable', { sessionDurationMinutes: duration })}>Enable</button>{' '}
      <button onClick={() => call('/fast-polling/disable')}>Disable</button>
      {response && (
        <pre className="response">
          {'error' in response ? response.error : `${response.status}\n${response.body}`}
        </pre>
      )}
    </section>
  )
}
```

`web/src/components/RawDrawer.tsx`:
```tsx
import type { FoldedSample } from '../types'

export function RawDrawer({ latest }: { latest: FoldedSample | null }) {
  if (!latest) return null
  const changed = new Set(latest.changed)
  return (
    <details className="card">
      <summary>Raw folded snapshot · {new Date(latest.ts).toLocaleTimeString()} · {latest.changed.length} changed</summary>
      <pre className="raw">
        {Object.entries(latest.values).map(([k, v]) => (
          <div key={k} className={changed.has(k) ? 'hl' : ''}>{k}: {JSON.stringify(v)}</div>
        ))}
      </pre>
    </details>
  )
}
```

- [ ] **Step 3: App, entry, styles**

`web/src/App.tsx`:
```tsx
import { useState } from 'react'
import { useTelemetry } from './useTelemetry'
import { toRows } from './rows'
import { Header } from './components/Header'
import { TrendChart } from './components/TrendChart'
import { StatusPanel } from './components/StatusPanel'
import { SessionControls } from './components/SessionControls'
import { RawDrawer } from './components/RawDrawer'

const FUNCTIONS_URL = import.meta.env.VITE_FUNCTIONS_URL ?? 'http://localhost:7071'
const SIMULATOR_URL = import.meta.env.VITE_SIMULATOR_URL ?? 'http://localhost:5100'

export default function App() {
  const { queue, conn } = useTelemetry(FUNCTIONS_URL)
  const [unit, setUnit] = useState<'PSI' | 'Bar'>('PSI')
  const [, setWindowTick] = useState(0)

  const latest = queue.latest
  const end = latest ? Date.parse(latest.ts) : Date.now()
  const domain: [number, number] = [end - queue.windowSeconds * 1000, end]
  const num = (k: string) => (typeof latest?.values[k] === 'number' ? (latest.values[k] as number) : null)

  const pressureKey = `OperatingPressure${unit}`
  const high = num(`HighPressureSetpoint${unit}`)
  const low = num(`LowPressureSetpoint${unit}`)
  const refLines = [
    ...(high !== null ? [{ y: high, label: 'high setpoint' }] : []),
    ...(low !== null ? [{ y: low, label: 'low setpoint' }] : []),
  ]

  return (
    <main>
      <Header
        conn={conn}
        sessionId={queue.sessionId}
        partial={queue.partial}
        lagSeconds={latest ? (Date.now() - Date.parse(latest.ts)) / 1000 : null}
        queueDepth={queue.queueDepth}
        state={queue.state}
        windowSeconds={queue.windowSeconds}
        onWindowChange={s => { queue.setWindow(s); setWindowTick(x => x + 1) }}
      />
      <div className="grid">
        <div className="charts">
          <div className="unit-toggle">
            <button className={unit === 'PSI' ? 'on' : ''} onClick={() => setUnit('PSI')}>PSI</button>
            <button className={unit === 'Bar' ? 'on' : ''} onClick={() => setUnit('Bar')}>Bar</button>
          </div>
          <TrendChart title={`Operating pressure (${unit})`} rows={toRows(queue.points, [pressureKey])} domain={domain}
            sessionStarts={queue.sessionStarts} refLines={refLines}
            series={[{ key: pressureKey, name: pressureKey, color: 'var(--s1)' }]} />
          <TrendChart title="Stroke rate" rows={toRows(queue.points, ['StrokeRateLeft', 'StrokeRateRight'])} domain={domain}
            sessionStarts={queue.sessionStarts}
            series={[
              { key: 'StrokeRateLeft', name: 'Left', color: 'var(--s1)' },
              { key: 'StrokeRateRight', name: 'Right', color: 'var(--s2)' },
            ]} />
          <TrendChart title="Oil temperature & proportional output"
            rows={toRows(queue.points, ['HydraulicOilTemperatureF', 'ProportionalOutputValue'])} domain={domain}
            sessionStarts={queue.sessionStarts}
            series={[
              { key: 'HydraulicOilTemperatureF', name: 'Oil temp °F', color: 'var(--s3)' },
              { key: 'ProportionalOutputValue', name: 'Prop. output', color: 'var(--s2)', axis: 'right' },
            ]} />
        </div>
        <aside>
          <SessionControls simulatorUrl={SIMULATOR_URL} />
          <StatusPanel latest={latest} />
        </aside>
      </div>
      <RawDrawer latest={latest} />
    </main>
  )
}
```

`web/src/main.tsx` (no StrictMode: its double-mounted effect would open and immediately close a hub connection):
```tsx
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App'

createRoot(document.getElementById('root')!).render(<App />)
```

`web/src/index.css`:
```css
:root {
  --bg: #f6f7f9; --card: #ffffff; --text: #1d2330; --muted: #6b7385; --grid: #e3e6ec;
  --accent: #5b5bd6; --s1: #2f6fdb; --s2: #d9731a; --s3: #1f9d73;
  --ok: #1f9d73; --warn: #c98a06; --alarm: #d4373b; --off: #c9ced8;
  color-scheme: light dark;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #12151b; --card: #1b1f27; --text: #e6e8ee; --muted: #8b93a5; --grid: #2a303b;
    --accent: #8f8ff0; --s1: #6ea0f5; --s2: #f0a15a; --s3: #4cc79c; --off: #3a404c;
  }
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--text); font: 14px/1.4 system-ui, sans-serif; }
main { max-width: 1400px; margin: 0 auto; padding: 16px; }
h1 { font-size: 18px; margin: 0 16px 0 0; }
h2 { font-size: 14px; margin: 0 0 8px; color: var(--muted); font-weight: 600; }
.header { display: flex; flex-wrap: wrap; align-items: center; gap: 12px 16px; margin-bottom: 16px; }
.dot { width: 10px; height: 10px; border-radius: 50%; display: inline-block; background: var(--off); }
.dot-live { background: var(--ok); } .dot-reconnecting, .dot-connecting { background: var(--warn); } .dot-down { background: var(--alarm); }
.badge { background: var(--warn); color: #000; padding: 1px 8px; border-radius: 999px; font-size: 12px; }
.warn { color: var(--warn); }
.grid { display: grid; grid-template-columns: 1fr 340px; gap: 16px; }
@media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
.charts { display: flex; flex-direction: column; gap: 16px; }
.card { background: var(--card); border-radius: 8px; padding: 12px 14px; margin-bottom: 16px; }
.charts .card { margin-bottom: 0; }
.unit-toggle button { margin-right: 4px; }
.unit-toggle .on { background: var(--accent); color: #fff; }
button { font: inherit; padding: 4px 12px; border-radius: 6px; border: 1px solid var(--grid); background: var(--card); color: var(--text); cursor: pointer; }
input { width: 64px; font: inherit; }
.chips { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; }
.chip { font-size: 11px; padding: 2px 8px; border-radius: 999px; border: 1px solid var(--off); color: var(--muted); }
.chip.ok { border-color: var(--ok); color: var(--ok); } .chip.warn { background: var(--warn); color: #000; border-color: var(--warn); }
.chip.alarm { background: var(--alarm); color: #fff; border-color: var(--alarm); }
.counters { display: grid; grid-template-columns: 1fr; gap: 4px; margin: 0; }
.counters div { display: flex; justify-content: space-between; padding: 2px 4px; border-radius: 4px; }
.counters dt { color: var(--muted); } .counters dd { margin: 0; font-variant-numeric: tabular-nums; }
.flash { animation: flash 600ms ease-out; }
@keyframes flash { from { background: color-mix(in srgb, var(--accent) 35%, transparent); } to { background: transparent; } }
.response, .raw { font-size: 12px; background: var(--bg); padding: 8px; border-radius: 6px; overflow: auto; max-height: 320px; }
.raw .hl { color: var(--accent); font-weight: 600; }
.muted { color: var(--muted); }
```

- [ ] **Step 4: Type-check and build**

Run: `cd web; npm run build; npm test; cd ..`
Expected: `vite build` finishes with `✓ built in …`; tests still pass.

- [ ] **Step 5: Manual browser check** (full stack)

Start Azurite, `asrs-emulator start`, `func start` in `src/Shape.Functions`, the simulator (`dotnet run --project src/Shape.GatewaySimulator -- --seed 42`) and `cd web; npm run dev`. Open `http://localhost:5173`:
1. Header dot turns green `live`.
2. Click **Enable** → response `200` JSON; within ~6 s the pressure chart starts adding **one point per second**; queue depth cycles 5→0.
3. Oil temperature steps only every ~10 s; the raw drawer highlights changed keys.
4. PSI/Bar toggle swaps line and setpoint reference lines.
5. Click **Disable**, then **Enable** again → dashed "session start" line and a break in each series.
6. Stop the simulator, restart it with `--drop-every 2`, enable → visible breaks in lines every other batch.

- [ ] **Step 6: Commit**

```powershell
git add web
git commit -m "feat(web): add SignalR dashboard with live trend charts and session controls"
```

---

### Task 12: Smoke client, run scripts, README

**Files:**
- Create: `src/Shape.SmokeClient/Shape.SmokeClient.csproj` (template), `src/Shape.SmokeClient/Program.cs`, `scripts/run-local.ps1`, `scripts/smoke.ps1`, `README.md`

**Interfaces:**
- Consumes: `FoldedBatch`, `PumpSeed` (Task 1); negotiate + hub (Task 9); simulator API (Task 6).
- Produces: `Shape.SmokeClient` CLI — `--functions <url>` (default `http://localhost:7071`), `--simulator <url>` (default `http://localhost:5100`), `--count <n>` (default 3), `--expect-gap`; exit code 0 = pass, 1 = fail.

- [ ] **Step 1: Create project**

```powershell
dotnet new console -n Shape.SmokeClient -o src/Shape.SmokeClient -f net8.0
dotnet sln add src/Shape.SmokeClient
dotnet add src/Shape.SmokeClient reference src/Shape.Contracts
dotnet add src/Shape.SmokeClient package Microsoft.AspNetCore.SignalR.Client
```

Add inside `<Project>` of `src/Shape.SmokeClient/Shape.SmokeClient.csproj`:
```xml
  <ItemGroup>
    <None Include="..\..\seed\pump-sample.json" Link="pump-sample.json" CopyToOutputDirectory="PreserveNewest" />
  </ItemGroup>
```

- [ ] **Step 2: Write** `src/Shape.SmokeClient/Program.cs`

```csharp
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR.Client;
using Shape.Contracts;

var functions = Arg("--functions") ?? "http://localhost:7071";
var simulator = Arg("--simulator") ?? "http://localhost:5100";
var count = int.Parse(Arg("--count") ?? "3");
var expectGap = args.Contains("--expect-gap");
var seedKeys = PumpSeed.LoadDefault().Keys.ToHashSet();

var received = Channel.CreateUnbounded<FoldedBatch>();
await using var hub = new HubConnectionBuilder().WithUrl($"{functions}/api").Build();
hub.On<FoldedBatch>("batch", b => received.Writer.TryWrite(b));
await hub.StartAsync();
Console.WriteLine($"Connected to hub via {functions}/api/negotiate");

using var http = new HttpClient();
var enable = await http.PostAsync($"{simulator}/fast-polling/enable", new StringContent("{}", Encoding.UTF8, "application/json"));
if (!enable.IsSuccessStatusCode) return Fail($"enable returned {(int)enable.StatusCode}: {await enable.Content.ReadAsStringAsync()}");
var sessionId = (await enable.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("sessionId").GetString()!;
Console.WriteLine($"Session {sessionId} enabled; waiting for {count} batches");

var batches = new List<FoldedBatch>();
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(count * 5 + 30));
try
{
    while (batches.Count < count)
    {
        var batch = await received.Reader.ReadAsync(timeout.Token);
        if (batch.SessionId != sessionId) continue;
        batches.Add(batch);
        Console.WriteLine($"  batch {batch.BatchSeq}: {batch.Samples.Count} samples, gap={batch.Gap}, partial={batch.Partial}");
    }
}
catch (OperationCanceledException)
{
    return Fail($"timed out with {batches.Count}/{count} batches");
}
finally
{
    await http.PostAsync($"{simulator}/fast-polling/disable", null);
}

if (expectGap)
{
    if (!batches.Any(b => b.Gap)) return Fail("expected at least one batch with gap=true");
}
else
{
    var seqs = batches.Select(b => b.BatchSeq).ToList();
    if (!seqs.SequenceEqual(Enumerable.Range(0, count).Select(i => (long)i))) return Fail($"unexpected batchSeq order {string.Join(",", seqs)}");
    if (batches.Any(b => b.Gap || b.Partial)) return Fail("unexpected gap/partial flag");
    if (batches.Any(b => b.Samples.Count != 5)) return Fail("expected 5 samples per batch");
    var bad = batches.SelectMany(b => b.Samples).FirstOrDefault(s => !s.Values.Keys.ToHashSet().SetEquals(seedKeys));
    if (bad is not null) return Fail($"sample at {bad.Ts} does not carry all {seedKeys.Count} fields");
}

Console.WriteLine("SMOKE PASS");
return 0;

string? Arg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

static int Fail(string message)
{
    Console.Error.WriteLine($"SMOKE FAIL: {message}");
    return 1;
}
```

- [ ] **Step 3: Write** `scripts/run-local.ps1`

```powershell
<#
.SYNOPSIS  Starts the local stack, each process in its own window.
.PARAMETER Infra  Start only Azurite, the SignalR emulator and the Functions host (used by smoke.ps1).
#>
param([switch]$Infra)

$root = Split-Path $PSScriptRoot -Parent

function Start-Window([string]$Title, [string]$Dir, [string]$Command) {
    Start-Process pwsh -WorkingDirectory $Dir -ArgumentList '-NoExit', '-Command', "`$Host.UI.RawUI.WindowTitle = '$Title'; $Command"
}

Start-Window 'azurite' $root 'azurite --silent --location .azurite'
Start-Window 'signalr-emulator' $root 'asrs-emulator start'
Start-Window 'functions' (Join-Path $root 'src/Shape.Functions') 'func start'

if (-not $Infra) {
    Start-Window 'simulator' $root 'dotnet run --project src/Shape.GatewaySimulator -- --seed 42'
    Start-Window 'web' (Join-Path $root 'web') 'npm run dev'
    Write-Host 'Dashboard: http://localhost:5173  (click Enable to start a session)'
}
```

- [ ] **Step 4: Write** `scripts/smoke.ps1`

```powershell
<#
.SYNOPSIS  End-to-end smoke test. Requires `scripts/run-local.ps1 -Infra` to be running (no simulator on :5100).
#>
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

dotnet build Shape5Sec.sln -v q | Out-Null
$simDll = Join-Path $root 'src/Shape.GatewaySimulator/bin/Debug/net8.0/Shape.GatewaySimulator.dll'
$client = Join-Path $root 'src/Shape.SmokeClient/bin/Debug/net8.0/Shape.SmokeClient.dll'

function Wait-Url([string]$Url, [string]$Method = 'Get', [int]$TimeoutSec = 90) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try { Invoke-WebRequest -Uri $Url -Method $Method -UseBasicParsing -TimeoutSec 5 | Out-Null; return }
        catch { Start-Sleep -Seconds 1 }
    }
    throw "Timed out waiting for $Url"
}

function Invoke-Scenario([string[]]$SimArgs, [string[]]$ClientArgs) {
    $sim = Start-Process dotnet -ArgumentList (@($simDll, '--seed', '42') + $SimArgs) -PassThru -WindowStyle Hidden
    try {
        Wait-Url 'http://localhost:5100/fast-polling/status'
        dotnet $client @ClientArgs
        if ($LASTEXITCODE -ne 0) { throw "Smoke client failed for simulator args '$SimArgs'" }
    }
    finally {
        Stop-Process -Id $sim.Id -ErrorAction SilentlyContinue
    }
}

Wait-Url 'http://localhost:7071/api/negotiate' -Method Post

Write-Host '== Scenario 1: clean run =='
Invoke-Scenario @() @('--count', '3')

Write-Host '== Scenario 2: dropped batches =='
Invoke-Scenario @('--drop-every', '2') @('--count', '3', '--expect-gap')

Write-Host 'ALL SMOKE SCENARIOS PASSED'
```

- [ ] **Step 5: Write** `README.md`

````markdown
# Shape 5-sec batched telemetry — local simulation

Gateway simulator → Event Grid–shaped webhook → Azure Function (fold snapshot + deltas, Azurite Table state)
→ Azure SignalR Emulator → React dashboard replaying 1 sample/second.

Design: `docs/superpowers/specs/2026-09-23-5sec-batch-telemetry-design.md`

## Prerequisites

- .NET 8 SDK, Azure Functions Core Tools v4, Node 20+, PowerShell 7
- `npm install -g azurite`
- `dotnet tool install -g Microsoft.Azure.SignalR.Emulator`
- `cd web; npm install`

## Run

```powershell
./scripts/run-local.ps1
```
Open http://localhost:5173 and click **Enable**.

Simulator flags: `--seed N`, `--drop-every N` (N ≥ 2), `--reorder`, `--functions-url URL`.

## Test

```powershell
dotnet test tests/Shape.Tests --filter "Category!=Integration"   # unit
azurite --silent --location .azurite                               # then, in another shell:
dotnet test tests/Shape.Tests --filter "Category=Integration"     # Azurite-backed
cd web; npm test                                                    # replay queue
./scripts/run-local.ps1 -Infra; ./scripts/smoke.ps1                # end-to-end
```

## Ports

simulator 5100 · Functions 7071 · SignalR emulator 8888 · Azurite 10000–10002 · Vite 5173
````

- [ ] **Step 6: Run the smoke test**

```powershell
./scripts/run-local.ps1 -Infra
# wait until the functions window shows "Functions:" with IngestTelemetry and negotiate listed
./scripts/smoke.ps1
```
Expected output ends with:
```
  batch 0: 5 samples, gap=False, partial=False
  batch 1: 5 samples, gap=False, partial=False
  batch 2: 5 samples, gap=False, partial=False
SMOKE PASS
== Scenario 2: dropped batches ==
...
  batch 2: 5 samples, gap=True, partial=False
...
SMOKE PASS
ALL SMOKE SCENARIOS PASSED
```

- [ ] **Step 7: Full verification and commit**

```powershell
dotnet test tests/Shape.Tests --filter "Category!=Integration"
cd web; npm test; npm run build; cd ..
git add src/Shape.SmokeClient scripts README.md Shape5Sec.sln
git commit -m "feat: add e2e smoke client, local run scripts and README"
```
Expected: all .NET unit tests pass, Vitest passes, web build succeeds.
