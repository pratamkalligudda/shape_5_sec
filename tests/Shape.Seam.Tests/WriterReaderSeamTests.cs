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
