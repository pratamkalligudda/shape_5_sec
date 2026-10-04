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
