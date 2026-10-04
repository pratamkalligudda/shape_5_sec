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
