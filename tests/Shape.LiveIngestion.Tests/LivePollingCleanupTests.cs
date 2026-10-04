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
