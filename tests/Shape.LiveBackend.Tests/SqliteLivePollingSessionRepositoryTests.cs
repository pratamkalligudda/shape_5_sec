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
