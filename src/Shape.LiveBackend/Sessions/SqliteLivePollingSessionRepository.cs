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
