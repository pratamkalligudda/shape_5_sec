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
