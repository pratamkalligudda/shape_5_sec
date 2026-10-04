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
