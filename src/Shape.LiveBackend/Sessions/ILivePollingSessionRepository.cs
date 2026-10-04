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
