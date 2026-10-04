using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

public sealed record CleanupReport(int Purged, int Failed);

/// <summary>
/// Spec §5.4: for every finished, unpurged session — harvest TickCount/BytesTotal from the head (if any), delete the
/// partition (≤ 100 per transaction), stamp PurgedUtc. A failure leaves PurgedUtc null, so the next run retries it.
/// </summary>
public sealed class LivePollingCleanup(
    ICleanupSessionSource sessions, ILiveSessionStore store, TimeProvider time, ILogger<LivePollingCleanup> logger)
{
    public async Task<CleanupReport> RunAsync(CancellationToken ct = default)
    {
        await sessions.EnsureSchemaAsync(ct);
        var now = time.GetUtcNow();
        int purged = 0, failed = 0;
        foreach (var id in await sessions.GetFinishedUnpurgedAsync(now, ct))
        {
            try
            {
                if (await store.LoadHeadAsync(id, ct) is { } head)
                    await sessions.RecordCountersAsync(id, head.TickCount, head.BytesTotal, ct);
                var rows = await store.DeletePartitionAsync(id, ct);
                await sessions.MarkPurgedAsync(id, now, ct);
                logger.LogInformation("Purged live session {SessionId}: {Rows} rows", id, rows);
                purged++;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                logger.LogError(ex, "Purging live session {SessionId} failed; retrying next run", id);
                failed++;
            }
        }
        return new CleanupReport(purged, failed);
    }
}
