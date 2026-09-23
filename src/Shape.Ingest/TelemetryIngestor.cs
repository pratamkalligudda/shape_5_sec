using System.Text.Json;
using Microsoft.Extensions.Logging;
using Shape.Contracts;

namespace Shape.Ingest;

/// <summary>Load state → fold → save (optimistic, retried) → return what to push. Null means "push nothing".</summary>
public sealed class TelemetryIngestor(ISnapshotStore store, ILogger<TelemetryIngestor> logger)
{
    public const int MaxSaveAttempts = 3;

    public async Task<FoldedBatch?> IngestAsync(string dataJson, CancellationToken ct)
    {
        TelemetryBatch? batch;
        try
        {
            batch = JsonSerializer.Deserialize<TelemetryBatch>(dataJson, ShapeJson.Options);
        }
        catch (JsonException ex)
        {
            logger.LogError(ex, "Discarding telemetry batch: data is not valid JSON");
            return null;
        }

        var sessionId = batch?.Properties?.SessionId;
        if (batch is null || string.IsNullOrEmpty(sessionId))
        {
            logger.LogError("Discarding telemetry batch: missing Properties/session-id");
            return null;
        }

        for (var attempt = 1; ; attempt++)
        {
            var state = await store.LoadAsync(sessionId, ct);
            var result = SnapshotFolder.Fold(state, batch);
            var seq = batch.Properties!.BatchSeq;

            switch (result.Outcome)
            {
                case FoldOutcome.Invalid:
                    logger.LogError("Discarding invalid batch {BatchSeq} for session {SessionId}", seq, sessionId);
                    return null;
                case FoldOutcome.Stale:
                    logger.LogWarning("Dropping stale batch {BatchSeq} for session {SessionId} (last {LastSeq})",
                        seq, sessionId, state?.LastBatchSeq);
                    return null;
                case FoldOutcome.Gap:
                    logger.LogWarning("Gap before batch {BatchSeq} for session {SessionId} (last {LastSeq})",
                        seq, sessionId, state?.LastBatchSeq);
                    break;
                case FoldOutcome.Orphan:
                    logger.LogWarning("Batch {BatchSeq} for unknown session {SessionId} has no Body; folding partial",
                        seq, sessionId);
                    break;
            }

            try
            {
                await store.SaveAsync(result.NewState!, ct);
                return result.Folded;
            }
            catch (SnapshotConflictException) when (attempt < MaxSaveAttempts)
            {
                logger.LogWarning("Snapshot conflict for session {SessionId}, attempt {Attempt}; retrying", sessionId, attempt);
            }
        }
    }
}
