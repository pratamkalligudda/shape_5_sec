using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

public sealed record LiveBroadcast(string GroupName, LiveEnvelope Envelope);

/// <summary>Spec §5.1 steps 1–8. Returns null whenever nothing should be broadcast.</summary>
public sealed class LiveTelemetryProcessor(
    DeviceResolver devices, IAlertDefinitionCatalog catalog, ILiveSessionStore store, ILogger<LiveTelemetryProcessor> logger)
{
    public const int MaxRetries = 3;

    public async Task<LiveBroadcast?> ProcessAsync(string eventJson, CancellationToken ct = default)
    {
        var decoded = LiveEventGridDecoder.Decode(eventJson);
        if (decoded.Status != DecodeStatus.Ok)
        {
            logger.LogError("Dropped live telemetry event: {Status} {Reason}", decoded.Status, decoded.Reason);
            return null;
        }
        var batch = decoded.Batch!;

        if (!devices.TryResolve(batch.GatewayId, out var device))
        {
            logger.LogError("Dropped live telemetry batch from unknown gateway {GatewayId}", batch.GatewayId);
            return null;
        }

        IReadOnlyDictionary<string, AlertDefinition> definitions;
        try
        {
            definitions = await catalog.GetAsync(ct);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Alert catalog unavailable; events resolve as unknown");
            definitions = new Dictionary<string, AlertDefinition>();
        }

        for (var attempt = 0; ; attempt++)
        {
            var head = await store.LoadHeadAsync(batch.SessionId, ct);
            var fold = SnapshotFolder.Fold(head, batch, device, definitions, logger);
            if (!fold.HasWrites)
            {
                logger.LogInformation("Session {SessionId}: batch of {Count} entries is not newer than the head; skipped",
                    batch.SessionId, batch.Entries.Count);
                return null;
            }

            try
            {
                await store.WriteBatchAsync(batch.SessionId, fold.Head, fold.ChangePoints, fold.Events, ct);
            }
            catch (LiveHeadConflictException ex) when (attempt < MaxRetries)
            {
                logger.LogWarning(ex, "Session {SessionId}: head conflict, re-folding (retry {Retry})", batch.SessionId, attempt + 1);
                continue;
            }

            logger.LogInformation(
                "Session {SessionId}: applied {Applied}, back-filled {Backfilled}, {Rows} rows, {Events} events",
                batch.SessionId, fold.Applied, fold.Backfilled, fold.ChangePoints.Count, fold.Events.Count);
            if (fold.Broadcast.Count == 0) return null;
            return new LiveBroadcast(SignalRNames.DeviceGroup(device.DeviceGuid),
                BroadcastEncoder.Encode(batch.SessionId, device.DeviceGuid, batch.BatchSentUtc, fold.Broadcast));
        }
    }
}
