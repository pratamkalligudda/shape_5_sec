using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Pure fold of one gateway batch onto the stored head (spec §5.1 step 5–6, 09-20 §5.2, 09-30 §5).
/// Entries not newer than LastSnapshotTimestampUtc are skipped, except an older baseline when none has been folded
/// yet, which is back-filled. Never mutates its inputs.
/// </summary>
public static class SnapshotFolder
{
    public const string StatusParameter = "DeviceStatus";

    public static FoldResult Fold(
        LiveHead? head, LiveTelemetryBatch batch, ResolvedDevice device,
        IReadOnlyDictionary<string, AlertDefinition> catalog, ILogger logger)
    {
        var snapshot = head is null ? new Dictionary<string, JsonElement>() : new Dictionary<string, JsonElement>(head.Snapshot);
        var hasBaseline = head?.HasBaseline ?? false;
        var last = head?.LastSnapshotTimestampUtc;
        var status = head?.DeviceStatus;
        var ticks = head?.TickCount ?? 0;
        var firstTick = head?.FirstTickUtc;
        var lastTick = head?.LastTickUtc;

        var changePoints = new List<ChangePointWrite>();
        var events = new List<LiveEventWrite>();
        var broadcast = new List<BroadcastEntry>();
        int applied = 0, backfilled = 0;

        foreach (var entry in batch.Entries.OrderBy(e => e.TimestampUtc))
        {
            var ts = entry.TimestampUtc;
            if (last is not null && ts <= last)
            {
                if (entry.Body is null || hasBaseline) continue;   // duplicate / overlapping redelivery
                // Out-of-order baseline: record it at its own timestamp without moving the head backwards.
                foreach (var (k, v) in entry.Body)
                {
                    changePoints.Add(new ChangePointWrite(k, ts, v));
                    snapshot.TryAdd(k, v);
                }
                changePoints.Add(new ChangePointWrite(StatusParameter, ts, StatusValue(DeviceStatusDeriver.Derive(entry.Body))));
                foreach (var e in LiveEventResolver.Resolve(entry.Event, isBaseline: true, catalog, logger))
                    events.Add(new LiveEventWrite(ts, e));
                hasBaseline = true;
                ticks++;
                if (firstTick is null || ts < firstTick) firstTick = ts;
                backfilled++;
                continue;
            }

            if (entry.Body is not null)
            {
                snapshot = new Dictionary<string, JsonElement>(entry.Body);
                hasBaseline = true;
                foreach (var (k, v) in entry.Body) changePoints.Add(new ChangePointWrite(k, ts, v));
            }
            else
            {
                foreach (var (k, v) in entry.Changes!)
                {
                    snapshot[k] = v;
                    changePoints.Add(new ChangePointWrite(k, ts, v));
                }
            }

            var entryStatus = DeviceStatusDeriver.Derive(snapshot);
            if (entryStatus != status)
            {
                changePoints.Add(new ChangePointWrite(StatusParameter, ts, StatusValue(entryStatus)));
                status = entryStatus;
            }

            var resolved = LiveEventResolver.Resolve(entry.Event, isBaseline: entry.Body is not null, catalog, logger);
            foreach (var e in resolved) events.Add(new LiveEventWrite(ts, e));

            var isFirstOut = broadcast.Count == 0;
            broadcast.Add(new BroadcastEntry(
                ts, entryStatus,
                isFirstOut ? new Dictionary<string, JsonElement>(snapshot) : null,
                isFirstOut ? null : new Dictionary<string, JsonElement>(entry.Changes ?? entry.Body!),
                resolved.Count > 0 ? resolved : null));

            ticks++;
            firstTick ??= ts;
            lastTick = ts;
            last = ts;
            applied++;
        }

        var newHead = new LiveHead
        {
            DeviceGuid = device.DeviceGuid,
            DeviceId = device.SerialNumber,
            Snapshot = snapshot,
            DeviceStatus = status,
            LastSnapshotTimestampUtc = last,
            HasBaseline = hasBaseline,
            TickCount = ticks,
            BytesTotal = (head?.BytesTotal ?? 0) + (applied + backfilled > 0 ? batch.CompressedBytes : 0),
            FirstTickUtc = firstTick,
            LastTickUtc = lastTick,
            ETag = head?.ETag,
        };
        return new FoldResult(newHead, changePoints, events, broadcast, applied, backfilled);
    }

    private static JsonElement StatusValue(int status) => JsonSerializer.SerializeToElement((long)status);
}
