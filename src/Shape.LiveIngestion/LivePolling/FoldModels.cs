using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

public sealed record ResolvedDevice(Guid DeviceGuid, string SerialNumber);

public sealed record ChangePointWrite(string Parameter, DateTimeOffset TimestampUtc, JsonElement Value);

public sealed record LiveEventWrite(DateTimeOffset TimestampUtc, LiveEvent Event);

/// <summary>One entry of the outgoing SignalR batch. Exactly one of Snapshot/Changes; Events null when none.</summary>
public sealed record BroadcastEntry(
    DateTimeOffset TimestampUtc, int DeviceStatus,
    Dictionary<string, JsonElement>? Snapshot, Dictionary<string, JsonElement>? Changes, IReadOnlyList<LiveEvent>? Events);

public sealed record FoldResult(
    LiveHead Head, IReadOnlyList<ChangePointWrite> ChangePoints, IReadOnlyList<LiveEventWrite> Events,
    IReadOnlyList<BroadcastEntry> Broadcast, int Applied, int Backfilled)
{
    public bool HasWrites => Applied + Backfilled > 0;

    /// <summary>Entities one commit writes: the head row plus every change-point and event row.</summary>
    public int EntityCount => 1 + ChangePoints.Count + Events.Count;
}
