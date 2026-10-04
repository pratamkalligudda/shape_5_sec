using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>The "!current" head row (spec §5.6). ETag null means "not yet stored".</summary>
public sealed record LiveHead
{
    public Guid DeviceGuid { get; init; }
    public string DeviceId { get; init; } = "";
    public Dictionary<string, JsonElement> Snapshot { get; init; } = new();
    public int? DeviceStatus { get; init; }
    public DateTimeOffset? LastSnapshotTimestampUtc { get; init; }
    public bool HasBaseline { get; init; }
    public long TickCount { get; init; }
    public long BytesTotal { get; init; }
    public DateTimeOffset? FirstTickUtc { get; init; }
    public DateTimeOffset? LastTickUtc { get; init; }
    public string? ETag { get; init; }
}
