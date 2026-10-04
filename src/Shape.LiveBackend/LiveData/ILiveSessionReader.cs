using System.Text.Json;
using Shape.LiveBackend.Api;

namespace Shape.LiveBackend.LiveData;

public sealed record LiveHeadView(
    Dictionary<string, JsonElement> Snapshot, int? DeviceStatus, DateTimeOffset? LastSnapshotTimestampUtc,
    long TickCount, long BytesTotal, DateTimeOffset? FirstTickUtc, DateTimeOffset? LastTickUtc);

public sealed record ChangePoint(DateTimeOffset TimestampUtc, JsonElement Value);

/// <summary>Read side of the live session partition (spec §7.4). Every list is oldest first.</summary>
public interface ILiveSessionReader
{
    Task<LiveHeadView?> ReadHeadAsync(Guid sessionId, CancellationToken ct = default);
    Task<IReadOnlyList<ChangePoint>> ReadParameterAsync(Guid sessionId, string parameter, CancellationToken ct = default);
    Task<IReadOnlyList<LiveEventDto>> ReadEventsAsync(Guid sessionId, CancellationToken ct = default);
}
