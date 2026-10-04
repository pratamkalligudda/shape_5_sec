using System.Text.Json;

namespace Shape.LiveBackend.Api;

public sealed record StartLiveSessionRequest(int? PollIntervalSeconds, int? SessionDurationMinutes);

/// <summary>Start and extend response. sessionDurationMinutes carries the current total, as in the reference example.</summary>
public sealed record LiveSessionDto(
    Guid SessionId, string DeviceId, int PollIntervalSeconds, int SessionDurationMinutes,
    DateTimeOffset StartedUtc, DateTimeOffset ExpiresUtc, int TotalDurationMinutes, bool CanExtend);

/// <summary>GET /live-session. When Active is false every other field is an explicit null (see F3).</summary>
public sealed record LiveSessionStatusDto(
    bool Active, Guid? SessionId, int? PollIntervalSeconds, DateTimeOffset? ExpiresUtc, int? SecondsRemaining,
    int? TotalDurationMinutes, bool? CanExtend, int? DeviceStatus, Dictionary<string, JsonElement>? Snapshot,
    DateTimeOffset? SnapshotTimestampUtc, long? TickCount, long? BytesTotal)
{
    public static readonly LiveSessionStatusDto Inactive = new(false, null, null, null, null, null, null, null, null, null, null, null);
}

public sealed record SeriesMetricDto(string Metric, IReadOnlyList<JsonElement?> V);

public sealed record LiveEventDto(
    DateTimeOffset TimestampUtc, string Parameter, long Value, string State, string EventType, string Domain, string Severity, string? Label);

public sealed record SeriesDto(
    Guid SessionId, int PollIntervalSeconds, DateTimeOffset StartedUtc, DateTimeOffset? LastTickUtc, long TickCount,
    IReadOnlyList<DateTimeOffset> T, IReadOnlyList<int> Status, IReadOnlyList<SeriesMetricDto> Series, IReadOnlyList<LiveEventDto> Events);
