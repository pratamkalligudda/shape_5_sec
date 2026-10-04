using Shape.LiveBackend.Options;

namespace Shape.LiveBackend.Sessions;

/// <summary>One LivePollingSession row (spec §7.5).</summary>
public sealed record LivePollingSessionRecord
{
    public Guid SessionId { get; init; }
    public Guid DeviceGuid { get; init; }
    public string DeviceId { get; init; } = "";
    public string GatewayId { get; init; } = "";
    public DateTimeOffset StartedUtc { get; init; }
    public string StartedByUserId { get; init; } = "local";
    public int PollIntervalSeconds { get; init; }
    public int SessionDurationMinutes { get; init; }
    public DateTimeOffset ExpiresUtc { get; init; }
    public int ExtensionsUsed { get; init; }
    public DateTimeOffset? LastExtendedUtc { get; init; }
    public DateTimeOffset? StoppedUtc { get; init; }
    public string? StopReason { get; init; }
    public long? TickCount { get; init; }
    public long? BytesTotal { get; init; }
    public DateTimeOffset? PurgedUtc { get; init; }

    public int TotalDurationMinutes => (int)Math.Round((ExpiresUtc - StartedUtc).TotalMinutes);
    public bool CanExtend => TotalDurationMinutes < LivePollingOptions.MaxTotalDurationMinutes;
}
