using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>One gateway tick. Exactly one of Body/Changes; Event is a sibling, never nested.</summary>
public sealed record LiveEntry(
    DateTimeOffset TimestampUtc,
    Dictionary<string, JsonElement>? Body,
    Dictionary<string, JsonElement>? Changes,
    Dictionary<string, JsonElement>? Event);

public sealed record LiveTelemetryBatch(
    Guid SessionId, DateTimeOffset BatchSentUtc, string GatewayId, long CompressedBytes, IReadOnlyList<LiveEntry> Entries);

public enum DecodeStatus { Ok, NotFastPolling, Invalid }

public sealed record DecodeResult(DecodeStatus Status, LiveTelemetryBatch? Batch, string? Reason)
{
    public static DecodeResult Invalid(string reason) => new(DecodeStatus.Invalid, null, reason);
}
