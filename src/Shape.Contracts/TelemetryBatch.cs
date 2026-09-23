using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.Contracts;

/// <summary>One 5-second packet sent by the gateway (the Event Grid event's data).</summary>
public sealed record TelemetryBatch
{
    [JsonPropertyName("Properties")] public BatchProperties? Properties { get; init; }
    [JsonPropertyName("Samples")] public IReadOnlyList<Sample>? Samples { get; init; }
}

public sealed record BatchProperties
{
    [JsonPropertyName("session-id")] public string SessionId { get; init; } = "";
    [JsonPropertyName("telemetry-type")] public string TelemetryType { get; init; } = "fast-polling";
    [JsonPropertyName("batchSeq")] public long BatchSeq { get; init; }
    [JsonPropertyName("batchStartUtc")] public DateTimeOffset BatchStartUtc { get; init; }
}

/// <summary>One 1-second reading. Exactly one of <see cref="Body"/> (full snapshot) or <see cref="Changes"/> (delta) is set.</summary>
public sealed record Sample
{
    [JsonPropertyName("snapshotTimestampUtc")] public DateTimeOffset SnapshotTimestampUtc { get; init; }
    [JsonPropertyName("Body")] public Dictionary<string, JsonElement>? Body { get; init; }
    [JsonPropertyName("Changes")] public Dictionary<string, JsonElement>? Changes { get; init; }
}
