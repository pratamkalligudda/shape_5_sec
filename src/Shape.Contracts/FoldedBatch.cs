using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.Contracts;

/// <summary>What the Function pushes to SignalR: every sample carries the full folded snapshot.</summary>
public sealed record FoldedBatch
{
    [JsonPropertyName("sessionId")] public string SessionId { get; init; } = "";
    [JsonPropertyName("batchSeq")] public long BatchSeq { get; init; }
    [JsonPropertyName("gap")] public bool Gap { get; init; }
    [JsonPropertyName("partial")] public bool Partial { get; init; }
    [JsonPropertyName("samples")] public IReadOnlyList<FoldedSample> Samples { get; init; } = [];
}

public sealed record FoldedSample
{
    [JsonPropertyName("ts")] public DateTimeOffset Ts { get; init; }
    [JsonPropertyName("values")] public Dictionary<string, JsonElement> Values { get; init; } = new();
    [JsonPropertyName("changed")] public IReadOnlyList<string> Changed { get; init; } = [];
}
