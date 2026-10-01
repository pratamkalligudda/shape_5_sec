using System.Text.Json;
using System.Text.Json.Serialization;

namespace Shape.Gateway.Simulator;

/// <summary>One tick as the module sends it (module doc v3 "Sample Telemetry Payloads").</summary>
public sealed record GatewayEntry
{
    [JsonPropertyName("TimestampUtc")] public DateTime TimestampUtc { get; init; }

    [JsonPropertyName("Body"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, JsonElement>? Body { get; init; }

    [JsonPropertyName("Changes"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, JsonElement>? Changes { get; init; }

    [JsonPropertyName("Event"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, int>? Event { get; init; }
}

public sealed record OutgoingBatch(Guid SessionId, int Seq, DateTime SentUtc, IReadOnlyList<GatewayEntry> Entries);
