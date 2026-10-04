using System.IO.Compression;
using System.Text.Json;
using System.Text.Json.Serialization;
using Shape.LiveIngestion.Infrastructure;

namespace Shape.LiveIngestion.LivePolling;

public sealed record LiveEnvelope(
    [property: JsonPropertyName("encoding")] string Encoding,
    [property: JsonPropertyName("payload")] string Payload);

/// <summary>
/// Spec §5.1 step 8: compact camelCase JSON, nulls written (every entry key always present), field-name dictionary
/// keys untouched, gzip, base64. Event objects carry no timestampUtc — the entry's own is the event's.
/// </summary>
public static class BroadcastEncoder
{
    public const string Encoding = "gzip+base64";

    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.Never,
        Converters = { new UtcDateTimeOffsetConverter() },
    };

    private sealed record WireBatch(Guid SessionId, Guid DeviceGuid, DateTimeOffset BatchSentUtc, IReadOnlyList<BroadcastEntry> Entries);

    public static string BatchJson(Guid sessionId, Guid deviceGuid, DateTimeOffset batchSentUtc, IReadOnlyList<BroadcastEntry> entries) =>
        JsonSerializer.Serialize(new WireBatch(sessionId, deviceGuid, batchSentUtc, entries), Json);

    public static LiveEnvelope Encode(Guid sessionId, Guid deviceGuid, DateTimeOffset batchSentUtc, IReadOnlyList<BroadcastEntry> entries)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(BatchJson(sessionId, deviceGuid, batchSentUtc, entries));
        using var output = new MemoryStream();
        using (var gz = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gz.Write(bytes);
        return new LiveEnvelope(Encoding, Convert.ToBase64String(output.ToArray()));
    }
}
