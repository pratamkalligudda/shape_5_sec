using System.IO.Compression;
using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// The only class that knows how Event Grid presents an IoT Hub message: data.properties (app properties),
/// data.systemProperties, data.body = base64(gzip(JSON)). Assumption for 09-20 §16 item 5 — change here once confirmed.
/// </summary>
public static class LiveEventGridDecoder
{
    public static DecodeResult Decode(string eventJson)
    {
        try
        {
            using var doc = JsonDocument.Parse(eventJson);
            var ev = doc.RootElement.ValueKind == JsonValueKind.Array ? doc.RootElement[0] : doc.RootElement;
            var data = ev.GetProperty("data");
            var props = data.GetProperty("properties");

            if (!props.TryGetProperty("telemetry-type", out var type) || type.GetString() != "fast-polling")
                return new DecodeResult(DecodeStatus.NotFastPolling, null, $"telemetry-type is '{(type.ValueKind == JsonValueKind.String ? type.GetString() : "<absent>")}'");

            var sessionId = Guid.Parse(props.GetProperty("session-id").GetString()!);
            var sentUtc = DateTimeOffset.Parse(props.GetProperty("batchSentUtc").GetString()!, null,
                System.Globalization.DateTimeStyles.RoundtripKind);
            var sys = data.GetProperty("systemProperties");
            var gatewayId = sys.GetProperty("iothub-connection-device-id").GetString()!;
            var gzip = sys.TryGetProperty("iothub-content-encoding", out var enc) && enc.GetString() == "gzip";

            byte[] raw;
            try { raw = Convert.FromBase64String(data.GetProperty("body").GetString()!); }
            catch (FormatException) { return DecodeResult.Invalid("body is not base64"); }

            string json;
            try { json = gzip ? Gunzip(raw) : System.Text.Encoding.UTF8.GetString(raw); }
            catch (InvalidDataException) { return DecodeResult.Invalid("body is not valid gzip"); }

            using var batchDoc = JsonDocument.Parse(json);
            var entries = new List<LiveEntry>();
            foreach (var e in batchDoc.RootElement.GetProperty("Entries").EnumerateArray())
            {
                if (!e.TryGetProperty("TimestampUtc", out var ts) || ts.ValueKind != JsonValueKind.String)
                    return DecodeResult.Invalid("entry without TimestampUtc");
                var body = ObjectOrNull(e, "Body");
                var changes = ObjectOrNull(e, "Changes");
                if ((body is null) == (changes is null))
                    return DecodeResult.Invalid("entry must carry exactly one of Body or Changes");
                entries.Add(new LiveEntry(ts.GetDateTimeOffset(), body, changes, ObjectOrNull(e, "Event")));
            }
            if (entries.Count == 0) return DecodeResult.Invalid("batch has no entries");

            return new DecodeResult(DecodeStatus.Ok, new LiveTelemetryBatch(sessionId, sentUtc, gatewayId, raw.LongLength, entries), null);
        }
        catch (Exception ex) when (ex is JsonException or KeyNotFoundException or InvalidOperationException or FormatException
                                       or IndexOutOfRangeException or ArgumentNullException)
        {
            return DecodeResult.Invalid(ex.Message);
        }
    }

    private static Dictionary<string, JsonElement>? ObjectOrNull(JsonElement entry, string name) =>
        entry.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Object
            ? v.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone())
            : null;

    private static string Gunzip(byte[] bytes)
    {
        using var gz = new GZipStream(new MemoryStream(bytes), CompressionMode.Decompress);
        using var reader = new StreamReader(gz);
        return reader.ReadToEnd();
    }
}
