using System.IO.Compression;
using System.Text;
using System.Text.Json;

namespace Shape.LiveIngestion.Tests;

/// <summary>Builds Event Grid Microsoft.Devices.DeviceTelemetry events in the simulator's shape (spec §4.4).</summary>
public static class TestEvents
{
    public static readonly Guid Session = Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8");

    public static byte[] Gzip(string json)
    {
        using var output = new MemoryStream();
        using (var gz = new GZipStream(output, CompressionLevel.Optimal, leaveOpen: true))
            gz.Write(Encoding.UTF8.GetBytes(json));
        return output.ToArray();
    }

    public static string Build(string entriesJson, string telemetryType = "fast-polling", Guid? session = null,
        string gateway = "gw-001", string? rawBody = null, string batchSentUtc = "2026-10-01T10:00:05.0000000Z")
    {
        var body = rawBody ?? Convert.ToBase64String(Gzip($$"""{"Entries":{{entriesJson}}}"""));
        return JsonSerializer.Serialize(new
        {
            id = "1",
            subject = $"devices/{gateway}",
            eventType = "Microsoft.Devices.DeviceTelemetry",
            eventTime = batchSentUtc,
            dataVersion = "",
            data = new
            {
                properties = new Dictionary<string, string>
                {
                    ["batchSentUtc"] = batchSentUtc,
                    ["session-id"] = (session ?? Session).ToString(),
                    ["telemetry-type"] = telemetryType,
                },
                systemProperties = new Dictionary<string, string>
                {
                    ["iothub-connection-device-id"] = gateway,
                    ["iothub-content-encoding"] = "gzip",
                },
                body,
            },
        });
    }
}
