using System.Globalization;
using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>
/// IoT Hub → Event Grid system topic shape (Microsoft.Devices.DeviceTelemetry). This is the harness's assumption
/// for open item 5 of the 09-20 design: a gzip body arrives base64 in data.body, encoding in systemProperties.
/// </summary>
public static class EventGridEnvelope
{
    public static string Build(DeviceConfig device, OutgoingBatch batch, byte[] gzipBody)
    {
        var sent = batch.SentUtc.ToString("O", CultureInfo.InvariantCulture);
        return JsonSerializer.Serialize(new[]
        {
            new
            {
                id = Guid.NewGuid().ToString(),
                topic = "/subscriptions/local/resourceGroups/local/providers/Microsoft.Devices/IotHubs/local-hub",
                subject = $"devices/{device.GatewayId}",
                eventType = "Microsoft.Devices.DeviceTelemetry",
                eventTime = sent,
                dataVersion = "",
                metadataVersion = "1",
                data = new
                {
                    properties = new Dictionary<string, string>
                    {
                        ["batchSentUtc"] = sent,
                        ["session-id"] = batch.SessionId.ToString(),
                        ["telemetry-type"] = "fast-polling",
                    },
                    systemProperties = new Dictionary<string, string>
                    {
                        ["iothub-connection-device-id"] = device.GatewayId,
                        ["iothub-connection-module-id"] = device.ModuleId,
                        ["iothub-content-encoding"] = "gzip",
                        ["iothub-content-type"] = "application/json",
                        ["iothub-enqueuedtime"] = sent,
                    },
                    body = Convert.ToBase64String(gzipBody),
                },
            },
        });
    }
}
