using System.IO.Compression;
using System.Text.Json;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class EnvelopeTests
{
    private static readonly DateTime T0 = DateTime.Parse("2026-10-01T10:00:00Z").ToUniversalTime();

    [Fact]
    public void Gzip_round_trips_Entries()
    {
        var entries = new[] { new GatewayEntry { TimestampUtc = T0, Changes = new() { ["A"] = J(2) } } };

        var json = GzipBatchCodec.DecodeJson(GzipBatchCodec.Encode(entries));

        using var doc = JsonDocument.Parse(json);
        var e = Assert.Single(doc.RootElement.GetProperty("Entries").EnumerateArray());
        Assert.Equal(2, e.GetProperty("Changes").GetProperty("A").GetInt32());
    }

    [Fact]
    public void Event_grid_envelope_matches_iot_hub_device_telemetry_shape()
    {
        var device = Device();
        var batch = new OutgoingBatch(Guid.Parse("cc27db6b-508b-488e-a816-faae684491d8"), 0, T0,
            [new GatewayEntry { TimestampUtc = T0, Changes = new() }]);
        var gz = GzipBatchCodec.Encode(batch.Entries);

        using var doc = JsonDocument.Parse(EventGridEnvelope.Build(device, batch, gz));

        var ev = Assert.Single(doc.RootElement.EnumerateArray());
        Assert.Equal("Microsoft.Devices.DeviceTelemetry", ev.GetProperty("eventType").GetString());
        Assert.Equal("devices/gw-001", ev.GetProperty("subject").GetString());
        var data = ev.GetProperty("data");
        var props = data.GetProperty("properties");
        Assert.Equal("fast-polling", props.GetProperty("telemetry-type").GetString());
        Assert.Equal("cc27db6b-508b-488e-a816-faae684491d8", props.GetProperty("session-id").GetString());
        Assert.Equal("2026-10-01T10:00:00.0000000Z", props.GetProperty("batchSentUtc").GetString());
        var sys = data.GetProperty("systemProperties");
        Assert.Equal("gw-001", sys.GetProperty("iothub-connection-device-id").GetString());
        Assert.Equal("gzip", sys.GetProperty("iothub-content-encoding").GetString());
        var body = Convert.FromBase64String(data.GetProperty("body").GetString()!);
        Assert.Equal(gz, body);
        using var gzip = new GZipStream(new MemoryStream(body), CompressionMode.Decompress);
        Assert.Contains("Entries", new StreamReader(gzip).ReadToEnd());
    }
}
