using System.IO.Compression;
using System.Text;
using System.Text.Json;
using Shape.LiveIngestion.LivePolling;
using static Shape.LiveIngestion.Tests.FoldBuilders;

namespace Shape.LiveIngestion.Tests;

public class BroadcastEncoderTests
{
    private static readonly Guid Session = TestEvents.Session;

    public static string Decode(LiveEnvelope envelope)
    {
        using var gz = new GZipStream(new MemoryStream(Convert.FromBase64String(envelope.Payload)), CompressionMode.Decompress);
        using var reader = new StreamReader(gz, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    private static IReadOnlyList<BroadcastEntry> Entries() =>
    [
        new(T0, 0, M("""{"MotorRunning":true,"OperatingPressurePSI":54351}"""), null, null),
        new(T0.AddSeconds(1), 0, null, M("""{"OperatingPressurePSI":54360}"""), null),
        new(T0.AddSeconds(2), 0, null, M("{}"), null),
        new(T0.AddSeconds(3), 1, null, M("""{"OperatingWarningActive":21}"""),
            [new LiveEvent("OperatingWarningActive", 21, "triggered", "warning", "operational", "major", "Left Overstroke Warning")]),
        new(T0.AddSeconds(4), 1, null, M("{}"),
            [new LiveEvent("MaintenanceAlarmActive", 1, "triggered", "alarm", "maintenance", "critical", null)]),
    ];

    private static JsonElement Batch() =>
        JsonDocument.Parse(Decode(BroadcastEncoder.Encode(Session, Device.DeviceGuid, T0.AddSeconds(5), Entries()))).RootElement;

    [Fact]
    public void Envelope_is_gzip_base64_with_lowercase_property_names()
    {
        var envelope = BroadcastEncoder.Encode(Session, Device.DeviceGuid, T0.AddSeconds(5), Entries());
        var json = JsonSerializer.Serialize(envelope);

        // System.Text.Json escapes '+' as + by default; any JSON parser reads it back as '+'.
        var parsed = JsonDocument.Parse(json).RootElement;
        Assert.Equal(new[] { "encoding", "payload" }, parsed.EnumerateObject().Select(p => p.Name));
        Assert.Equal("gzip+base64", parsed.GetProperty("encoding").GetString());
        Assert.StartsWith("H4sI", parsed.GetProperty("payload").GetString());
    }

    [Fact]
    public void Batch_has_session_device_and_sent_time()
    {
        var b = Batch();
        Assert.Equal(Session.ToString(), b.GetProperty("sessionId").GetString());
        Assert.Equal(Device.DeviceGuid.ToString(), b.GetProperty("deviceGuid").GetString());
        Assert.Equal("2026-10-01T10:00:05.0000000Z", b.GetProperty("batchSentUtc").GetString());
    }

    [Fact]
    public void Every_entry_carries_all_five_keys_with_explicit_nulls()
    {
        foreach (var e in Batch().GetProperty("entries").EnumerateArray())
            Assert.Equal(new[] { "timestampUtc", "deviceStatus", "snapshot", "changes", "events" },
                e.EnumerateObject().Select(p => p.Name));

        var entries = Batch().GetProperty("entries");
        Assert.Equal(JsonValueKind.Null, entries[0].GetProperty("changes").ValueKind);
        Assert.Equal(JsonValueKind.Null, entries[1].GetProperty("snapshot").ValueKind);
        Assert.Equal(JsonValueKind.Null, entries[1].GetProperty("events").ValueKind);
        Assert.Equal(JsonValueKind.Object, entries[2].GetProperty("changes").ValueKind);
        Assert.Equal(JsonValueKind.Number, entries[3].GetProperty("deviceStatus").ValueKind);
    }

    [Fact]
    public void Field_names_keep_telemetry_casing_and_events_are_camel_case_with_null_label()
    {
        var entries = Batch().GetProperty("entries");
        Assert.True(entries[0].GetProperty("snapshot").TryGetProperty("OperatingPressurePSI", out _));

        var labelled = entries[3].GetProperty("events")[0];
        Assert.Equal(new[] { "parameter", "value", "state", "eventType", "domain", "severity", "label" },
            labelled.EnumerateObject().Select(p => p.Name));
        Assert.Equal("Left Overstroke Warning", labelled.GetProperty("label").GetString());
        Assert.Equal(JsonValueKind.Null, entries[4].GetProperty("events")[0].GetProperty("label").ValueKind);
        Assert.Equal("2026-10-01T10:00:03.0000000Z", entries[3].GetProperty("timestampUtc").GetString());
    }

    [Fact]
    public void Real_seed_batch_stays_inside_the_message_budget()
    {
        var seed = Seed();
        var keys = new[] { "OperatingPressurePSI", "OperatingPressureBar", "StrokeRateLeft", "StrokeRateRight",
            "StrokeTimeLeft", "StrokeTimeRight", "StrokeCounterTW1", "ProportionalOutputValue" };
        var entries = new List<BroadcastEntry> { new(T0, 0, seed, null, null) };
        for (var i = 1; i < 5; i++)
            entries.Add(new(T0.AddSeconds(i), 0, null,
                keys.ToDictionary(k => k, _ => JsonSerializer.SerializeToElement(54000 + i)), null));

        var decoded = Encoding.UTF8.GetByteCount(BroadcastEncoder.BatchJson(Session, Device.DeviceGuid, T0.AddSeconds(5), entries));
        var envelope = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(
            BroadcastEncoder.Encode(Session, Device.DeviceGuid, T0.AddSeconds(5), entries)));

        Assert.True(decoded < 4096, $"decoded batch = {decoded} bytes");
        Assert.True(envelope < 2048, $"envelope = {envelope} bytes");
    }
}
