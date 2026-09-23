using System.Text.Json;
using Shape.Contracts;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Contracts;

public class ContractsSerializationTests
{
    [Fact]
    public void Delta_sample_serializes_Changes_and_omits_Body()
    {
        var sample = new Sample
        {
            SnapshotTimestampUtc = DateTimeOffset.Parse("2026-09-23T12:00:01Z"),
            Changes = Map(("OperatingPressurePSI", 54360)),
        };

        var json = JsonSerializer.Serialize(sample, ShapeJson.Options);

        Assert.Contains("\"snapshotTimestampUtc\"", json);
        Assert.Contains("\"Changes\":{\"OperatingPressurePSI\":54360}", json);
        Assert.DoesNotContain("\"Body\"", json);
    }

    [Fact]
    public void Heartbeat_serializes_empty_Changes_object()
    {
        var sample = new Sample { SnapshotTimestampUtc = DateTimeOffset.UnixEpoch, Changes = new() };

        var json = JsonSerializer.Serialize(sample, ShapeJson.Options);

        Assert.Contains("\"Changes\":{}", json);
    }

    [Fact]
    public void Deserializes_wire_packet_from_spec()
    {
        const string json = """
        {
          "Properties": { "session-id": "cc27db6b-508b-488e-a816-faae684491d8", "telemetry-type": "fast-polling",
                          "batchSeq": 3, "batchStartUtc": "2026-09-23T12:00:00Z" },
          "Samples": [
            { "snapshotTimestampUtc": "2026-09-23T12:00:00Z", "Body": { "MotorRunning": true } },
            { "snapshotTimestampUtc": "2026-09-23T12:00:01Z", "Changes": {} }
          ]
        }
        """;

        var batch = JsonSerializer.Deserialize<TelemetryBatch>(json, ShapeJson.Options)!;

        Assert.Equal("cc27db6b-508b-488e-a816-faae684491d8", batch.Properties!.SessionId);
        Assert.Equal("fast-polling", batch.Properties.TelemetryType);
        Assert.Equal(3, batch.Properties.BatchSeq);
        Assert.Equal(2, batch.Samples!.Count);
        Assert.True(batch.Samples[0].Body!["MotorRunning"].GetBoolean());
        Assert.Null(batch.Samples[0].Changes);
        Assert.Empty(batch.Samples[1].Changes!);
    }

    [Fact]
    public void Folded_batch_uses_camelCase_push_names()
    {
        var folded = new FoldedBatch
        {
            SessionId = "s1", BatchSeq = 7, Gap = true, Partial = false,
            Samples = [new FoldedSample { Ts = DateTimeOffset.UnixEpoch, Values = Map(("A", 1)), Changed = ["A"] }],
        };

        var json = JsonSerializer.Serialize(folded, ShapeJson.Options);

        Assert.Contains("\"sessionId\":\"s1\"", json);
        Assert.Contains("\"batchSeq\":7", json);
        Assert.Contains("\"gap\":true", json);
        Assert.Contains("\"partial\":false", json);
        Assert.Contains("\"samples\":[{\"ts\":", json);
        Assert.Contains("\"values\":{\"A\":1}", json);
        Assert.Contains("\"changed\":[\"A\"]", json);
    }

    [Fact]
    public void Seed_loads_all_73_fields()
    {
        var seed = Seed();

        Assert.Equal(73, seed.Count);
        Assert.Equal(50623, seed["OperatingPressurePSI"].GetInt32());
        Assert.Equal("0.0.103.9", seed["IPAddressRemoteStation"].GetString());
    }
}
