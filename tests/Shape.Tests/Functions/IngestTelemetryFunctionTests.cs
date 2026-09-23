using Azure.Messaging.EventGrid;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.Contracts;
using Shape.Functions;
using Shape.Ingest;
using Shape.Tests.Ingest;

namespace Shape.Tests.Functions;

public class IngestTelemetryFunctionTests
{
    private const string EventJson = """
    {
      "id": "1", "eventType": "Shape.Telemetry.FastPollingBatch", "subject": "pumps/pump-001",
      "eventTime": "2026-09-23T12:00:05Z", "dataVersion": "1.0",
      "data": {
        "Properties": { "session-id": "s1", "telemetry-type": "fast-polling", "batchSeq": 0, "batchStartUtc": "2026-09-23T12:00:00Z" },
        "Samples": [
          { "snapshotTimestampUtc": "2026-09-23T12:00:00Z", "Body": { "OperatingPressurePSI": 54000, "MotorRunning": true } },
          { "snapshotTimestampUtc": "2026-09-23T12:00:01Z", "Changes": { "OperatingPressurePSI": 54360 } }
        ]
      }
    }
    """;

    private static IngestTelemetryFunction Function(ISnapshotStore store) =>
        new(new TelemetryIngestor(store, NullLogger<TelemetryIngestor>.Instance));

    [Fact]
    public async Task Real_event_grid_event_produces_batch_message()
    {
        var ev = EventGridEvent.Parse(BinaryData.FromString(EventJson));

        var action = await Function(new InMemorySnapshotStore()).Run(ev, CancellationToken.None);

        Assert.NotNull(action);
        Assert.Equal("batch", action!.Target);
        var folded = Assert.IsType<FoldedBatch>(Assert.Single(action.Arguments!));
        Assert.Equal("s1", folded.SessionId);
        Assert.Equal(2, folded.Samples.Count);
        Assert.Equal(54360, folded.Samples[1].Values["OperatingPressurePSI"].GetInt32());
        Assert.True(folded.Samples[1].Values["MotorRunning"].GetBoolean());
    }

    [Fact]
    public async Task Stale_redelivery_produces_no_message()
    {
        var store = new InMemorySnapshotStore();
        var function = Function(store);
        await function.Run(EventGridEvent.Parse(BinaryData.FromString(EventJson)), CancellationToken.None);

        var second = await function.Run(EventGridEvent.Parse(BinaryData.FromString(EventJson)), CancellationToken.None);

        Assert.Null(second);
    }
}
