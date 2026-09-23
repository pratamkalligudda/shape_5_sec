using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.Contracts;
using Shape.Ingest;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Ingest;

public class TelemetryIngestorTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-09-23T12:00:00Z");

    private static string Json(long seq, bool withBody, string session = "s1")
    {
        var first = withBody
            ? new Sample { SnapshotTimestampUtc = T0, Body = Map(("A", 1), ("B", 2)) }
            : new Sample { SnapshotTimestampUtc = T0, Changes = Map(("A", (int)seq)) };
        var samples = new List<Sample> { first };
        for (var i = 1; i < 5; i++) samples.Add(new Sample { SnapshotTimestampUtc = T0.AddSeconds(i), Changes = new() });
        return JsonSerializer.Serialize(new TelemetryBatch
        {
            Properties = new BatchProperties { SessionId = session, BatchSeq = seq, BatchStartUtc = T0 },
            Samples = samples,
        }, ShapeJson.Options);
    }

    private static TelemetryIngestor Ingestor(ISnapshotStore store) => new(store, NullLogger<TelemetryIngestor>.Instance);

    [Fact]
    public async Task First_batch_is_folded_saved_and_returned()
    {
        var store = new InMemorySnapshotStore();

        var folded = await Ingestor(store).IngestAsync(Json(0, withBody: true), CancellationToken.None);

        Assert.NotNull(folded);
        Assert.Equal(5, folded!.Samples.Count);
        Assert.Equal(0, (await store.LoadAsync("s1", CancellationToken.None))!.LastBatchSeq);
    }

    [Fact]
    public async Task State_survives_across_ingestor_instances()
    {
        var store = new InMemorySnapshotStore();
        await Ingestor(store).IngestAsync(Json(0, withBody: true), CancellationToken.None);

        var folded = await Ingestor(store).IngestAsync(Json(1, withBody: false), CancellationToken.None);

        Assert.False(folded!.Gap);
        Assert.Equal(2, folded.Samples[0].Values["B"].GetInt32()); // B carried over from the Body
    }

    [Fact]
    public async Task Duplicate_delivery_is_dropped_without_push()
    {
        var store = new InMemorySnapshotStore();
        var ingestor = Ingestor(store);
        await ingestor.IngestAsync(Json(0, withBody: true), CancellationToken.None);
        await ingestor.IngestAsync(Json(1, withBody: false), CancellationToken.None);
        var savesBefore = store.SaveCalls;

        var again = await ingestor.IngestAsync(Json(1, withBody: false), CancellationToken.None);

        Assert.Null(again);
        Assert.Equal(savesBefore, store.SaveCalls);
        Assert.Equal(1, (await store.LoadAsync("s1", CancellationToken.None))!.LastBatchSeq);
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("null")]
    [InlineData("{}")]
    [InlineData("""{"Properties":{"session-id":"s1","batchSeq":0},"Samples":[]}""")]
    public async Task Malformed_data_returns_null_without_throwing(string json)
    {
        var store = new InMemorySnapshotStore();

        Assert.Null(await Ingestor(store).IngestAsync(json, CancellationToken.None));
        Assert.Equal(0, store.SaveCalls);
    }

    [Fact]
    public async Task Conflict_is_retried_and_then_succeeds()
    {
        var store = new InMemorySnapshotStore { ConflictsToInject = 2 };

        var folded = await Ingestor(store).IngestAsync(Json(0, withBody: true), CancellationToken.None);

        Assert.NotNull(folded);
        Assert.Equal(3, store.SaveCalls);
    }

    [Fact]
    public async Task Third_conflict_propagates()
    {
        var store = new InMemorySnapshotStore { ConflictsToInject = 3 };

        await Assert.ThrowsAsync<SnapshotConflictException>(
            () => Ingestor(store).IngestAsync(Json(0, withBody: true), CancellationToken.None));
    }
}
