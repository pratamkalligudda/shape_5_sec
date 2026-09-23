using System.Text.Json;
using Shape.Contracts;
using Shape.Ingest;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Ingest;

public class SnapshotFolderTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-09-23T12:00:00Z");

    private static Sample BodySample(int sec, params (string, object)[] fields) =>
        new() { SnapshotTimestampUtc = T0.AddSeconds(sec), Body = Map(fields) };

    private static Sample Delta(int sec, params (string, object)[] fields) =>
        new() { SnapshotTimestampUtc = T0.AddSeconds(sec), Changes = Map(fields) };

    private static TelemetryBatch Batch(long seq, params Sample[] samples) => new()
    {
        Properties = new BatchProperties { SessionId = "s1", BatchSeq = seq, BatchStartUtc = samples[0].SnapshotTimestampUtc },
        Samples = samples,
    };

    private static SessionState State(long lastSeq, params (string, object)[] snapshot) => new()
    {
        SessionId = "s1", LastBatchSeq = lastSeq, Snapshot = Map(snapshot), ETag = "etag-1",
    };

    private static int Int(FoldedSample s, string key) => s.Values[key].GetInt32();

    [Fact]
    public void New_session_folds_body_then_deltas_into_full_samples()
    {
        var batch = Batch(0,
            BodySample(0, ("A", 1), ("B", 10)),
            Delta(1, ("A", 2)),
            Delta(2),
            Delta(3, ("B", 11)),
            Delta(4, ("A", 3), ("B", 12)));

        var result = SnapshotFolder.Fold(null, batch);

        Assert.Equal(FoldOutcome.NewSession, result.Outcome);
        var f = result.Folded!;
        Assert.Equal("s1", f.SessionId);
        Assert.Equal(0, f.BatchSeq);
        Assert.False(f.Gap);
        Assert.False(f.Partial);
        Assert.Equal(new[] { 1, 2, 2, 2, 3 }, f.Samples.Select(s => Int(s, "A")));
        Assert.Equal(new[] { 10, 10, 10, 11, 12 }, f.Samples.Select(s => Int(s, "B")));
        Assert.Equal(new[] { "A", "B" }, f.Samples[0].Changed);
        Assert.Equal(new[] { "A" }, f.Samples[1].Changed);
        Assert.Empty(f.Samples[2].Changed);
        Assert.Equal(T0.AddSeconds(3), f.Samples[3].Ts);
        Assert.Equal(0, result.NewState!.LastBatchSeq);
        Assert.Equal(3, result.NewState.Snapshot["A"].GetInt32());
        Assert.Null(result.NewState.ETag);
    }

    [Fact]
    public void Next_batch_in_sequence_is_Normal_and_folds_onto_stored_snapshot()
    {
        var result = SnapshotFolder.Fold(State(4, ("A", 1), ("B", 10)), Batch(5, Delta(0, ("A", 5))));

        Assert.Equal(FoldOutcome.Normal, result.Outcome);
        Assert.False(result.Folded!.Gap);
        Assert.Equal(5, Int(result.Folded.Samples[0], "A"));
        Assert.Equal(10, Int(result.Folded.Samples[0], "B"));
        Assert.Equal(5, result.NewState!.LastBatchSeq);
        Assert.Equal("etag-1", result.NewState.ETag);
    }

    [Fact]
    public void Skipped_seq_is_Gap_but_still_folds()
    {
        var result = SnapshotFolder.Fold(State(4, ("A", 1)), Batch(7, Delta(0, ("A", 9))));

        Assert.Equal(FoldOutcome.Gap, result.Outcome);
        Assert.True(result.Folded!.Gap);
        Assert.Equal(9, Int(result.Folded.Samples[0], "A"));
        Assert.Equal(7, result.NewState!.LastBatchSeq);
    }

    [Theory]
    [InlineData(4)]
    [InlineData(2)]
    public void Old_or_duplicate_seq_is_Stale_with_no_output(long seq)
    {
        var result = SnapshotFolder.Fold(State(4, ("A", 1)), Batch(seq, Delta(0, ("A", 9))));

        Assert.Equal(FoldOutcome.Stale, result.Outcome);
        Assert.Null(result.Folded);
        Assert.Null(result.NewState);
    }

    [Fact]
    public void Unknown_session_without_body_is_Orphan_partial_gap()
    {
        var result = SnapshotFolder.Fold(null, Batch(12, Delta(0, ("A", 1)), Delta(1, ("B", 2))));

        Assert.Equal(FoldOutcome.Orphan, result.Outcome);
        Assert.True(result.Folded!.Gap);
        Assert.True(result.Folded.Partial);
        Assert.Single(result.Folded.Samples[0].Values);
        Assert.Equal(2, result.Folded.Samples[1].Values.Count);
        Assert.True(result.NewState!.Partial);
    }

    [Fact]
    public void Partial_flag_persists_on_later_batches()
    {
        var state = State(12, ("A", 1));
        state.Partial = true;

        var result = SnapshotFolder.Fold(state, Batch(13, Delta(0)));

        Assert.Equal(FoldOutcome.Normal, result.Outcome);
        Assert.True(result.Folded!.Partial);
        Assert.True(result.NewState!.Partial);
    }

    [Fact]
    public void Folds_partial_final_batch()
    {
        var result = SnapshotFolder.Fold(State(4, ("A", 1)), Batch(5, Delta(0, ("A", 2)), Delta(1)));

        Assert.Equal(2, result.Folded!.Samples.Count);
        Assert.Equal(new[] { 2, 2 }, result.Folded.Samples.Select(s => Int(s, "A")));
    }

    [Fact]
    public void Output_samples_are_independent_copies()
    {
        var state = State(0, ("A", 1));
        var result = SnapshotFolder.Fold(state, Batch(1, Delta(0, ("A", 2)), Delta(1, ("A", 3))));

        result.Folded!.Samples[0].Values["A"] = J(999);

        Assert.Equal(3, Int(result.Folded.Samples[1], "A"));
        Assert.Equal(3, result.NewState!.Snapshot["A"].GetInt32());
        Assert.Equal(1, state.Snapshot["A"].GetInt32()); // input state not mutated
    }

    [Fact]
    public void Invalid_batches_are_rejected()
    {
        var noProps = new TelemetryBatch { Samples = [Delta(0)] };
        var noSamples = new TelemetryBatch { Properties = new BatchProperties { SessionId = "s1" }, Samples = [] };
        var both = Batch(0, new Sample { Body = Map(("A", 1)), Changes = Map(("A", 1)) });
        var neither = Batch(0, new Sample { SnapshotTimestampUtc = T0 });
        var noSession = new TelemetryBatch { Properties = new BatchProperties { SessionId = "" }, Samples = [Delta(0)] };

        foreach (var b in new[] { noProps, noSamples, both, neither, noSession })
            Assert.Equal(FoldOutcome.Invalid, SnapshotFolder.Fold(null, b).Outcome);
    }
}
