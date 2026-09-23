using Shape.Contracts;
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class BatchBufferTests
{
    private static Sample S(int second) => new()
    {
        SnapshotTimestampUtc = DateTimeOffset.Parse("2026-09-23T12:00:00Z").AddSeconds(second),
        Changes = new(),
    };

    [Fact]
    public void Emits_batch_on_fifth_sample_only()
    {
        var buffer = new BatchBuffer("s1");

        for (var i = 0; i < 4; i++) Assert.Null(buffer.Add(S(i)));
        var batch = buffer.Add(S(4));

        Assert.NotNull(batch);
        Assert.Equal(5, batch!.Samples!.Count);
        Assert.Equal("s1", batch.Properties!.SessionId);
        Assert.Equal("fast-polling", batch.Properties.TelemetryType);
        Assert.Equal(S(0).SnapshotTimestampUtc, batch.Properties.BatchStartUtc);
    }

    [Fact]
    public void BatchSeq_starts_at_zero_and_increments()
    {
        var buffer = new BatchBuffer("s1");
        var seqs = new List<long>();

        for (var i = 0; i < 15; i++)
            if (buffer.Add(S(i)) is { } b) seqs.Add(b.Properties!.BatchSeq);

        Assert.Equal(new[] { 0L, 1L, 2L }, seqs);
    }

    [Fact]
    public void Flush_emits_partial_batch_then_nothing()
    {
        var buffer = new BatchBuffer("s1");
        buffer.Add(S(0));
        buffer.Add(S(1));

        var partial = buffer.Flush();

        Assert.Equal(2, partial!.Samples!.Count);
        Assert.Null(buffer.Flush());
    }

    [Fact]
    public void Flush_on_empty_buffer_returns_null()
    {
        Assert.Null(new BatchBuffer("s1").Flush());
    }

    [Fact]
    public void New_buffer_for_new_session_restarts_seq()
    {
        var first = new BatchBuffer("s1");
        for (var i = 0; i < 10; i++) first.Add(S(i));

        var second = new BatchBuffer("s2");
        for (var i = 0; i < 4; i++) second.Add(S(i));
        var batch = second.Add(S(4))!;

        Assert.Equal(0, batch.Properties!.BatchSeq);
        Assert.Equal("s2", batch.Properties.SessionId);
    }
}
