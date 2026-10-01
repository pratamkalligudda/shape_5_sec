using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class BatchBufferTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    private static GatewayEntry Entry(int s) => new() { TimestampUtc = T0.AddSeconds(s).UtcDateTime, Changes = new() };

    [Fact]
    public void Emits_on_fifth_entry_with_increasing_seq()
    {
        var id = Guid.NewGuid();
        var buffer = new BatchBuffer(id);
        var batches = new List<OutgoingBatch>();
        for (var i = 0; i < 10; i++)
            if (buffer.Add(Entry(i), T0.AddSeconds(i)) is { } b) batches.Add(b);

        Assert.Equal(2, batches.Count);
        Assert.Equal(new[] { 0, 1 }, batches.Select(b => b.Seq));
        Assert.All(batches, b => Assert.Equal(5, b.Entries.Count));
        Assert.All(batches, b => Assert.Equal(id, b.SessionId));
        Assert.Equal(T0.AddSeconds(4).UtcDateTime, batches[0].SentUtc);
    }

    [Fact]
    public void Flush_emits_partial_then_nothing()
    {
        var buffer = new BatchBuffer(Guid.NewGuid());
        buffer.Add(Entry(0), T0);
        buffer.Add(Entry(1), T0);

        Assert.Equal(2, buffer.Flush(T0)!.Entries.Count);
        Assert.Null(buffer.Flush(T0));
    }
}
