using Shape.Contracts;
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class FaultInjectorTests
{
    private static TelemetryBatch B(long seq) => new()
    {
        Properties = new BatchProperties { SessionId = "s1", BatchSeq = seq },
        Samples = [new Sample { Changes = new() }],
    };

    private static List<long> Run(FaultInjector f, int count)
    {
        var sent = new List<long>();
        for (var i = 0; i < count; i++) sent.AddRange(f.Process(B(i)).Select(b => b.Properties!.BatchSeq));
        sent.AddRange(f.Drain().Select(b => b.Properties!.BatchSeq));
        return sent;
    }

    [Fact]
    public void No_faults_passes_everything_through_in_order()
    {
        Assert.Equal(new[] { 0L, 1, 2, 3 }, Run(new FaultInjector(0, false), 4));
    }

    [Fact]
    public void Drop_every_2_keeps_baseline_and_drops_odd_batches()
    {
        Assert.Equal(new[] { 0L, 2, 4 }, Run(new FaultInjector(2, false), 6));
    }

    [Fact]
    public void Reorder_keeps_baseline_first_then_swaps_adjacent_pairs()
    {
        // Batch 0 carries the Body; swapping it away would turn the whole session into a partial orphan.
        Assert.Equal(new[] { 0L, 2, 1, 4, 3 }, Run(new FaultInjector(0, true), 5));
    }

    [Fact]
    public void Reorder_drains_leftover_at_session_end()
    {
        Assert.Equal(new[] { 0L, 2, 1, 3 }, Run(new FaultInjector(0, true), 4));
    }

    [Fact]
    public void Drop_applies_before_reorder()
    {
        // dropped: 2,5 → survivors 0,1,3,4 → 0 passes, then (1,3) swapped, 4 drained
        Assert.Equal(new[] { 0L, 3, 1, 4 }, Run(new FaultInjector(3, true), 6));
    }
}
