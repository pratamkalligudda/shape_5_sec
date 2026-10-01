using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class FaultInjectorTests
{
    private static OutgoingBatch B(int seq) => new(Guid.Empty, seq, DateTime.UtcNow, []);

    private static List<int> Run(FaultInjector f, int n)
    {
        var sent = new List<int>();
        for (var i = 0; i < n; i++) sent.AddRange(f.Process(B(i)).Select(b => b.Seq));
        sent.AddRange(f.Drain().Select(b => b.Seq));
        return sent;
    }

    [Fact] public void No_faults_passes_through() => Assert.Equal(new[] { 0, 1, 2 }, Run(new FaultInjector(0, false, false), 3));
    [Fact] public void Drop_every_2_keeps_baseline() => Assert.Equal(new[] { 0, 2, 4 }, Run(new FaultInjector(2, false, false), 6));
    [Fact] public void Reorder_keeps_baseline_first_then_swaps_pairs() => Assert.Equal(new[] { 0, 2, 1, 4, 3 }, Run(new FaultInjector(0, true, false), 5));
    [Fact] public void Reorder_drains_leftover() => Assert.Equal(new[] { 0, 2, 1, 3 }, Run(new FaultInjector(0, true, false), 4));
    [Fact] public void Duplicate_sends_every_batch_twice() => Assert.Equal(new[] { 0, 0, 1, 1 }, Run(new FaultInjector(0, false, true), 2));
}
