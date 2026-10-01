namespace Shape.Gateway.Simulator;

/// <summary>Delivery faults for validating the ordering and redelivery guards. One per session.</summary>
public sealed class FaultInjector(int dropEvery, bool reorder, bool duplicate)
{
    private long _count;
    private OutgoingBatch? _held;

    public IReadOnlyList<OutgoingBatch> Process(OutgoingBatch batch)
    {
        _count++;
        if (dropEvery >= 2 && _count % dropEvery == 0) return [];
        IReadOnlyList<OutgoingBatch> result;
        if (!reorder || batch.Seq == 0) result = [batch];   // never hold the baseline batch
        else if (_held is null) { _held = batch; result = []; }
        else { result = [batch, _held]; _held = null; }
        return duplicate ? result.SelectMany(b => new[] { b, b }).ToList() : result;
    }

    public IReadOnlyList<OutgoingBatch> Drain()
    {
        if (_held is null) return [];
        var held = _held;
        _held = null;
        return duplicate ? [held, held] : [held];
    }
}
