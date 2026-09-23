using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>Test-only faults: drop every Nth batch, and/or send adjacent batches swapped. One instance per session.</summary>
public sealed class FaultInjector(int dropEvery, bool reorder)
{
    private long _count;
    private TelemetryBatch? _held;

    public IReadOnlyList<TelemetryBatch> Process(TelemetryBatch batch)
    {
        _count++;
        if (dropEvery >= 2 && _count % dropEvery == 0) return [];
        if (!reorder) return [batch];
        if (_held is null)
        {
            _held = batch;
            return [];
        }
        var pair = new[] { batch, _held };
        _held = null;
        return pair;
    }

    public IReadOnlyList<TelemetryBatch> Drain()
    {
        if (_held is null) return [];
        var held = _held;
        _held = null;
        return [held];
    }
}
