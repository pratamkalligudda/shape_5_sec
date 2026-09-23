using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>Collects samples into fixed-size batches with a per-session sequence number. One instance per session.</summary>
public sealed class BatchBuffer
{
    public const int DefaultSize = 5;

    private readonly string _sessionId;
    private readonly int _size;
    private readonly List<Sample> _pending = new();
    private long _nextSeq;

    public BatchBuffer(string sessionId, int size = DefaultSize)
    {
        _sessionId = sessionId;
        _size = size;
    }

    public TelemetryBatch? Add(Sample sample)
    {
        _pending.Add(sample);
        return _pending.Count >= _size ? Emit() : null;
    }

    public TelemetryBatch? Flush() => _pending.Count == 0 ? null : Emit();

    private TelemetryBatch Emit()
    {
        var batch = new TelemetryBatch
        {
            Properties = new BatchProperties
            {
                SessionId = _sessionId,
                BatchSeq = _nextSeq++,
                BatchStartUtc = _pending[0].SnapshotTimestampUtc,
            },
            Samples = _pending.ToList(),
        };
        _pending.Clear();
        return batch;
    }
}
