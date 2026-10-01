namespace Shape.Gateway.Simulator;

/// <summary>Buffers entries into batches of 5 (module BatchSize). One per session.</summary>
public sealed class BatchBuffer(Guid sessionId, int size = 5)
{
    private readonly List<GatewayEntry> _pending = new();
    private int _seq;

    public OutgoingBatch? Add(GatewayEntry entry, DateTimeOffset now)
    {
        _pending.Add(entry);
        return _pending.Count >= size ? Emit(now) : null;
    }

    public OutgoingBatch? Flush(DateTimeOffset now) => _pending.Count == 0 ? null : Emit(now);

    private OutgoingBatch Emit(DateTimeOffset now)
    {
        var batch = new OutgoingBatch(sessionId, _seq++, now.UtcDateTime, _pending.ToList());
        _pending.Clear();
        return batch;
    }
}
