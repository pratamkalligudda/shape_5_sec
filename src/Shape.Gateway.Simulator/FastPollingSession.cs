namespace Shape.Gateway.Simulator;

/// <summary>One fast-polling session. One expiry timer, rescheduled on extend (module doc v3).</summary>
public sealed class FastPollingSession
{
    private readonly object _gate = new();
    private readonly TimeProvider _time;
    private readonly CancellationTokenSource _expiry;
    private readonly CancellationTokenSource _stop = new();
    private int _totalMinutes;

    public FastPollingSession(Guid sessionId, int pollIntervalSeconds, int durationMinutes, TimeProvider time)
    {
        SessionId = sessionId;
        PollIntervalSeconds = pollIntervalSeconds;
        _time = time;
        StartedUtc = time.GetUtcNow();
        _totalMinutes = durationMinutes;
        _expiry = new CancellationTokenSource(TimeSpan.FromMinutes(durationMinutes), time);
    }

    public Guid SessionId { get; }
    public int PollIntervalSeconds { get; }
    public DateTimeOffset StartedUtc { get; }
    public int TotalDurationMinutes { get { lock (_gate) return _totalMinutes; } }
    public CancellationToken Expired => _expiry.Token;
    public CancellationToken Stopped => _stop.Token;

    public void Stop() => _stop.Cancel();

    /// <returns>null on success; otherwise the cap-exceeded reason.</returns>
    public string? TryExtend(int minutes, int capMinutes)
    {
        lock (_gate)
        {
            var proposed = _totalMinutes + minutes;
            if (proposed > capMinutes)
                return $"Extending by {minutes} min would bring the session's total duration to {proposed} min, " +
                       $"exceeding the {capMinutes} min cap (currently {_totalMinutes} min).";
            _totalMinutes = proposed;
            var remaining = StartedUtc.AddMinutes(proposed) - _time.GetUtcNow();
            _expiry.CancelAfter(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero);
            return null;
        }
    }
}
