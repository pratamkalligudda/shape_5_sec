using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

/// <summary>In-memory store with ETag semantics and injectable conflicts.</summary>
public sealed class FakeLiveSessionStore : ILiveSessionStore
{
    public LiveHead? Head { get; set; }
    public int Loads { get; private set; }
    public int Writes { get; private set; }
    public int ConflictsToThrow { get; set; }
    public List<ChangePointWrite> ChangePoints { get; } = [];
    public List<LiveEventWrite> Events { get; } = [];

    public Task<LiveHead?> LoadHeadAsync(Guid sessionId, CancellationToken ct = default)
    {
        Loads++;
        return Task.FromResult(Head);
    }

    public Task WriteBatchAsync(Guid sessionId, LiveHead head, IReadOnlyList<ChangePointWrite> changePoints,
        IReadOnlyList<LiveEventWrite> events, CancellationToken ct = default)
    {
        if (ConflictsToThrow > 0)
        {
            ConflictsToThrow--;
            throw new LiveHeadConflictException("injected", new InvalidOperationException());
        }
        if (head.ETag != Head?.ETag) throw new LiveHeadConflictException("etag", new InvalidOperationException());
        Writes++;
        Head = head with { ETag = Guid.NewGuid().ToString() };
        ChangePoints.AddRange(changePoints);
        Events.AddRange(events);
        return Task.CompletedTask;
    }

    public Task<int> DeletePartitionAsync(Guid sessionId, CancellationToken ct = default)
    {
        var n = ChangePoints.Count + Events.Count + (Head is null ? 0 : 1);
        Head = null;
        ChangePoints.Clear();
        Events.Clear();
        return Task.FromResult(n);
    }
}
