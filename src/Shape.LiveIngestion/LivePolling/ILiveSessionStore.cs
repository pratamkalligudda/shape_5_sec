namespace Shape.LiveIngestion.LivePolling;

public interface ILiveSessionStore
{
    Task<LiveHead?> LoadHeadAsync(Guid sessionId, CancellationToken ct = default);

    /// <summary>Commits one folded batch. Throws <see cref="LiveHeadConflictException"/> when the head moved.</summary>
    Task WriteBatchAsync(Guid sessionId, LiveHead head, IReadOnlyList<ChangePointWrite> changePoints,
        IReadOnlyList<LiveEventWrite> events, CancellationToken ct = default);

    /// <summary>Deletes every row of the session's partition. Returns the number deleted.</summary>
    Task<int> DeletePartitionAsync(Guid sessionId, CancellationToken ct = default);
}

public sealed class LiveHeadConflictException(string message, Exception inner) : Exception(message, inner);
