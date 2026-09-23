namespace Shape.Ingest;

public interface ISnapshotStore
{
    Task<SessionState?> LoadAsync(string sessionId, CancellationToken ct);

    /// <summary>
    /// Inserts when <c>state.ETag</c> is null, otherwise replaces only if the stored ETag still matches.
    /// Sets <c>state.ETag</c> to the new value. Throws <see cref="SnapshotConflictException"/> on conflict.
    /// </summary>
    Task SaveAsync(SessionState state, CancellationToken ct);
}

public sealed class SnapshotConflictException(string sessionId, Exception? inner = null)
    : Exception($"Snapshot for session '{sessionId}' was modified concurrently.", inner);
