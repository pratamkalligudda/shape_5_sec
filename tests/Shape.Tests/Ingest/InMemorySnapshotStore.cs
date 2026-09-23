using System.Text.Json;
using Shape.Ingest;

namespace Shape.Tests.Ingest;

/// <summary>Store fake with real ETag semantics and injectable conflicts.</summary>
public sealed class InMemorySnapshotStore : ISnapshotStore
{
    private readonly Dictionary<string, (string Json, long Seq, bool Partial, string ETag)> _rows = new();
    private int _version;

    public int ConflictsToInject { get; set; }
    public int SaveCalls { get; private set; }

    public Task<SessionState?> LoadAsync(string sessionId, CancellationToken ct)
    {
        if (!_rows.TryGetValue(sessionId, out var row)) return Task.FromResult<SessionState?>(null);
        return Task.FromResult<SessionState?>(new SessionState
        {
            SessionId = sessionId,
            Snapshot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(row.Json)!,
            LastBatchSeq = row.Seq,
            Partial = row.Partial,
            ETag = row.ETag,
        });
    }

    public Task SaveAsync(SessionState state, CancellationToken ct)
    {
        SaveCalls++;
        if (ConflictsToInject > 0)
        {
            ConflictsToInject--;
            throw new SnapshotConflictException(state.SessionId);
        }
        var exists = _rows.TryGetValue(state.SessionId, out var current);
        if (exists ? current.ETag != state.ETag : state.ETag is not null)
            throw new SnapshotConflictException(state.SessionId);

        var etag = $"v{++_version}";
        _rows[state.SessionId] = (JsonSerializer.Serialize(state.Snapshot), state.LastBatchSeq, state.Partial, etag);
        state.ETag = etag;
        return Task.CompletedTask;
    }
}
