using System.Text.Json;

namespace Shape.Ingest;

/// <summary>Per-session folded state persisted between batches.</summary>
public sealed class SessionState
{
    public required string SessionId { get; init; }
    public Dictionary<string, JsonElement> Snapshot { get; init; } = new();
    public long LastBatchSeq { get; init; }
    /// <summary>True when the session started without a Body (orphan); stays true for the session's lifetime.</summary>
    public bool Partial { get; set; }
    /// <summary>Storage concurrency token; null means "not yet stored".</summary>
    public string? ETag { get; set; }
}
