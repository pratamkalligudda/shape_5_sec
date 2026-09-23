using System.Text.Json;
using Azure;
using Azure.Data.Tables;

namespace Shape.Ingest;

/// <summary>Table storage: PK = session id, RK = "state". Works against Azurite and Azure.</summary>
public sealed class TableSnapshotStore(TableClient table) : ISnapshotStore
{
    public const string DefaultTableName = "sessionsnapshots";
    private const string RowKey = "state";
    private int _tableReady;

    public async Task<SessionState?> LoadAsync(string sessionId, CancellationToken ct)
    {
        await EnsureTableAsync(ct);
        var response = await table.GetEntityIfExistsAsync<TableEntity>(sessionId, RowKey, cancellationToken: ct);
        if (!response.HasValue) return null;
        var e = response.Value!;
        return new SessionState
        {
            SessionId = sessionId,
            Snapshot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(e.GetString("SnapshotJson"))!,
            LastBatchSeq = e.GetInt64("LastBatchSeq") ?? 0,
            Partial = e.GetBoolean("Partial") ?? false,
            ETag = e.ETag.ToString(),
        };
    }

    public async Task SaveAsync(SessionState state, CancellationToken ct)
    {
        await EnsureTableAsync(ct);
        var entity = new TableEntity(state.SessionId, RowKey)
        {
            ["SnapshotJson"] = JsonSerializer.Serialize(state.Snapshot),
            ["LastBatchSeq"] = state.LastBatchSeq,
            ["Partial"] = state.Partial,
        };
        try
        {
            var response = state.ETag is null
                ? await table.AddEntityAsync(entity, ct)
                : await table.UpdateEntityAsync(entity, new ETag(state.ETag), TableUpdateMode.Replace, ct);
            state.ETag = response.Headers.ETag?.ToString();
        }
        catch (RequestFailedException ex) when (ex.Status is 409 or 412)
        {
            throw new SnapshotConflictException(state.SessionId, ex);
        }
    }

    private async Task EnsureTableAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _tableReady) == 1) return;
        await table.CreateIfNotExistsAsync(ct);
        Volatile.Write(ref _tableReady, 1);
    }
}
