using Azure;
using Azure.Data.Tables;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Spec §5.1 step 7. ≤ 100 entities: one entity-group transaction, head first. > 100: rows upserted in chunks of
/// ≤ 100 (idempotent — deterministic keys and values), then the head alone with If-Match as the commit point.
/// </summary>
public sealed class TableLiveSessionStore(TableServiceClient service, string tableName = TableLiveSessionStore.DefaultTable)
    : ILiveSessionStore
{
    public const string DefaultTable = "livesessions";
    public const int MaxBatch = 100;

    private readonly TableClient _table = service.GetTableClient(tableName);
    private int _ensured;

    public async Task<LiveHead?> LoadHeadAsync(Guid sessionId, CancellationToken ct = default)
    {
        await EnsureTableAsync(ct);
        var r = await _table.GetEntityIfExistsAsync<TableEntity>(LiveRowMapper.PartitionKey(sessionId), LiveSessionKeys.HeadRowKey,
            cancellationToken: ct);
        return r.HasValue ? LiveRowMapper.ReadHead(r.Value!) : null;
    }

    public async Task WriteBatchAsync(Guid sessionId, LiveHead head, IReadOnlyList<ChangePointWrite> changePoints,
        IReadOnlyList<LiveEventWrite> events, CancellationToken ct = default)
    {
        await EnsureTableAsync(ct);
        var pk = LiveRowMapper.PartitionKey(sessionId);
        var rows = changePoints.Select(c => LiveRowMapper.ChangePoint(pk, c))
            .Concat(events.Select(e => LiveRowMapper.Event(pk, e)))
            .GroupBy(e => e.RowKey).Select(g => g.Last())         // a transaction rejects a repeated RowKey
            .Select(e => new TableTransactionAction(TableTransactionActionType.UpsertReplace, e))
            .ToList();
        var headEntity = LiveRowMapper.Head(pk, head);
        var headAction = head.ETag is null
            ? new TableTransactionAction(TableTransactionActionType.Add, headEntity)
            : new TableTransactionAction(TableTransactionActionType.UpdateReplace, headEntity, LiveRowMapper.ToETag(head.ETag));

        try
        {
            if (rows.Count + 1 <= MaxBatch)
            {
                await _table.SubmitTransactionAsync(rows.Prepend(headAction), ct);
                return;
            }
            foreach (var chunk in rows.Chunk(MaxBatch))
                await _table.SubmitTransactionAsync(chunk, ct);
            await _table.SubmitTransactionAsync([headAction], ct);
        }
        catch (RequestFailedException ex) when (IsHeadConflict(ex))
        {
            throw new LiveHeadConflictException($"Head row of session {sessionId} changed concurrently ({ex.Status} {ex.ErrorCode}).", ex);
        }
    }

    public async Task<int> DeletePartitionAsync(Guid sessionId, CancellationToken ct = default)
    {
        await EnsureTableAsync(ct);
        var pk = LiveRowMapper.PartitionKey(sessionId);
        var keys = new List<string>();
        await foreach (var e in _table.QueryAsync<TableEntity>(TableClient.CreateQueryFilter($"PartitionKey eq {pk}"),
                           select: ["RowKey"], cancellationToken: ct))
            keys.Add(e.RowKey);

        foreach (var chunk in keys.Chunk(MaxBatch))
            await _table.SubmitTransactionAsync(
                chunk.Select(rk => new TableTransactionAction(TableTransactionActionType.Delete, new TableEntity(pk, rk), ETag.All)), ct);
        return keys.Count;
    }

    private static bool IsHeadConflict(RequestFailedException ex) =>
        ex.Status is 409 or 412 || ex.ErrorCode is "EntityAlreadyExists" or "UpdateConditionNotSatisfied";

    private async Task EnsureTableAsync(CancellationToken ct)
    {
        if (Volatile.Read(ref _ensured) == 1) return;
        await _table.CreateIfNotExistsAsync(ct);
        Volatile.Write(ref _ensured, 1);
    }
}
