using System.Text.Json;
using Azure;
using Azure.Data.Tables;
using Shape.LiveBackend.Api;

namespace Shape.LiveBackend.LiveData;

/// <summary>
/// Decodes the columns the ingestion writer produces (spec §5.6): change point V + T (b/i/d/s, n = null),
/// head row with Snapshot JSON, event rows with Label absent when null.
/// </summary>
public static class LiveRowReader
{
    private static readonly JsonElement Null = JsonDocument.Parse("null").RootElement.Clone();

    public static JsonElement Value(TableEntity e) => e.GetString("T") switch
    {
        "b" => JsonSerializer.SerializeToElement(e.GetBoolean("V")),
        "i" => JsonSerializer.SerializeToElement(e.GetInt64("V")),
        "d" => JsonSerializer.SerializeToElement(e.GetDouble("V")),
        "s" => JsonSerializer.SerializeToElement(e.GetString("V")),
        _ => Null,
    };

    public static LiveHeadView Head(TableEntity e) => new(
        JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(e.GetString("Snapshot") ?? "{}")!,
        e.GetInt32("DeviceStatus"),
        e.GetDateTimeOffset("LastSnapshotTimestampUtc"),
        e.GetInt64("TickCount") ?? 0,
        e.GetInt64("BytesTotal") ?? 0,
        e.GetDateTimeOffset("FirstTickUtc"),
        e.GetDateTimeOffset("LastTickUtc"));

    public static LiveEventDto Event(TableEntity e) => new(
        e.GetDateTimeOffset("TimestampUtc")!.Value,
        e.GetString("Parameter")!,
        e.GetInt64("Value") ?? 0,
        e.GetString("State")!,
        e.GetString("EventType")!,
        e.GetString("Domain")!,
        e.GetString("Severity")!,
        e.GetString("Label"));
}

public sealed class TableLiveSessionReader(TableServiceClient service, string tableName = TableLiveSessionReader.DefaultTable)
    : ILiveSessionReader
{
    public const string DefaultTable = "livesessions";
    private readonly TableClient _table = service.GetTableClient(tableName);

    public async Task<LiveHeadView?> ReadHeadAsync(Guid sessionId, CancellationToken ct = default)
    {
        try
        {
            var r = await _table.GetEntityIfExistsAsync<TableEntity>(sessionId.ToString(), LiveSessionKeys.HeadRowKey, cancellationToken: ct);
            return r.HasValue ? LiveRowReader.Head(r.Value!) : null;
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
            return null;   // the table does not exist until the first batch is written
        }
    }

    public async Task<IReadOnlyList<ChangePoint>> ReadParameterAsync(Guid sessionId, string parameter, CancellationToken ct = default)
    {
        var (from, to) = LiveSessionKeys.ParamRange(parameter);
        return (await QueryAsync(sessionId, from, to, ct))
            .Select(e => new ChangePoint(e.GetDateTimeOffset("TimestampUtc")!.Value, LiveRowReader.Value(e)))
            .ToList();
    }

    public async Task<IReadOnlyList<LiveEventDto>> ReadEventsAsync(Guid sessionId, CancellationToken ct = default)
    {
        var (from, to) = LiveSessionKeys.EventRange();
        return (await QueryAsync(sessionId, from, to, ct)).Select(LiveRowReader.Event).ToList();
    }

    /// <summary>One partition, one RowKey range. Table Storage returns rows in RowKey order, i.e. chronologically.</summary>
    private async Task<List<TableEntity>> QueryAsync(Guid sessionId, string from, string to, CancellationToken ct)
    {
        var rows = new List<TableEntity>();
        var filter = TableClient.CreateQueryFilter($"PartitionKey eq {sessionId.ToString()} and RowKey ge {from} and RowKey lt {to}");
        try
        {
            await foreach (var e in _table.QueryAsync<TableEntity>(filter, cancellationToken: ct))
                rows.Add(e);
        }
        catch (RequestFailedException ex) when (ex.Status == 404)
        {
        }
        return rows;
    }
}
