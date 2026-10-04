using System.Text.Json;
using Azure.Data.Tables;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;
using static Shape.LiveIngestion.Tests.FoldBuilders;

namespace Shape.LiveIngestion.Tests;

[Trait("Category", "Integration")]
public class TableLiveSessionStoreTests
{
    private static readonly IReadOnlyDictionary<string, AlertDefinition> Catalog = TestCatalog.Load();
    private readonly string _table = DevStorage.UniqueTable();
    private readonly TableLiveSessionStore _store;
    private readonly Guid _session = Guid.NewGuid();

    public TableLiveSessionStoreTests() => _store = new TableLiveSessionStore(DevStorage.Service(), _table);

    private static FoldResult Fold(LiveHead? head, params LiveEntry[] entries) =>
        SnapshotFolder.Fold(head, Batch(entries), Device, Catalog, NullLogger.Instance);

    private static FoldResult SmallFirst() => Fold(null,
        Base(0, """{"P":100,"OperatingAlarmActive":0,"OK":true,"S":"a"}""", """{"OperatingAlarmActive":0,"MotorRunning":1}"""),
        Delta(1, """{"P":101}"""), Delta(2), Delta(3, """{"OperatingAlarmActive":6}""", """{"OperatingAlarmActive":6}"""), Delta(4));

    private static FoldResult LargeFirst()
    {
        var keys = new[] { "OperatingPressurePSI", "OperatingPressureBar", "StrokeRateLeft", "StrokeRateRight",
            "StrokeTimeLeft", "StrokeTimeRight", "StrokeCounterTW1", "ProportionalOutputValue" };
        string D(int i) => JsonSerializer.Serialize(keys.ToDictionary(k => k, _ => i));
        return Fold(null, Base(0, JsonSerializer.Serialize(Seed()), """{"MotorRunning":1}"""),
            Delta(1, D(1)), Delta(2, D(2)), Delta(3, D(3)), Delta(4, D(4)));
    }

    private Task Write(FoldResult r) => _store.WriteBatchAsync(_session, r.Head, r.ChangePoints, r.Events);

    private async Task<List<TableEntity>> Rows()
    {
        var client = DevStorage.Service().GetTableClient(_table);
        var rows = new List<TableEntity>();
        await foreach (var e in client.QueryAsync<TableEntity>(TableClient.CreateQueryFilter($"PartitionKey eq {_session.ToString()}")))
            rows.Add(e);
        return rows;
    }

    [Fact]
    public async Task Load_head_returns_null_for_a_new_session() =>
        Assert.Null(await _store.LoadHeadAsync(_session));

    [Fact]
    public async Task Small_batch_commits_head_and_rows_together()
    {
        var r = SmallFirst();
        Assert.True(r.EntityCount <= 100);

        await Write(r);

        var head = await _store.LoadHeadAsync(_session);
        Assert.NotNull(head);
        Assert.NotNull(head!.ETag);
        Assert.Equal(2, head.DeviceStatus);
        Assert.Equal(101, head.Snapshot["P"].GetInt32());
        Assert.Equal(r.EntityCount, (await Rows()).Count);
    }

    [Fact]
    public async Task Next_batch_with_the_loaded_etag_commits()
    {
        await Write(SmallFirst());
        var head = await _store.LoadHeadAsync(_session);

        var next = Fold(head, Delta(5, """{"P":102}"""), Delta(6), Delta(7), Delta(8), Delta(9));
        await Write(next);

        var after = await _store.LoadHeadAsync(_session);
        Assert.Equal(10, after!.TickCount);
        Assert.NotEqual(head!.ETag, after.ETag);
    }

    [Fact]
    public async Task Stale_etag_conflicts_and_the_transaction_writes_no_rows()
    {
        await Write(SmallFirst());
        var head = await _store.LoadHeadAsync(_session);
        await Write(Fold(head, Delta(5, """{"P":102}""")));               // moves the ETag on
        var before = (await Rows()).Count;

        var stale = Fold(head, Delta(5, """{"P":999}"""), Delta(6, """{"Q":1}"""));
        await Assert.ThrowsAsync<LiveHeadConflictException>(() => Write(stale));

        Assert.Equal(before, (await Rows()).Count);
        Assert.DoesNotContain(await Rows(), e => e.RowKey.StartsWith("Q|"));
    }

    [Fact]
    public async Task Two_writers_adding_the_head_conflict()
    {
        await Write(SmallFirst());
        await Assert.ThrowsAsync<LiveHeadConflictException>(() => Write(SmallFirst()));
    }

    [Fact]
    public async Task Large_first_batch_takes_the_chunked_path_and_commits()
    {
        var r = LargeFirst();
        Assert.True(r.EntityCount > 100);

        await Write(r);

        Assert.Equal(r.EntityCount, (await Rows()).Count);
        Assert.Equal(5, (await _store.LoadHeadAsync(_session))!.TickCount);
    }

    [Fact]
    public async Task Chunked_commit_retried_after_head_conflict_produces_no_duplicate_rows()
    {
        var r = LargeFirst();
        await Write(r);
        var stored = await _store.LoadHeadAsync(_session);

        // A redelivered copy with a stale view of the head: rows are upserted, then the head add conflicts.
        await Assert.ThrowsAsync<LiveHeadConflictException>(() => Write(r));
        // The retry re-folds on the stored head: every entry is not-newer, so nothing is written.
        var retry = Fold(stored, Delta(4));
        Assert.False(retry.HasWrites);

        var rows = await Rows();
        Assert.Equal(r.EntityCount, rows.Count);
        Assert.Equal(rows.Count, rows.Select(e => e.RowKey).Distinct().Count());
    }

    [Fact]
    public async Task Delete_partition_removes_more_than_100_rows()
    {
        var r = LargeFirst();
        await Write(r);

        var deleted = await _store.DeletePartitionAsync(_session);

        Assert.Equal(r.EntityCount, deleted);
        Assert.Empty(await Rows());
        Assert.Null(await _store.LoadHeadAsync(_session));
    }
}
