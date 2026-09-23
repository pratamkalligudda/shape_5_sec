using Azure.Data.Tables;
using Shape.Ingest;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Ingest;

/// <summary>Requires Azurite: `azurite --silent --location .azurite --blobPort 10100 --queuePort 10101 --tablePort 10102`.</summary>
[Trait("Category", "Integration")]
public class TableSnapshotStoreTests
{
    private static TableSnapshotStore NewStore() =>
        new(new TableServiceClient(DevStorage.ConnectionString).GetTableClient($"t{Guid.NewGuid():N}"));

    [Fact]
    public async Task Load_returns_null_for_unknown_session()
    {
        Assert.Null(await NewStore().LoadAsync("nope", CancellationToken.None));
    }

    [Fact]
    public async Task Insert_load_update_round_trip()
    {
        var store = NewStore();
        var state = new SessionState { SessionId = "s1", Snapshot = Map(("A", 1), ("Ip", "0.0.103.9")), LastBatchSeq = 0 };

        await store.SaveAsync(state, CancellationToken.None);
        Assert.NotNull(state.ETag);

        var loaded = (await store.LoadAsync("s1", CancellationToken.None))!;
        Assert.Equal(0, loaded.LastBatchSeq);
        Assert.Equal("0.0.103.9", loaded.Snapshot["Ip"].GetString());
        Assert.False(loaded.Partial);

        var next = new SessionState { SessionId = "s1", Snapshot = Map(("A", 2)), LastBatchSeq = 1, Partial = true, ETag = loaded.ETag };
        await store.SaveAsync(next, CancellationToken.None);

        var reloaded = (await store.LoadAsync("s1", CancellationToken.None))!;
        Assert.Equal(1, reloaded.LastBatchSeq);
        Assert.True(reloaded.Partial);
        Assert.Equal(2, reloaded.Snapshot["A"].GetInt32());
    }

    [Fact]
    public async Task Stale_etag_throws_conflict()
    {
        var store = NewStore();
        var state = new SessionState { SessionId = "s1", Snapshot = Map(("A", 1)) };
        await store.SaveAsync(state, CancellationToken.None);
        var staleEtag = state.ETag;
        await store.SaveAsync(new SessionState { SessionId = "s1", Snapshot = Map(("A", 2)), LastBatchSeq = 1, ETag = staleEtag }, CancellationToken.None);

        await Assert.ThrowsAsync<SnapshotConflictException>(() => store.SaveAsync(
            new SessionState { SessionId = "s1", Snapshot = Map(("A", 3)), LastBatchSeq = 2, ETag = staleEtag }, CancellationToken.None));
    }

    [Fact]
    public async Task Concurrent_first_insert_throws_conflict()
    {
        var store = NewStore();
        await store.SaveAsync(new SessionState { SessionId = "s1", Snapshot = Map(("A", 1)) }, CancellationToken.None);

        await Assert.ThrowsAsync<SnapshotConflictException>(() => store.SaveAsync(
            new SessionState { SessionId = "s1", Snapshot = Map(("A", 9)) }, CancellationToken.None));
    }
}
