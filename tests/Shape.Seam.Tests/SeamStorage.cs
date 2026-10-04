using Azure.Data.Tables;
using Microsoft.Data.Sqlite;

namespace Shape.Seam.Tests;

/// <summary>Azurite (10100–10102) plus a temp SQLite file. Each test gets its own table and file.</summary>
public sealed class SeamStorage : IDisposable
{
    public const string ConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;";

    public TableServiceClient Service { get; } = new(ConnectionString);
    public string Table { get; } = "seam" + Guid.NewGuid().ToString("N")[..20];
    public string DbPath { get; } = Path.Combine(Path.GetTempPath(), "shape-live-tests", $"{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        try { Service.DeleteTable(Table); } catch (Azure.RequestFailedException) { }
        SqliteConnection.ClearAllPools();
        foreach (var f in new[] { DbPath, DbPath + "-wal", DbPath + "-shm" })
            if (File.Exists(f)) File.Delete(f);
    }
}
