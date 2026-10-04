using Microsoft.Data.Sqlite;

namespace Shape.LiveBackend.Tests;

/// <summary>A throwaway SQLite file. Pools are cleared before deletion so Windows releases the handle.</summary>
public sealed class TempDb : IDisposable
{
    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "shape-live-tests", $"{Guid.NewGuid():N}.db");

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var f in new[] { Path, Path + "-wal", Path + "-shm" })
            if (File.Exists(f)) File.Delete(f);
    }
}
