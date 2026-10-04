namespace Shape.LiveIngestion.Infrastructure;

/// <summary>Repo-relative paths (data/live-polling.db) resolve against the directory holding ShapeLive.sln.</summary>
public static class RepoPaths
{
    public static string Root { get; } = Find();

    public static string Resolve(string path) =>
        Path.IsPathRooted(path) ? path : Path.GetFullPath(Path.Combine(Root, path.Replace('/', Path.DirectorySeparatorChar)));

    private static string Find()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "ShapeLive.sln"))) return dir.FullName;
        throw new InvalidOperationException($"ShapeLive.sln not found above {AppContext.BaseDirectory}.");
    }
}
