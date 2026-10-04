namespace Shape.SmokeClient;

/// <summary>
/// Local endpoints. DeviceGuid is configured, not discovered: no live-session response returns it (finding F1),
/// so the frontend team will hit the same wall.
/// </summary>
public sealed record SmokeConfig(
    string BackendUrl, string NegotiatorUrl, string SimulatorUrl, string Serial, string DeviceGuid,
    string TableConnectionString, string DbPath, int CleanupWaitSeconds)
{
    public static SmokeConfig Default() => new(
        BackendUrl: "http://localhost:5080",
        NegotiatorUrl: "http://localhost:7071",
        SimulatorUrl: "http://localhost:5100",
        Serial: "PUMP-00417",
        DeviceGuid: "9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284",
        TableConnectionString: "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;",
        DbPath: Path.Combine(FindRepoRoot(), "data", "live-polling.db"),
        CleanupWaitSeconds: 45);   // two runs of the 15-second local cleanup schedule, plus slack

    private static string FindRepoRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
            if (File.Exists(Path.Combine(dir.FullName, "ShapeLive.sln"))) return dir.FullName;
        throw new InvalidOperationException("ShapeLive.sln not found.");
    }
}

public sealed class SmokeFailure(string message) : Exception(message);
