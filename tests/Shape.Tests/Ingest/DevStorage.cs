namespace Shape.Tests.Ingest;

/// <summary>
/// Azurite runs on 10100-10102 on this machine (10000/10002 are taken by other services), so the
/// "UseDevelopmentStorage=true" shortcut cannot be used. Override with SHAPE_STORAGE if needed.
/// </summary>
public static class DevStorage
{
    public const string Default =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;" +
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
        "BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;" +
        "QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;" +
        "TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;";

    public static string ConnectionString => Environment.GetEnvironmentVariable("SHAPE_STORAGE") ?? Default;
}
