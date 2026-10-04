using Azure.Data.Tables;

namespace Shape.LiveIngestion.Tests;

/// <summary>Azurite on 10100–10102 (Global Constraints). Each test uses its own table.</summary>
public static class DevStorage
{
    public const string ConnectionString =
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10100/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10101/devstoreaccount1;TableEndpoint=http://127.0.0.1:10102/devstoreaccount1;";

    public static TableServiceClient Service() => new(ConnectionString);

    public static string UniqueTable() => "t" + Guid.NewGuid().ToString("N")[..20];
}
