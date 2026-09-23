using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Shape.Ingest;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        var connection = context.Configuration["SnapshotStorage"] ?? "UseDevelopmentStorage=true";
        services.AddSingleton<ISnapshotStore>(_ =>
            new TableSnapshotStore(new TableServiceClient(connection).GetTableClient(TableSnapshotStore.DefaultTableName)));
        services.AddSingleton<TelemetryIngestor>();
    })
    .Build();

host.Run();
