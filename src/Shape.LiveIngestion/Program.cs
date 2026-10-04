using Azure.Core;
using Azure.Data.Tables;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Shape.LiveIngestion.Infrastructure;
using Shape.LiveIngestion.LivePolling;

var host = new HostBuilder()
    .ConfigureFunctionsWorkerDefaults()
    .ConfigureServices((context, services) =>
    {
        var config = context.Configuration;
        services.AddSingleton(new DeviceResolver(DeviceConfig.Load(DeviceConfig.DefaultPath)));
        services.AddSingleton<IAlertDefinitionCatalog>(sp =>
            new AlertDefinitionCatalog(AlertDefinitionCatalog.DefaultPath, sp.GetRequiredService<ILogger<AlertDefinitionCatalog>>()));
        services.AddSingleton(_ =>
        {
            var options = new TableClientOptions();
            options.Retry.Mode = RetryMode.Exponential;   // 503/500 back-off (spec §9)
            options.Retry.MaxRetries = 5;
            options.Retry.Delay = TimeSpan.FromMilliseconds(200);
            return new TableServiceClient(config["LiveTableStorage"]
                ?? throw new InvalidOperationException("LiveTableStorage is not configured."), options);
        });
        services.AddSingleton<ILiveSessionStore>(sp => new TableLiveSessionStore(sp.GetRequiredService<TableServiceClient>()));
        services.AddSingleton<LiveTelemetryProcessor>();
        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<ICleanupSessionSource>(_ =>
            new SqliteCleanupSessionSource(RepoPaths.Resolve(config["LivePolling:SessionDbPath"] ?? "data/live-polling.db")));
        services.AddSingleton<LivePollingCleanup>();
    })
    .Build();

host.Run();
