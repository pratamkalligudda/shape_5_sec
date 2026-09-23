using Azure.Messaging.EventGrid;
using Microsoft.Azure.Functions.Worker;
using Shape.Ingest;

namespace Shape.Functions;

public sealed class IngestTelemetryFunction(TelemetryIngestor ingestor)
{
    public const string HubName = "telemetry";
    public const string Target = "batch";

    [Function("IngestTelemetry")]
    [SignalROutput(HubName = HubName, ConnectionStringSetting = "AzureSignalRConnectionString")]
    public async Task<SignalRMessageAction?> Run([EventGridTrigger] EventGridEvent ev, CancellationToken ct)
    {
        var folded = await ingestor.IngestAsync(ev.Data.ToString(), ct);
        return folded is null ? null : new SignalRMessageAction(Target, new object[] { folded });
    }
}
