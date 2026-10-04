using Microsoft.Azure.Functions.Worker;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Functions;

public sealed class LiveTelemetryProcessorFunction(LiveTelemetryProcessor processor)
{
    [Function("LiveTelemetryProcessorEventGrid")]
    [SignalROutput(HubName = SignalRNames.Hub, ConnectionStringSetting = SignalRNames.ConnectionSetting)]
    public async Task<SignalRMessageAction?> Run([EventGridTrigger] string eventJson, CancellationToken ct)
    {
        var broadcast = await processor.ProcessAsync(eventJson, ct);
        return broadcast is null
            ? null
            : new SignalRMessageAction(SignalRNames.Target, [broadcast.Envelope]) { GroupName = broadcast.GroupName };
    }
}
