using Microsoft.Azure.Functions.Worker;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Functions;

public sealed class LivePollingCleanupTimerFunction(LivePollingCleanup cleanup)
{
    [Function("LivePollingCleanupTimer")]
    public Task Run([TimerTrigger("%LivePolling:CleanupSchedule%")] TimerInfo timer, CancellationToken ct) => cleanup.RunAsync(ct);
}
