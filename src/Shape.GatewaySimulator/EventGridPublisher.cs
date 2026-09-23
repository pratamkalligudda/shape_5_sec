using System.Text;
using System.Text.Json;
using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>Delivers a batch the way Event Grid would: POST an EventGridEvent array to the Function's webhook.</summary>
public sealed class EventGridPublisher(
    HttpClient http, Uri endpoint, IReadOnlyList<TimeSpan> retryDelays, ILogger<EventGridPublisher> logger) : IBatchPublisher
{
    public const string EventType = "Shape.Telemetry.FastPollingBatch";
    public const string Subject = "pumps/pump-001";
    public static readonly TimeSpan[] DefaultRetryDelays =
        [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4)];

    public async Task<bool> PublishAsync(TelemetryBatch batch, CancellationToken ct)
    {
        var payload = JsonSerializer.Serialize(new[]
        {
            new
            {
                id = Guid.NewGuid().ToString(),
                eventType = EventType,
                subject = Subject,
                eventTime = DateTimeOffset.UtcNow,
                dataVersion = "1.0",
                data = batch,
            },
        }, ShapeJson.Options);

        var seq = batch.Properties?.BatchSeq;
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Post, endpoint)
                {
                    Content = new StringContent(payload, Encoding.UTF8, "application/json"),
                };
                request.Headers.Add("aeg-event-type", "Notification");
                using var response = await http.SendAsync(request, ct);
                if (response.IsSuccessStatusCode)
                {
                    logger.LogInformation("Delivered batch {BatchSeq} ({Count} samples)", seq, batch.Samples?.Count);
                    return true;
                }
                logger.LogWarning("Batch {BatchSeq} attempt {Attempt} got HTTP {Status}", seq, attempt + 1, (int)response.StatusCode);
            }
            catch (HttpRequestException ex)
            {
                logger.LogWarning("Batch {BatchSeq} attempt {Attempt} failed: {Error}", seq, attempt + 1, ex.Message);
            }

            if (attempt >= retryDelays.Count)
            {
                logger.LogError("Dropping batch {BatchSeq} after {Attempts} attempts", seq, attempt + 1);
                return false;
            }
            await Task.Delay(retryDelays[attempt], ct);
        }
    }
}
