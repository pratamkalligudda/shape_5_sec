using System.Text;

namespace Shape.Gateway.Simulator;

/// <summary>Delivers a batch as Event Grid would: POST to the function's local EventGrid webhook.</summary>
public sealed class EventGridPublisher(
    HttpClient http, Uri endpoint, DeviceConfig device, IReadOnlyList<TimeSpan> retryDelays, ILogger<EventGridPublisher> logger)
    : IBatchPublisher
{
    public static readonly Uri DefaultEndpoint =
        new("http://localhost:7071/runtime/webhooks/EventGrid?functionName=LiveTelemetryProcessorEventGrid");

    public static readonly TimeSpan[] DefaultRetryDelays = [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4)];

    public async Task<bool> PublishAsync(OutgoingBatch batch, CancellationToken ct)
    {
        var gz = GzipBatchCodec.Encode(batch.Entries);
        var json = EventGridEnvelope.Build(device, batch, gz);
        for (var attempt = 0; ; attempt++)
        {
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Post, endpoint)
                {
                    Content = new StringContent(json, Encoding.UTF8, "application/json"),
                };
                req.Headers.Add("aeg-event-type", "Notification");
                using var res = await http.SendAsync(req, ct);
                if (res.IsSuccessStatusCode)
                {
                    logger.LogInformation("Delivered batch {Seq} ({Entries} entries, {Bytes} gzip bytes)", batch.Seq, batch.Entries.Count, gz.Length);
                    return true;
                }
                logger.LogWarning("Batch {Seq} attempt {Attempt} got HTTP {Status}", batch.Seq, attempt + 1, (int)res.StatusCode);
            }
            catch (HttpRequestException ex)
            {
                logger.LogWarning("Batch {Seq} attempt {Attempt} failed: {Error}", batch.Seq, attempt + 1, ex.Message);
            }
            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning("Batch {Seq} attempt {Attempt} timed out", batch.Seq, attempt + 1);
            }

            if (attempt >= retryDelays.Count)
            {
                logger.LogError("Dropping batch {Seq} after {Attempts} attempts", batch.Seq, attempt + 1);
                return false;
            }
            await Task.Delay(retryDelays[attempt], ct);
        }
    }
}
