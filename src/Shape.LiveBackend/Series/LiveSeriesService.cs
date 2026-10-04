using Microsoft.Extensions.Options;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Options;
using Shape.LiveBackend.Sessions;

namespace Shape.LiveBackend.Series;

/// <summary>GET /live-session/series (spec §7.3). One range read per metric, plus status and events, in parallel.</summary>
public sealed class LiveSeriesService(
    IDeviceDirectory devices, ILivePollingSessionRepository sessions, ILiveSessionReader live, MetricRegistry registry,
    IOptions<LivePollingOptions> options, TimeProvider time, ILogger<LiveSeriesService> logger)
{
    public async Task<ServiceResult> GetSeriesAsync(string serial, IReadOnlyList<string?>? metrics, DateTimeOffset? since, CancellationToken ct)
    {
        if (LiveSessionService.LookupDevice(devices, serial, out var device) is { } notFound) return notFound;

        var selection = registry.Select(metrics, options.Value.DefaultSeriesMetrics);
        if (selection.Errors.Count > 0)
            return ServiceResult.Fail(400, ErrorCodes.InvalidParameters, selection.Message!, selection.Errors);

        var open = await sessions.GetOpenForDeviceAsync(device.DeviceGuid, time.GetUtcNow(), ct);
        if (open is null) return LiveSessionService.NotActive();

        var id = open.SessionId;
        var interval = open.PollIntervalSeconds;
        try
        {
            var head = live.ReadHeadAsync(id, ct);
            var reads = selection.Metrics.Select(m => live.ReadParameterAsync(id, m, ct)).ToList();
            var status = selection.Metrics.Contains(MetricRegistry.DeviceStatus)
                ? reads[selection.Metrics.ToList().IndexOf(MetricRegistry.DeviceStatus)]
                : live.ReadParameterAsync(id, MetricRegistry.DeviceStatus, ct);
            var events = live.ReadEventsAsync(id, ct);
            await Task.WhenAll(reads.Cast<Task>().Append(head).Append(status).Append(events));

            var h = await head;
            var grid = SeriesStepExpander.Grid(h?.FirstTickUtc, h?.LastTickUtc, interval, since);
            var series = new List<SeriesMetricDto>();
            for (var i = 0; i < selection.Metrics.Count; i++)
                series.Add(new SeriesMetricDto(selection.Metrics[i], SeriesStepExpander.Expand(grid, await reads[i], interval)));
            var kept = (await events)
                .Where(e => since is null || e.State == "active" || e.TimestampUtc >= since)
                .ToList();

            return ServiceResult.Ok(new SeriesDto(id, interval, open.StartedUtc, h?.LastTickUtc, h?.TickCount ?? 0, grid,
                SeriesStepExpander.ExpandStatus(grid, await status, interval), series, kept));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger.LogError(ex, "Reading the series of session {SessionId} failed", id);
            return new ServiceResult(500, new ApiResponse { Succeeded = false, Message = "Live session data could not be read." });
        }
    }
}
