using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Series;
using static Shape.LiveBackend.Tests.ServiceFixture;

namespace Shape.LiveBackend.Tests;

public sealed class LiveSeriesServiceTests : IDisposable
{
    private readonly ServiceFixture _f = new();
    private readonly LiveSeriesService _series;

    public LiveSeriesServiceTests() =>
        _series = new LiveSeriesService(new ConfigDeviceDirectory(Device), _f.Sessions, _f.Reader,
            new MetricRegistry(["OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight", "OperatingAlarmActive"]),
            Microsoft.Extensions.Options.Options.Create(_f.Options), _f.Time, NullLogger<LiveSeriesService>.Instance);

    public void Dispose() => _f.Dispose();

    private static ChangePoint P(DateTimeOffset at, object value) => new(at, JsonSerializer.SerializeToElement(value));

    private static LiveEventDto E(DateTimeOffset at, string parameter, long value, string state, string? label = null) =>
        new(at, parameter, value, state, "alarm", "operational", "critical", label);

    /// <summary>Session started at T0, ticks at T0+1 … T0+5.</summary>
    private async Task<Guid> SessionWithTicksAsync()
    {
        var id = await _f.StartedSessionAsync();
        var first = T0.AddSeconds(1);
        _f.Reader.Heads[id] = new LiveHeadView(new(), 2, first.AddSeconds(4), 5, 3500, first, first.AddSeconds(4));
        _f.Reader.Points[(id, "OperatingPressurePSI")] = [P(first, 54000), P(first.AddSeconds(2), 54100)];
        _f.Reader.Points[(id, "StrokeRateLeft")] = [P(first, 90)];
        _f.Reader.Points[(id, "DeviceStatus")] = [P(first, 2L), P(first.AddSeconds(3), 0L)];
        _f.Reader.Events[id] =
        [
            E(first, "OperatingAlarmActive", 5, "active", "Emergency Stop Active"),
            E(first.AddSeconds(1), "OperatingWarningActive", 21, "triggered", "Left Overstroke Warning"),
            E(first.AddSeconds(3), "OperatingAlarmActive", 0, "cleared"),
        ];
        _f.Time.Advance(TimeSpan.FromSeconds(6));
        return id;
    }

    [Fact]
    public async Task Default_call_returns_grid_status_default_series_and_events()
    {
        var id = await SessionWithTicksAsync();

        var r = await _series.GetSeriesAsync("PUMP-00417", null, null, default);

        var dto = Assert.IsType<SeriesDto>(r.Body.Data);
        Assert.Equal((id, 1, T0, 5L), (dto.SessionId, dto.PollIntervalSeconds, dto.StartedUtc, dto.TickCount));
        Assert.Equal(T0.AddSeconds(5), dto.LastTickUtc);
        Assert.Equal(5, dto.T.Count);
        Assert.Equal(new[] { 2, 2, 2, 0, 0 }, dto.Status);
        Assert.Equal(new[] { "OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight" },
            dto.Series.Select(s => s.Metric));
        Assert.Equal(new[] { "54000", "54000", "54100", "54100", "54100" }, dto.Series[0].V.Select(v => v!.Value.GetRawText()));
        Assert.All(dto.Series[1].V, v => Assert.Null(v));         // valid but never reported: nulls, not an error
        Assert.Equal(3, dto.Events.Count);
    }

    [Fact]
    public async Task Metric_filter_keeps_status_and_events_and_request_order()
    {
        await SessionWithTicksAsync();

        var dto = Assert.IsType<SeriesDto>((await _series.GetSeriesAsync("PUMP-00417", ["strokerateleft", "OperatingPressurePSI"], null, default)).Body.Data);

        Assert.Equal(new[] { "StrokeRateLeft", "OperatingPressurePSI" }, dto.Series.Select(s => s.Metric));
        Assert.Equal(5, dto.Status.Count);
        Assert.Equal(3, dto.Events.Count);
    }

    [Fact]
    public async Task Each_metric_status_and_events_are_read_once()
    {
        await SessionWithTicksAsync();

        await _series.GetSeriesAsync("PUMP-00417", ["StrokeRateLeft"], null, default);

        Assert.Equal(new[] { "DeviceStatus", "StrokeRateLeft" }, _f.Reader.ParameterReads.Order());
    }

    [Fact]
    public async Task Since_trims_the_grid_and_transitions_but_keeps_active_events()
    {
        await SessionWithTicksAsync();
        var since = T0.AddSeconds(3);

        var dto = Assert.IsType<SeriesDto>((await _series.GetSeriesAsync("PUMP-00417", ["OperatingPressurePSI"], since, default)).Body.Data);

        Assert.Equal(new[] { 3, 4, 5 }, dto.T.Select(t => (int)(t - T0).TotalSeconds));
        Assert.Equal(new[] { 2, 0, 0 }, dto.Status);
        Assert.Equal(new[] { "active", "cleared" }, dto.Events.Select(e => e.State));
    }

    [Fact]
    public async Task Session_without_ticks_returns_empty_arrays()
    {
        await _f.StartedSessionAsync();

        var dto = Assert.IsType<SeriesDto>((await _series.GetSeriesAsync("PUMP-00417", null, null, default)).Body.Data);

        Assert.Empty(dto.T);
        Assert.Empty(dto.Status);
        Assert.All(dto.Series, s => Assert.Empty(s.V));
        Assert.Null(dto.LastTickUtc);
        Assert.Equal(0, dto.TickCount);
    }

    [Fact]
    public async Task No_open_session_is_not_active()
    {
        var r = await _series.GetSeriesAsync("PUMP-00417", null, null, default);
        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, r.Body.Errors![0]));
    }

    [Fact]
    public async Task Invalid_metrics_are_rejected_with_details()
    {
        await SessionWithTicksAsync();

        var r = await _series.GetSeriesAsync("PUMP-00417", ["Nope"], null, default);

        Assert.Equal(400, r.StatusCode);
        Assert.Equal(ErrorCodes.InvalidParameters, r.Body.Errors![0]);
        Assert.Equal("'Nope' is not a known metric.", r.Body.Errors[1]);
        Assert.StartsWith("Valid metrics: ", r.Body.Errors[^1]);
    }
}
