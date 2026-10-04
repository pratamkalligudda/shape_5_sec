using Microsoft.Extensions.Configuration;
using Shape.LiveBackend.Options;

namespace Shape.LiveBackend.Tests;

public class LivePollingOptionsTests
{
    private static LivePollingOptions Bind(Dictionary<string, string?> values) =>
        new ConfigurationBuilder().AddInMemoryCollection(values).Build().GetSection("LivePolling").Get<LivePollingOptions>()!;

    [Theory]
    [InlineData("0", "5", 1, 10)]
    [InlineData("99", "45", 60, 30)]
    [InlineData("1", "10", 1, 10)]
    [InlineData("30", "20", 30, 20)]
    public void Interval_and_duration_are_clamped(string interval, string duration, int expectedInterval, int expectedDuration)
    {
        var o = Bind(new() { ["LivePolling:PollIntervalSeconds"] = interval, ["LivePolling:SessionDurationMinutes"] = duration });
        Assert.Equal(expectedInterval, o.PollIntervalSeconds);
        Assert.Equal(expectedDuration, o.SessionDurationMinutes);
    }

    [Fact]
    public void Default_series_metrics_bind_as_an_array_without_duplicating_defaults()
    {
        var o = Bind(new() { ["LivePolling:DefaultSeriesMetrics:0"] = "A", ["LivePolling:DefaultSeriesMetrics:1"] = "B" });
        Assert.Equal(new[] { "A", "B" }, o.DefaultSeriesMetrics);
        Assert.Equal(10, o.ExtensionMinutes);
        Assert.Equal(5, o.MaxConcurrentSessions);
    }
}
