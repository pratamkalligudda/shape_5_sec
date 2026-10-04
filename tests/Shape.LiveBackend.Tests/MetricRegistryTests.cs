using Shape.LiveBackend.Series;

namespace Shape.LiveBackend.Tests;

public class MetricRegistryTests
{
    private static readonly MetricRegistry Registry =
        new(["OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight", "MotorRunning"]);
    private static readonly string[] Defaults = ["OperatingPressurePSI", "StrokeRateLeft"];

    [Fact]
    public void No_metrics_means_the_configured_defaults()
    {
        Assert.Equal(Defaults, Registry.Select(null, Defaults).Metrics);
        Assert.Equal(Defaults, Registry.Select([], Defaults).Metrics);
    }

    [Fact]
    public void Names_match_case_insensitively_and_come_back_in_registry_casing_in_request_order()
    {
        var s = Registry.Select(["strokerateright", "OPERATINGPRESSUREPSI", "devicestatus"], Defaults);
        Assert.Empty(s.Errors);
        Assert.Equal(new[] { "StrokeRateRight", "OperatingPressurePSI", "DeviceStatus" }, s.Metrics);
    }

    [Fact]
    public void Duplicates_collapse_to_the_first_occurrence()
    {
        Assert.Equal(new[] { "StrokeRateLeft", "MotorRunning" },
            Registry.Select(["StrokeRateLeft", "motorrunning", "strokerateleft"], Defaults).Metrics);
    }

    [Fact]
    public void Unknown_and_blank_names_are_each_listed_then_the_sorted_valid_set()
    {
        var s = Registry.Select(["OperatingPresurePSI", "", "OperatingPressurePSI,StrokeRateLeft"], Defaults);

        Assert.Empty(s.Metrics);
        Assert.Equal("Unknown metric requested.", s.Message);
        Assert.Equal(new[]
        {
            "'OperatingPresurePSI' is not a known metric.",
            "Metric names must not be blank.",
            "'OperatingPressurePSI,StrokeRateLeft' is not a known metric.",
            "Valid metrics: DeviceStatus, HydraulicOilTemperatureF, MotorRunning, OperatingPressurePSI, StrokeRateLeft, StrokeRateRight",
        }, s.Errors);
    }

    [Fact]
    public void More_than_ten_distinct_metrics_are_rejected()
    {
        var registry = new MetricRegistry(Enumerable.Range(1, 12).Select(i => $"M{i}"));
        var s = registry.Select(Enumerable.Range(1, 11).Select(i => (string?)$"M{i}").ToList(), Defaults);

        Assert.Equal("Too many metrics requested.", s.Message);
        Assert.Equal("At most 10 metrics may be requested; 11 were.", s.Errors[0]);
        Assert.StartsWith("Valid metrics: ", s.Errors[^1]);
    }

    [Fact]
    public void Ten_metrics_with_duplicates_beyond_ten_are_accepted()
    {
        var registry = new MetricRegistry(Enumerable.Range(1, 12).Select(i => $"M{i}"));
        var request = Enumerable.Range(1, 10).Select(i => (string?)$"M{i}").Append("m1").ToList();
        Assert.Equal(10, registry.Select(request, Defaults).Metrics.Count);
    }

    [Fact]
    public void Real_registry_is_the_seed_field_names_plus_device_status()
    {
        var registry = MetricRegistry.Load(MetricRegistry.DefaultSeedPath);
        Assert.Contains("DeviceStatus", registry.Names);
        Assert.Contains("OperatingPressurePSI", registry.Names);
        Assert.Contains("HydraulicOilTemperatureF", registry.Names);
        Assert.True(registry.Names.Count >= 74, $"{registry.Names.Count} names");
    }
}
