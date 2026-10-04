using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class LiveEventResolverTests
{
    private static readonly IReadOnlyDictionary<string, AlertDefinition> Catalog = TestCatalog.Load();

    private static Dictionary<string, JsonElement> Block(string json) =>
        JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());

    private static IReadOnlyList<LiveEvent> Resolve(string json, bool baseline) =>
        LiveEventResolver.Resolve(Block(json), baseline, Catalog, NullLogger.Instance);

    [Fact]
    public void Delta_non_zero_is_triggered_with_metadata_and_label()
    {
        var e = Assert.Single(Resolve("""{"OperatingAlarmActive":5}""", baseline: false));
        Assert.Equal(new LiveEvent("OperatingAlarmActive", 5, "triggered", "alarm", "operational", "critical", "Emergency Stop Active"), e);
    }

    [Fact]
    public void Delta_zero_is_cleared_with_null_label()
    {
        var e = Assert.Single(Resolve("""{"OperatingAlarmActive":0}""", baseline: false));
        Assert.Equal("cleared", e.State);
        Assert.Null(e.Label);
    }

    [Fact]
    public void Baseline_non_zero_is_active_and_zeros_are_suppressed()
    {
        var events = Resolve("""{"OperatingAlarmActive":6,"OilFilterAlarm":0,"MotorRunning":1}""", baseline: true);
        Assert.Equal(new[] { "OperatingAlarmActive", "MotorRunning" }, events.Select(e => e.Parameter));
        Assert.All(events, e => Assert.Equal("active", e.State));
        Assert.Equal("Overstroke Alarm", events[0].Label);
    }

    [Fact]
    public void Boolean_parameters_carry_metadata_without_label()
    {
        var e = Assert.Single(Resolve("""{"MaintenanceAlarmActive":1}""", baseline: false));
        Assert.Equal(("alarm", "maintenance", "critical"), (e.EventType, e.Domain, e.Severity));
        Assert.Null(e.Label);
    }

    [Fact]
    public void Value_code_without_a_label_has_null_label()
    {
        Assert.Null(Assert.Single(Resolve("""{"OperatingAlarmActive":99}""", baseline: false)).Label);
    }

    [Theory]
    [InlineData("\"5\"")]
    [InlineData("5.0")]
    public void String_and_double_values_are_coerced(string raw)
    {
        var e = Assert.Single(Resolve($$"""{"OperatingAlarmActive":{{raw}}}""", baseline: false));
        Assert.Equal(5, e.Value);
        Assert.Equal("Emergency Stop Active", e.Label);
    }

    [Fact]
    public void True_and_false_coerce_to_one_and_zero()
    {
        var events = Resolve("""{"OilFilterAlarm":true,"OilChangeAlarm":false}""", baseline: false);
        Assert.Equal(new long[] { 1, 0 }, events.Select(e => e.Value));
        Assert.Equal(new[] { "triggered", "cleared" }, events.Select(e => e.State));
    }

    [Theory]
    [InlineData("\"abc\"")]
    [InlineData("5.5")]
    [InlineData("null")]
    [InlineData("{}")]
    public void Uncoercible_values_are_skipped(string raw)
    {
        Assert.Empty(Resolve($$"""{"OperatingAlarmActive":{{raw}}}""", baseline: false));
    }

    [Fact]
    public void Unknown_parameter_is_emitted_with_unknown_metadata()
    {
        var e = Assert.Single(Resolve("""{"BrandNewAlarm":1}""", baseline: false));
        Assert.Equal(("unknown", "unknown", "unknown"), (e.EventType, e.Domain, e.Severity));
        Assert.Equal("triggered", e.State);
    }

    [Fact]
    public void Empty_or_missing_block_yields_nothing()
    {
        Assert.Empty(LiveEventResolver.Resolve(null, false, Catalog, NullLogger.Instance));
        Assert.Empty(Resolve("{}", baseline: true));
    }

    [Fact]
    public void Empty_catalog_resolves_everything_as_unknown()
    {
        var e = Assert.Single(LiveEventResolver.Resolve(Block("""{"OperatingAlarmActive":5}"""), false,
            new Dictionary<string, AlertDefinition>(), NullLogger.Instance));
        Assert.Equal("unknown", e.EventType);
        Assert.Null(e.Label);
    }
}
