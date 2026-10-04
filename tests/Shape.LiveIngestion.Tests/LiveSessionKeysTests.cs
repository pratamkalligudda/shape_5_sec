using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class LiveSessionKeysTests
{
    private static readonly DateTimeOffset T = DateTimeOffset.Parse("2026-09-28T10:27:02.218Z");

    private static bool InRange(string key, (string From, string To) r) =>
        string.CompareOrdinal(key, r.From) >= 0 && string.CompareOrdinal(key, r.To) < 0;

    [Fact]
    public void Row_keys_use_O_format_in_utc()
    {
        Assert.Equal("OperatingPressurePSI|2026-09-28T10:27:02.2180000Z", LiveSessionKeys.ChangeRowKey("OperatingPressurePSI", T));
        Assert.Equal("evt|2026-09-28T10:27:02.2180000Z|OperatingAlarmActive", LiveSessionKeys.EventRowKey("OperatingAlarmActive", T));
        Assert.Equal("2026-09-28T10:27:02.2180000Z", LiveSessionKeys.Timestamp(T.ToOffset(TimeSpan.FromHours(5.5))));
    }

    [Fact]
    public void Param_range_excludes_longer_names_with_the_same_prefix()
    {
        var range = LiveSessionKeys.ParamRange("StrokeRate");
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("StrokeRate", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("StrokeRateLeft", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("StrokeRateRight", T), range));
    }

    [Fact]
    public void Event_range_excludes_parameter_rows_and_the_head_row()
    {
        var range = LiveSessionKeys.EventRange();
        Assert.True(InRange(LiveSessionKeys.EventRowKey("OilFilterAlarm", T), range));
        Assert.False(InRange(LiveSessionKeys.HeadRowKey, range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("OperatingPressurePSI", T), range));
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("evtX", T), range));
    }

    [Fact]
    public void Since_lower_bounds_are_inclusive_and_chronological()
    {
        var range = LiveSessionKeys.ParamRange("A", T);
        Assert.False(InRange(LiveSessionKeys.ChangeRowKey("A", T.AddTicks(-1)), range));
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("A", T), range));
        Assert.True(InRange(LiveSessionKeys.ChangeRowKey("A", T.AddDays(1)), range));
    }

    [Fact]
    public void Two_events_on_one_tick_do_not_collide()
    {
        Assert.NotEqual(LiveSessionKeys.EventRowKey("OperatingAlarmActive", T), LiveSessionKeys.EventRowKey("MaintenanceAlarmActive", T));
    }

    [Theory]
    [InlineData("evt")]
    [InlineData("a|b")]
    [InlineData("")]
    public void Reserved_or_malformed_parameter_names_are_rejected(string name)
    {
        Assert.Throws<ArgumentException>(() => LiveSessionKeys.ChangeRowKey(name, T));
        Assert.Throws<ArgumentException>(() => LiveSessionKeys.ParamRange(name));
    }
}
