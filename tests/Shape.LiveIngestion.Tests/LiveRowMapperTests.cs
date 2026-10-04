using System.Text.Json;
using Shape.LiveIngestion.LivePolling;
using static Shape.LiveIngestion.Tests.FoldBuilders;

namespace Shape.LiveIngestion.Tests;

public class LiveRowMapperTests
{
    private const string Pk = "cc27db6b-508b-488e-a816-faae684491d8";

    [Theory]
    [InlineData("true", "b", typeof(bool))]
    [InlineData("54372", "i", typeof(long))]
    [InlineData("12.5", "d", typeof(double))]
    [InlineData("\"OK\"", "s", typeof(string))]
    public void Change_point_stores_native_value_and_type_code(string json, string code, Type clr)
    {
        var e = LiveRowMapper.ChangePoint(Pk, new ChangePointWrite("P", T0, JsonDocument.Parse(json).RootElement.Clone()));

        Assert.Equal("P|2026-10-01T10:00:00.0000000Z", e.RowKey);
        Assert.Equal(code, e.GetString("T"));
        Assert.IsType(clr, e["V"]);
        Assert.Equal(T0, e.GetDateTimeOffset("TimestampUtc"));
    }

    [Fact]
    public void Json_null_is_stored_as_type_n_without_value()
    {
        var e = LiveRowMapper.ChangePoint(Pk, new ChangePointWrite("P", T0, JsonDocument.Parse("null").RootElement.Clone()));
        Assert.Equal("n", e.GetString("T"));
        Assert.False(e.ContainsKey("V"));
    }

    [Fact]
    public void Event_row_omits_label_when_null_and_keeps_it_when_present()
    {
        var unlabelled = LiveRowMapper.Event(Pk, new LiveEventWrite(T0,
            new LiveEvent("MaintenanceAlarmActive", 1, "triggered", "alarm", "maintenance", "critical", null)));
        var labelled = LiveRowMapper.Event(Pk, new LiveEventWrite(T0,
            new LiveEvent("OperatingAlarmActive", 5, "active", "alarm", "operational", "critical", "Emergency Stop Active")));

        Assert.Equal("evt|2026-10-01T10:00:00.0000000Z|MaintenanceAlarmActive", unlabelled.RowKey);
        Assert.False(unlabelled.ContainsKey("Label"));
        Assert.Equal(1L, unlabelled["Value"]);
        Assert.Equal("Emergency Stop Active", labelled.GetString("Label"));
        Assert.Equal("active", labelled.GetString("State"));
    }

    [Fact]
    public void Head_round_trips_through_the_entity()
    {
        var head = new LiveHead
        {
            DeviceGuid = Device.DeviceGuid, DeviceId = "PUMP-00417", Snapshot = M("""{"P":1,"OK":true,"S":"x"}"""),
            DeviceStatus = 2, LastSnapshotTimestampUtc = T0.AddSeconds(4), HasBaseline = true, TickCount = 5,
            BytesTotal = 700, FirstTickUtc = T0, LastTickUtc = T0.AddSeconds(4),
        };

        var back = LiveRowMapper.ReadHead(LiveRowMapper.Head(Pk, head));

        Assert.Equal(head with { Snapshot = back.Snapshot, ETag = back.ETag }, back);
        Assert.Equal("""{"P":1,"OK":true,"S":"x"}""", JsonSerializer.Serialize(back.Snapshot));
    }

    [Fact]
    public void Head_before_first_tick_has_no_status_or_tick_columns()
    {
        var e = LiveRowMapper.Head(Pk, new LiveHead { DeviceGuid = Device.DeviceGuid, DeviceId = "PUMP-00417" });

        Assert.Equal("!current", e.RowKey);
        Assert.False(e.ContainsKey("DeviceStatus"));
        Assert.False(e.ContainsKey("FirstTickUtc"));
        Assert.Null(LiveRowMapper.ReadHead(e).DeviceStatus);
    }
}
