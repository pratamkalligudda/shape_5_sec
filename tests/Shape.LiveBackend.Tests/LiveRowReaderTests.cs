using System.Text.Json;
using Azure.Data.Tables;
using Shape.LiveBackend.LiveData;

namespace Shape.LiveBackend.Tests;

public class LiveRowReaderTests
{
    private static readonly DateTimeOffset T = DateTimeOffset.Parse("2026-10-01T10:00:00Z");

    [Theory]
    [InlineData("b", true, "true")]
    [InlineData("i", 54372L, "54372")]
    [InlineData("d", 12.5, "12.5")]
    [InlineData("s", "OK", "\"OK\"")]
    public void Change_point_value_decodes_by_type_code(string code, object value, string json)
    {
        var e = new TableEntity("pk", "P|x") { ["T"] = code, ["V"] = value, ["TimestampUtc"] = T };
        Assert.Equal(json, LiveRowReader.Value(e).GetRawText());
    }

    [Fact]
    public void Type_n_decodes_to_json_null() =>
        Assert.Equal(JsonValueKind.Null, LiveRowReader.Value(new TableEntity("pk", "P|x") { ["T"] = "n" }).ValueKind);

    [Fact]
    public void Event_row_without_label_decodes_to_null_label()
    {
        var e = new TableEntity("pk", "evt|x|MaintenanceAlarmActive")
        {
            ["Parameter"] = "MaintenanceAlarmActive", ["Value"] = 1L, ["State"] = "triggered", ["EventType"] = "alarm",
            ["Domain"] = "maintenance", ["Severity"] = "critical", ["TimestampUtc"] = T,
        };

        Assert.Equal(new Api.LiveEventDto(T, "MaintenanceAlarmActive", 1, "triggered", "alarm", "maintenance", "critical", null),
            LiveRowReader.Event(e));
    }

    [Fact]
    public void Head_decodes_snapshot_and_counters()
    {
        var e = new TableEntity("pk", "!current")
        {
            ["Snapshot"] = """{"P":1,"OK":true}""", ["DeviceStatus"] = 1, ["TickCount"] = 5L, ["BytesTotal"] = 700L,
            ["HasBaseline"] = true, ["FirstTickUtc"] = T, ["LastTickUtc"] = T.AddSeconds(4), ["LastSnapshotTimestampUtc"] = T.AddSeconds(4),
        };

        var h = LiveRowReader.Head(e);

        Assert.Equal(1, h.Snapshot["P"].GetInt32());
        Assert.Equal(1, h.DeviceStatus);
        Assert.Equal(5, h.TickCount);
        Assert.Equal(T.AddSeconds(4), h.LastTickUtc);
    }
}
