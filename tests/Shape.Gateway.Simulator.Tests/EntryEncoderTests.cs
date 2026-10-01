using System.Text.Json;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class EntryEncoderTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");

    private static Dictionary<string, JsonElement> R(params (string K, object V)[] kv) => kv.ToDictionary(x => x.K, x => J(x.V));
    private static Dictionary<string, int> E(params (string K, int V)[] kv) => kv.ToDictionary(x => x.K, x => x.V);

    [Fact]
    public void First_entry_is_Body_with_full_Event_including_zeros()
    {
        var entry = new EntryEncoder().Encode(R(("A", 1)), E(("OperatingAlarmActive", 0), ("MotorRunning", 1)), T0);

        Assert.Equal(T0.UtcDateTime, entry.TimestampUtc);
        Assert.NotNull(entry.Body);
        Assert.Null(entry.Changes);
        Assert.Equal(2, entry.Event!.Count);
        Assert.Equal(0, entry.Event["OperatingAlarmActive"]);
    }

    [Fact]
    public void Delta_carries_only_changed_fields_and_no_Event_when_events_unchanged()
    {
        var enc = new EntryEncoder();
        enc.Encode(R(("A", 1), ("B", 2)), E(("X", 0)), T0);

        var entry = enc.Encode(R(("A", 1), ("B", 3)), E(("X", 0)), T0.AddSeconds(1));

        Assert.Null(entry.Body);
        Assert.Equal(new[] { "B" }, entry.Changes!.Keys);
        Assert.Null(entry.Event);
    }

    [Fact]
    public void Heartbeat_is_empty_Changes()
    {
        var enc = new EntryEncoder();
        enc.Encode(R(("A", 1)), E(("X", 0)), T0);

        var entry = enc.Encode(R(("A", 1)), E(("X", 0)), T0.AddSeconds(1));

        Assert.Empty(entry.Changes!);
        Assert.Null(entry.Event);
    }

    [Fact]
    public void Event_carries_only_changed_parameters()
    {
        var enc = new EntryEncoder();
        enc.Encode(R(("A", 1)), E(("X", 0), ("Y", 0)), T0);

        var entry = enc.Encode(R(("A", 1)), E(("X", 5), ("Y", 0)), T0.AddSeconds(1));

        Assert.Equal(new Dictionary<string, int> { ["X"] = 5 }, entry.Event);
    }

    [Fact]
    public void Serialises_with_gateway_names_and_omits_nulls()
    {
        var entry = new EntryEncoder().Encode(R(("A", 1)), E(("X", 0)), T0);
        var json = JsonSerializer.Serialize(entry);

        Assert.Contains("\"TimestampUtc\":\"2026-10-01T10:00:00Z\"", json);
        Assert.Contains("\"Body\":{\"A\":1}", json);
        Assert.Contains("\"Event\":{\"X\":0}", json);
        Assert.DoesNotContain("Changes", json);
    }
}
