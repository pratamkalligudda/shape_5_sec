using Shape.GatewaySimulator;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Simulator;

public class DeltaEncoderTests
{
    private static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-09-23T12:00:00Z");

    [Fact]
    public void First_reading_is_full_Body()
    {
        var encoder = new DeltaEncoder();
        var reading = Map(("A", 1), ("B", true), ("C", "x"));

        var sample = encoder.Encode(reading, T0);

        Assert.Equal(T0, sample.SnapshotTimestampUtc);
        Assert.Null(sample.Changes);
        Assert.Equal(3, sample.Body!.Count);
    }

    [Fact]
    public void Identical_reading_is_empty_Changes_heartbeat()
    {
        var encoder = new DeltaEncoder();
        encoder.Encode(Map(("A", 1)), T0);

        var sample = encoder.Encode(Map(("A", 1)), T0.AddSeconds(1));

        Assert.Null(sample.Body);
        Assert.NotNull(sample.Changes);
        Assert.Empty(sample.Changes!);
    }

    [Fact]
    public void Only_changed_keys_carry_new_values()
    {
        var encoder = new DeltaEncoder();
        encoder.Encode(Map(("A", 1), ("B", 2), ("C", 3)), T0);

        var sample = encoder.Encode(Map(("A", 1), ("B", 20), ("C", 3)), T0.AddSeconds(1));

        Assert.Equal(new[] { "B" }, sample.Changes!.Keys);
        Assert.Equal(20, sample.Changes["B"].GetInt32());
    }

    [Fact]
    public void Delta_is_chained_against_previous_tick_not_baseline()
    {
        var encoder = new DeltaEncoder();
        encoder.Encode(Map(("A", 1)), T0);
        encoder.Encode(Map(("A", 2)), T0.AddSeconds(1));

        var sample = encoder.Encode(Map(("A", 2)), T0.AddSeconds(2));

        Assert.Empty(sample.Changes!);
    }

    [Fact]
    public void Bool_flip_and_string_change_are_detected()
    {
        var encoder = new DeltaEncoder();
        encoder.Encode(Map(("Flag", false), ("Ip", "0.0.103.9")), T0);

        var sample = encoder.Encode(Map(("Flag", true), ("Ip", "0.0.103.10")), T0.AddSeconds(1));

        Assert.True(sample.Changes!["Flag"].GetBoolean());
        Assert.Equal("0.0.103.10", sample.Changes["Ip"].GetString());
    }

    [Fact]
    public void Mutating_the_input_after_encode_does_not_affect_lastSent()
    {
        var encoder = new DeltaEncoder();
        var reading = Map(("A", 1));
        encoder.Encode(reading, T0);
        reading["A"] = J(99);

        var sample = encoder.Encode(Map(("A", 1)), T0.AddSeconds(1));

        Assert.Empty(sample.Changes!);
    }
}
