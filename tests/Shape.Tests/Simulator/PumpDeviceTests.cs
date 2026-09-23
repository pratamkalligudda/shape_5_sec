using System.Text.Json;
using Shape.GatewaySimulator;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Simulator;

public class PumpDeviceTests
{
    private static List<Dictionary<string, JsonElement>> ReadMany(PumpDevice device, int n) =>
        Enumerable.Range(0, n).Select(_ => device.Read()).ToList();

    [Fact]
    public void Every_reading_has_exactly_the_seed_keys()
    {
        var seed = Seed();
        var device = new PumpDevice(seed, randomSeed: 1);

        foreach (var reading in ReadMany(device, 50))
            Assert.True(reading.Keys.ToHashSet().SetEquals(seed.Keys));
    }

    [Fact]
    public void Same_random_seed_gives_identical_sequences()
    {
        var a = ReadMany(new PumpDevice(Seed(), randomSeed: 42), 200);
        var b = ReadMany(new PumpDevice(Seed(), randomSeed: 42), 200);

        for (var i = 0; i < a.Count; i++)
            Assert.Equal(JsonSerializer.Serialize(a[i]), JsonSerializer.Serialize(b[i]));
    }

    [Fact]
    public void Generated_values_stay_within_declared_ranges()
    {
        foreach (var r in ReadMany(new PumpDevice(Seed(), randomSeed: 7), 2000))
        {
            var psi = r["OperatingPressurePSI"].GetInt32();
            Assert.InRange(psi, 54000 - 3000 - 150, 54000 + 3000 + 150);
            Assert.Equal((int)Math.Round(psi / 14.5038), r["OperatingPressureBar"].GetInt32());
            Assert.InRange(r["StrokeRateLeft"].GetInt32(), 80, 100);
            Assert.InRange(r["StrokeRateRight"].GetInt32(), 80, 100);
            Assert.InRange(r["StrokeTimeLeft"].GetInt32(), 5200, 5400);
            Assert.InRange(r["StrokeTimeRight"].GetInt32(), 5200, 5400);
            var f = r["HydraulicOilTemperatureF"].GetInt32();
            Assert.InRange(f, 100, 115);
            Assert.Equal((int)Math.Round((f - 32) * 5 / 9.0), r["HydraulicOilTemperatureC"].GetInt32());
            Assert.InRange(r["ProportionalOutputValue"].GetInt32(), 70, 90);
            Assert.True(r["MotorRunning"].GetBoolean());
        }
    }

    [Fact]
    public void Oil_temperature_changes_only_on_every_tenth_tick()
    {
        var readings = ReadMany(new PumpDevice(Seed(), randomSeed: 3), 100);

        for (var t = 1; t < readings.Count; t++)
        {
            if (t % 10 == 0) continue;
            Assert.Equal(readings[t - 1]["HydraulicOilTemperatureF"].GetInt32(), readings[t]["HydraulicOilTemperatureF"].GetInt32());
        }
    }

    [Fact]
    public void Stroke_counter_is_monotonic_and_static_fields_never_change()
    {
        var seed = Seed();
        var readings = ReadMany(new PumpDevice(seed, randomSeed: 5), 300);

        for (var t = 1; t < readings.Count; t++)
            Assert.True(readings[t]["StrokeCounterTW1"].GetInt64() >= readings[t - 1]["StrokeCounterTW1"].GetInt64());
        Assert.True(readings[^1]["StrokeCounterTW1"].GetInt64() > seed["StrokeCounterTW1"].GetInt64());

        foreach (var r in readings)
        {
            Assert.Equal(seed["PumpSerialNumber"].GetRawText(), r["PumpSerialNumber"].GetRawText());
            Assert.Equal(seed["IPAddressRemoteStation"].GetRawText(), r["IPAddressRemoteStation"].GetRawText());
            Assert.Equal(seed["HighPressureSetpointPSI"].GetRawText(), r["HighPressureSetpointPSI"].GetRawText());
        }
    }

    [Fact]
    public void Flags_are_detected_by_name()
    {
        var flags = PumpDevice.FlagKeys(Seed().Keys);

        Assert.Equal(17, flags.Count);
        Assert.Contains("BoosterPressureFault", flags);
        Assert.Contains("OperatingAlarmActive", flags);
        Assert.DoesNotContain("MotorRunning", flags);
    }

    [Fact]
    public void Flag_probability_zero_keeps_all_flags_off_and_one_turns_them_on()
    {
        var off = new PumpDevice(Seed(), randomSeed: 1, flagProbability: 0).Read();
        var on = new PumpDevice(Seed(), randomSeed: 1, flagProbability: 1).Read();

        Assert.False(off["OilFilterWarning"].GetBoolean());
        Assert.Equal(0, off["OperatingAlarmActive"].GetInt32());
        Assert.True(on["OilFilterWarning"].GetBoolean());
        Assert.Equal(1, on["OperatingAlarmActive"].GetInt32());   // int flag toggles 0↔1, stays an int
    }
}
