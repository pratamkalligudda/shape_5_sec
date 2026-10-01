using System.Text.Json;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class PumpDeviceTests
{
    [Fact]
    public void Device_config_loads()
    {
        var d = Device();
        Assert.Equal("PUMP-00417", d.SerialNumber);
        Assert.Equal(Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), d.DeviceGuid);
        Assert.Equal("gw-001", d.GatewayId);
        Assert.Equal("DefaultModeModule", d.ModuleId);
    }

    [Fact]
    public void Seed_contains_every_event_parameter()
    {
        var seed = Seed();
        Assert.Equal(73, seed.Count);
        Assert.Equal(18, EventParameters.All.Count);
        Assert.All(EventParameters.All, p => Assert.True(seed.ContainsKey(p), p));
    }

    [Fact]
    public void Reading_has_exactly_the_seed_keys()
    {
        var seed = Seed();
        var reading = new PumpDevice(seed, randomSeed: 1).Read(EventParameters.Initial());
        Assert.True(reading.Keys.ToHashSet().SetEquals(seed.Keys));
    }

    [Fact]
    public void Same_seed_is_deterministic()
    {
        var a = new PumpDevice(Seed(), randomSeed: 7);
        var b = new PumpDevice(Seed(), randomSeed: 7);
        for (var i = 0; i < 100; i++)
            Assert.Equal(JsonSerializer.Serialize(a.Read(EventParameters.Initial())),
                JsonSerializer.Serialize(b.Read(EventParameters.Initial())));
    }

    [Fact]
    public void Generated_values_stay_in_range()
    {
        var device = new PumpDevice(Seed(), randomSeed: 3);
        for (var i = 0; i < 1000; i++)
        {
            var r = device.Read(EventParameters.Initial());
            Assert.InRange(r["OperatingPressurePSI"].GetInt32(), 50850, 57150);
            Assert.InRange(r["StrokeRateLeft"].GetInt32(), 80, 100);
            Assert.InRange(r["StrokeTimeRight"].GetInt32(), 5200, 5400);
            Assert.InRange(r["HydraulicOilTemperatureF"].GetInt32(), 100, 115);
            Assert.InRange(r["ProportionalOutputValue"].GetInt32(), 70, 90);
        }
    }

    [Fact]
    public void Event_parameters_mirror_event_values_keeping_seed_types()
    {
        var events = EventParameters.Initial();
        events["OperatingAlarmActive"] = 5;
        events["OilFilterWarning"] = 1;
        events["MotorRunning"] = 0;

        var r = new PumpDevice(Seed(), randomSeed: 1).Read(events);

        Assert.Equal(5, r["OperatingAlarmActive"].GetInt32());   // int in the seed
        Assert.True(r["OilFilterWarning"].GetBoolean());          // bool in the seed
        Assert.False(r["MotorRunning"].GetBoolean());
        Assert.False(r["MaintenanceAlarmActive"].GetBoolean());
    }
}
