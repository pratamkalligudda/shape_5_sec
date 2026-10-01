using System.Text.Json;
using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public static class TestConfig
{
    public static DeviceConfig Device() => DeviceConfig.Load(DeviceConfig.DefaultPath);
    public static Dictionary<string, JsonElement> Seed() => PumpSeed.Load(PumpSeed.DefaultPath);
    public static JsonElement J(object value) => JsonSerializer.SerializeToElement(value);
}
