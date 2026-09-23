using System.Text.Json;
using Shape.Contracts;

namespace Shape.Tests;

public static class TestJson
{
    public static JsonElement J(object value) => JsonSerializer.SerializeToElement(value);

    public static Dictionary<string, JsonElement> Map(params (string Key, object Value)[] entries) =>
        entries.ToDictionary(e => e.Key, e => J(e.Value));

    public static Dictionary<string, JsonElement> Seed() => PumpSeed.LoadDefault();
}
