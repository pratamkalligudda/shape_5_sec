using System.Text.Json;

namespace Shape.Gateway.Simulator;

public static class PumpSeed
{
    public static readonly string DefaultPath = Path.Combine(AppContext.BaseDirectory, "config", "pump-seed.json");

    public static Dictionary<string, JsonElement> Load(string path) =>
        JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(File.ReadAllText(path))
        ?? throw new InvalidDataException($"Seed '{path}' is empty.");
}
