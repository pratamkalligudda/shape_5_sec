using System.Text.Json;

namespace Shape.Contracts;

public static class PumpSeed
{
    public const string FileName = "pump-sample.json";

    public static Dictionary<string, JsonElement> Load(string path)
    {
        using var stream = File.OpenRead(path);
        return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(stream)
            ?? throw new InvalidDataException($"Seed file '{path}' is empty.");
    }

    public static Dictionary<string, JsonElement> LoadDefault() =>
        Load(Path.Combine(AppContext.BaseDirectory, FileName));
}
