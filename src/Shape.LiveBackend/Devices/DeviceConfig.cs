using System.Text.Json;

namespace Shape.LiveBackend.Devices;

public sealed record DeviceConfig(string SerialNumber, Guid DeviceGuid, string Name, string? GatewayId, string ModuleId)
{
    public static readonly string DefaultPath = Path.Combine(AppContext.BaseDirectory, "config", "device.json");

    public static DeviceConfig Load(string path) =>
        JsonSerializer.Deserialize<DeviceConfig>(File.ReadAllText(path), new JsonSerializerOptions(JsonSerializerDefaults.Web))
        ?? throw new InvalidDataException($"Device config '{path}' is empty.");
}
