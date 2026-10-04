namespace Shape.LiveBackend.Devices;

/// <summary>Stand-in for the SQL device + gateway-pairing lookup (spec §11). Serials compare case-insensitively.</summary>
public interface IDeviceDirectory
{
    DeviceConfig? FindBySerial(string serialNumber);
}

public sealed class ConfigDeviceDirectory(DeviceConfig device) : IDeviceDirectory
{
    public DeviceConfig? FindBySerial(string serialNumber) =>
        string.Equals(serialNumber, device.SerialNumber, StringComparison.OrdinalIgnoreCase) ? device : null;
}
