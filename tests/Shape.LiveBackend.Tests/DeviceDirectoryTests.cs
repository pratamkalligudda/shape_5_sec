using Shape.LiveBackend.Devices;

namespace Shape.LiveBackend.Tests;

public class DeviceDirectoryTests
{
    [Fact]
    public void Configured_device_loads_from_config_and_is_found_case_insensitively()
    {
        var device = DeviceConfig.Load(DeviceConfig.DefaultPath);
        var directory = new ConfigDeviceDirectory(device);

        Assert.Equal(Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), directory.FindBySerial("pump-00417")!.DeviceGuid);
        Assert.Equal("gw-001", device.GatewayId);
        Assert.Equal("DefaultModeModule", device.ModuleId);
    }

    [Fact]
    public void Other_serials_are_not_found() =>
        Assert.Null(new ConfigDeviceDirectory(DeviceConfig.Load(DeviceConfig.DefaultPath)).FindBySerial("PUMP-99999"));
}
