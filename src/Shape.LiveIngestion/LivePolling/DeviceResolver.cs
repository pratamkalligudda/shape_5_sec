namespace Shape.LiveIngestion.LivePolling;

/// <summary>Gateway id → device (spec §5.1 step 3). IoT Hub device ids are case-sensitive.</summary>
public sealed class DeviceResolver(DeviceConfig device)
{
    public bool TryResolve(string gatewayId, out ResolvedDevice resolved)
    {
        if (device.GatewayId is not null && string.Equals(device.GatewayId, gatewayId, StringComparison.Ordinal))
        {
            resolved = new ResolvedDevice(device.DeviceGuid, device.SerialNumber);
            return true;
        }
        resolved = default!;
        return false;
    }
}
