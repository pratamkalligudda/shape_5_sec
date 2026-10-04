namespace Shape.LiveIngestion.LivePolling;

public static class SignalRNames
{
    public const string Hub = "iot_telemetry";
    public const string Target = "liveTelemetryUpdate";
    public const string ConnectionSetting = "AzureSignalRConnectionString";

    public static string DeviceGroup(Guid deviceGuid) => DeviceGroup(deviceGuid.ToString());
    public static string DeviceGroup(string deviceId) => $"telemetry:device:{deviceId}";
}
