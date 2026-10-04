namespace Shape.LiveBackend.Api;

public static class ErrorCodes
{
    public const string AlreadyActive = "LIVE_SESSION_ALREADY_ACTIVE";
    public const string CapacityReached = "LIVE_SESSION_CAPACITY_REACHED";
    public const string NotActive = "LIVE_SESSION_NOT_ACTIVE";
    public const string ExtensionLimitReached = "LIVE_SESSION_EXTENSION_LIMIT_REACHED";
    public const string InvalidParameters = "LIVE_SESSION_INVALID_PARAMETERS";
    public const string DeviceNotFound = "DEVICE_NOT_FOUND";
    public const string GatewayNotPaired = "GATEWAY_NOT_PAIRED";
    public const string GatewayUnreachable = "GATEWAY_UNREACHABLE";
}
