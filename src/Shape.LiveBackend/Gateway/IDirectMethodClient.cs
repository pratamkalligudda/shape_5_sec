using System.Text.Json;

namespace Shape.LiveBackend.Gateway;

/// <summary>Status is the module's own status (200/400/409/501), not the hub's HTTP status.</summary>
public sealed record DirectMethodResult(int Status, JsonElement? Payload);

/// <summary>The hub could not reach the module: timeout, 404, 504, any other hub failure, or connection refused.</summary>
public sealed class GatewayUnreachableException(string message, Exception? inner = null) : Exception(message, inner);

public interface IDirectMethodClient
{
    Task<DirectMethodResult> InvokeAsync(string deviceId, string moduleId, string methodName, object? payload, TimeSpan timeout,
        CancellationToken ct = default);
}
