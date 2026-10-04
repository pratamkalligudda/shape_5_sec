using System.Net.Http.Json;
using System.Text.Json;

namespace Shape.LiveBackend.Gateway;

/// <summary>IoT Hub "invoke module method" REST call, against the simulator's facade (spec §4.2).</summary>
public sealed class HttpDirectMethodClient(HttpClient http) : IDirectMethodClient
{
    public static readonly TimeSpan DefaultGrace = TimeSpan.FromSeconds(5);
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    /// <summary>Time beyond responseTimeoutInSeconds that the hub gets to return its own 504.</summary>
    public TimeSpan Grace { get; init; } = DefaultGrace;

    public async Task<DirectMethodResult> InvokeAsync(string deviceId, string moduleId, string methodName, object? payload, TimeSpan timeout,
        CancellationToken ct = default)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        cts.CancelAfter(timeout + Grace);
        var body = new { methodName, payload, responseTimeoutInSeconds = Math.Max(1, (int)Math.Ceiling(timeout.TotalSeconds)) };
        try
        {
            using var response = await http.PostAsJsonAsync(
                $"twins/{Uri.EscapeDataString(deviceId)}/modules/{Uri.EscapeDataString(moduleId)}/methods", body, Web, cts.Token);
            if (!response.IsSuccessStatusCode)
                throw new GatewayUnreachableException($"IoT Hub returned {(int)response.StatusCode} for {methodName}.");
            var result = await response.Content.ReadFromJsonAsync<JsonElement>(Web, cts.Token);
            JsonElement? resultPayload = result.TryGetProperty("payload", out var p) ? p.Clone() : null;
            return new DirectMethodResult(result.GetProperty("status").GetInt32(), resultPayload);
        }
        catch (OperationCanceledException ex) when (!ct.IsCancellationRequested)
        {
            throw new GatewayUnreachableException($"{methodName} timed out after {timeout.TotalSeconds:0.#} s.", ex);
        }
        catch (HttpRequestException ex)
        {
            throw new GatewayUnreachableException($"IoT Hub could not be reached for {methodName}: {ex.Message}", ex);
        }
    }
}
