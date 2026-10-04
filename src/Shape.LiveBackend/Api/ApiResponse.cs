using System.Text.Json.Serialization;

namespace Shape.LiveBackend.Api;

/// <summary>The backend envelope: { succeeded, message, errors, data }; errors omitted when null, errors[0] = code.</summary>
public sealed class ApiResponse
{
    public bool Succeeded { get; init; }
    public string? Message { get; init; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IReadOnlyList<string>? Errors { get; init; }

    public object? Data { get; init; }

    public static ApiResponse Ok(object? data, string? message = null) => new() { Succeeded = true, Message = message, Data = data };

    public static ApiResponse Fail(string code, string message, IEnumerable<string>? details = null) =>
        new() { Succeeded = false, Message = message, Errors = [code, .. details ?? []] };
}
