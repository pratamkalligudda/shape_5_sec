namespace Shape.LiveBackend.Api;

/// <summary>HTTP status plus envelope; the controller only translates this to an ObjectResult.</summary>
public sealed record ServiceResult(int StatusCode, ApiResponse Body)
{
    public static ServiceResult Ok(object? data, string? message = null) => new(200, ApiResponse.Ok(data, message));

    public static ServiceResult Fail(int status, string code, string message, IEnumerable<string>? details = null) =>
        new(status, ApiResponse.Fail(code, message, details));
}
