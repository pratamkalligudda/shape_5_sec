using System.Text.Json;
using Shape.LiveBackend.Api;

namespace Shape.LiveBackend.Tests;

public class ApiResponseTests
{
    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    [Fact]
    public void Success_omits_errors_and_writes_message_and_data()
    {
        var json = JsonSerializer.Serialize(ApiResponse.Ok(new { active = false }), Web);
        Assert.Equal("""{"succeeded":true,"message":null,"data":{"active":false}}""", json);
    }

    [Fact]
    public void Failure_puts_the_code_first_and_details_after()
    {
        var r = ApiResponse.Fail(ErrorCodes.InvalidParameters, "Invalid live session parameters.",
            ["pollIntervalSeconds must be between 1 and 60."]);

        Assert.False(r.Succeeded);
        Assert.Equal(new[] { "LIVE_SESSION_INVALID_PARAMETERS", "pollIntervalSeconds must be between 1 and 60." }, r.Errors);
        Assert.Null(r.Data);
    }

    [Fact]
    public void Error_codes_are_the_contract_strings()
    {
        Assert.Equal(
            new[] { "LIVE_SESSION_ALREADY_ACTIVE", "LIVE_SESSION_CAPACITY_REACHED", "LIVE_SESSION_NOT_ACTIVE",
                "LIVE_SESSION_EXTENSION_LIMIT_REACHED", "LIVE_SESSION_INVALID_PARAMETERS", "DEVICE_NOT_FOUND",
                "GATEWAY_NOT_PAIRED", "GATEWAY_UNREACHABLE" },
            new[] { ErrorCodes.AlreadyActive, ErrorCodes.CapacityReached, ErrorCodes.NotActive, ErrorCodes.ExtensionLimitReached,
                ErrorCodes.InvalidParameters, ErrorCodes.DeviceNotFound, ErrorCodes.GatewayNotPaired, ErrorCodes.GatewayUnreachable });
    }
}
