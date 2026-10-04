using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Functions;

/// <summary>Spec §5.3. No auth: the caller names itself with x-user-id.</summary>
public sealed class NegotiatorFunctions
{
    public const string UserHeader = "x-user-id";

    public sealed class GroupActionOutput
    {
        [SignalROutput(HubName = SignalRNames.Hub, ConnectionStringSetting = SignalRNames.ConnectionSetting)]
        public SignalRGroupAction? Action { get; set; }

        public HttpResponseData Response { get; set; } = default!;
    }

    [Function("negotiate")]
    public static async Task<HttpResponseData> Negotiate(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "negotiate")] HttpRequestData request,
        [SignalRConnectionInfoInput(HubName = SignalRNames.Hub, UserId = "{headers.x-user-id}",
            ConnectionStringSetting = SignalRNames.ConnectionSetting)] string connectionInfo)
    {
        if (UserId(request) is null) return await BadRequest(request, $"Header '{UserHeader}' is required.");
        var response = request.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/json");
        await response.WriteStringAsync(connectionInfo);
        return response;
    }

    [Function("join-device")]
    public static Task<GroupActionOutput> Join(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "join-device")] HttpRequestData request) =>
        Group(request, SignalRGroupActionType.Add);

    [Function("leave-device")]
    public static Task<GroupActionOutput> Leave(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "leave-device")] HttpRequestData request) =>
        Group(request, SignalRGroupActionType.Remove);

    private static async Task<GroupActionOutput> Group(HttpRequestData request, SignalRGroupActionType type)
    {
        var user = UserId(request);
        var deviceId = System.Web.HttpUtility.ParseQueryString(request.Url.Query)["deviceId"];
        if (user is null) return new GroupActionOutput { Response = await BadRequest(request, $"Header '{UserHeader}' is required.") };
        if (string.IsNullOrWhiteSpace(deviceId))
            return new GroupActionOutput { Response = await BadRequest(request, "Query parameter 'deviceId' is required.") };

        return new GroupActionOutput
        {
            Action = new SignalRGroupAction(type) { UserId = user, GroupName = SignalRNames.DeviceGroup(deviceId) },
            Response = request.CreateResponse(HttpStatusCode.OK),
        };
    }

    private static string? UserId(HttpRequestData request) =>
        request.Headers.TryGetValues(UserHeader, out var values) && values.FirstOrDefault() is { Length: > 0 } v ? v : null;

    private static async Task<HttpResponseData> BadRequest(HttpRequestData request, string message)
    {
        var response = request.CreateResponse(HttpStatusCode.BadRequest);
        await response.WriteStringAsync(message);
        return response;
    }
}
