using System.Net;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;

namespace Shape.Functions;

public sealed class NegotiateFunction
{
    [Function("negotiate")]
    public static HttpResponseData Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post")] HttpRequestData request,
        [SignalRConnectionInfoInput(HubName = IngestTelemetryFunction.HubName, ConnectionStringSetting = "AzureSignalRConnectionString")]
        string connectionInfo)
    {
        var response = request.CreateResponse(HttpStatusCode.OK);
        response.Headers.Add("Content-Type", "application/json");
        response.WriteString(connectionInfo);
        return response;
    }
}
