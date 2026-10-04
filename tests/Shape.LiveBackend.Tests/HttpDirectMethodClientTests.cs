using System.Net;
using System.Text;
using System.Text.Json;
using Shape.LiveBackend.Gateway;

namespace Shape.LiveBackend.Tests;

public class HttpDirectMethodClientTests
{
    private sealed class StubHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send) : HttpMessageHandler
    {
        public HttpRequestMessage? Last { get; private set; }
        public string? LastBody { get; private set; }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Last = request;
            LastBody = request.Content is null ? null : await request.Content.ReadAsStringAsync(ct);
            return await send(request, ct);
        }
    }

    private static (HttpDirectMethodClient, StubHandler) Client(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> send,
        TimeSpan? grace = null)
    {
        var handler = new StubHandler(send);
        var client = new HttpDirectMethodClient(new HttpClient(handler) { BaseAddress = new Uri("http://localhost:5100") })
        {
            Grace = grace ?? HttpDirectMethodClient.DefaultGrace,
        };
        return (client, handler);
    }

    private static HttpResponseMessage Json(HttpStatusCode code, string body) =>
        new(code) { Content = new StringContent(body, Encoding.UTF8, "application/json") };

    [Fact]
    public async Task Posts_the_iot_hub_invocation_and_returns_module_status_and_payload()
    {
        var (client, handler) = Client((_, _) => Task.FromResult(Json(HttpStatusCode.OK, """{"status":409,"payload":{"result":"rejected"}}""")));

        var r = await client.InvokeAsync("gw-001", "DefaultModeModule", "FastPollingEnable", new { pollIntervalSeconds = 1 }, TimeSpan.FromSeconds(10));

        Assert.Equal(409, r.Status);
        Assert.Equal("rejected", r.Payload!.Value.GetProperty("result").GetString());
        Assert.Equal("http://localhost:5100/twins/gw-001/modules/DefaultModeModule/methods", handler.Last!.RequestUri!.ToString());
        var sent = JsonDocument.Parse(handler.LastBody!).RootElement;
        Assert.Equal("FastPollingEnable", sent.GetProperty("methodName").GetString());
        Assert.Equal(1, sent.GetProperty("payload").GetProperty("pollIntervalSeconds").GetInt32());
        Assert.Equal(10, sent.GetProperty("responseTimeoutInSeconds").GetInt32());
    }

    [Theory]
    [InlineData(HttpStatusCode.NotFound)]
    [InlineData(HttpStatusCode.GatewayTimeout)]
    [InlineData(HttpStatusCode.InternalServerError)]
    public async Task Hub_failures_are_gateway_unreachable(HttpStatusCode code)
    {
        var (client, _) = Client((_, _) => Task.FromResult(Json(code, """{"message":"x"}""")));
        await Assert.ThrowsAsync<GatewayUnreachableException>(() =>
            client.InvokeAsync("gw-001", "m", "FastPollingDisable", null, TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Connection_refused_is_gateway_unreachable()
    {
        var (client, _) = Client((_, _) => throw new HttpRequestException("refused"));
        await Assert.ThrowsAsync<GatewayUnreachableException>(() =>
            client.InvokeAsync("gw-001", "m", "FastPollingDisable", null, TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Hub_that_never_answers_times_out_as_gateway_unreachable()
    {
        var (client, _) = Client(async (_, ct) => { await Task.Delay(Timeout.Infinite, ct); return new HttpResponseMessage(); },
            grace: TimeSpan.Zero);
        await Assert.ThrowsAsync<GatewayUnreachableException>(() =>
            client.InvokeAsync("gw-001", "m", "FastPollingDisable", null, TimeSpan.FromMilliseconds(50)));
    }
}
