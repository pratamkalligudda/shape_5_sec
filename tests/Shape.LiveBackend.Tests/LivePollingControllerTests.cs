using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;

namespace Shape.LiveBackend.Tests;

/// <summary>The HTTP shape: routes, envelope, camelCase, Z dates, empty-body handling, model-state errors.</summary>
public sealed class LivePollingControllerTests : IDisposable
{
    private const string Base = "/api/v1/devices/PUMP-00417/live-session";
    private readonly TempDb _db = new();
    private readonly FakeDirectMethodClient _gateway = new();
    private readonly WebApplicationFactory<Program> _factory;

    public LivePollingControllerTests()
    {
        _factory = new WebApplicationFactory<Program>().WithWebHostBuilder(b =>
        {
            b.UseSetting("LivePolling:SessionDbPath", _db.Path);
            b.ConfigureServices(s =>
            {
                s.RemoveAll<IDirectMethodClient>();
                s.AddSingleton<IDirectMethodClient>(_gateway);
                s.RemoveAll<ILiveSessionReader>();
                s.AddSingleton<ILiveSessionReader>(new FakeLiveSessionReader());
            });
        });
    }

    public void Dispose()
    {
        _factory.Dispose();
        _db.Dispose();
    }

    private static async Task<JsonElement> Body(HttpResponseMessage r) => JsonDocument.Parse(await r.Content.ReadAsStringAsync()).RootElement;

    private void GatewayAccepts(Guid id) => _gateway.Respond = call => call.MethodName == "FastPollingEnable"
        ? FakeDirectMethodClient.Result(200, $$"""{"result":"accepted","durationMinutes":10,"intervalSeconds":1,"sessionId":"{{id}}","defaultsApplied":[]}""")
        : FakeDirectMethodClient.Result(200, $$"""{"result":"accepted","sessionId":"{{id}}"}""");

    [Fact]
    public async Task Post_without_a_body_starts_and_returns_the_camel_case_envelope()
    {
        var id = Guid.NewGuid();
        GatewayAccepts(id);

        var r = await _factory.CreateClient().PostAsync(Base, null);

        Assert.Equal(HttpStatusCode.OK, r.StatusCode);
        var body = await Body(r);
        Assert.True(body.GetProperty("succeeded").GetBoolean());
        Assert.False(body.TryGetProperty("errors", out _));
        var data = body.GetProperty("data");
        Assert.Equal(id.ToString(), data.GetProperty("sessionId").GetString());
        Assert.Equal("PUMP-00417", data.GetProperty("deviceId").GetString());
        Assert.EndsWith("Z", data.GetProperty("expiresUtc").GetString());
        Assert.True(data.GetProperty("canExtend").GetBoolean());
    }

    [Fact]
    public async Task Post_with_an_empty_json_object_uses_defaults()
    {
        GatewayAccepts(Guid.NewGuid());
        var r = await _factory.CreateClient().PostAsync(Base, new StringContent("{}", Encoding.UTF8, "application/json"));
        Assert.Equal(HttpStatusCode.OK, r.StatusCode);
    }

    [Fact]
    public async Task Post_with_a_non_integer_field_is_invalid_parameters_in_the_envelope()
    {
        var r = await _factory.CreateClient().PostAsync(Base,
            new StringContent("""{"pollIntervalSeconds":"fast"}""", Encoding.UTF8, "application/json"));

        Assert.Equal(HttpStatusCode.BadRequest, r.StatusCode);
        var body = await Body(r);
        Assert.False(body.GetProperty("succeeded").GetBoolean());
        Assert.Equal("LIVE_SESSION_INVALID_PARAMETERS", body.GetProperty("errors")[0].GetString());
        Assert.True(body.GetProperty("errors").GetArrayLength() >= 2);
    }

    [Fact]
    public async Task Unknown_device_is_404_device_not_found()
    {
        var r = await _factory.CreateClient().GetAsync("/api/v1/devices/NOPE/live-session");

        Assert.Equal(HttpStatusCode.NotFound, r.StatusCode);
        Assert.Equal("DEVICE_NOT_FOUND", (await Body(r)).GetProperty("errors")[0].GetString());
    }

    [Fact]
    public async Task Get_when_idle_is_200_with_active_false()
    {
        var body = await Body(await _factory.CreateClient().GetAsync(Base));
        var data = body.GetProperty("data");
        Assert.False(data.GetProperty("active").GetBoolean());
        Assert.Equal(JsonValueKind.Null, data.GetProperty("sessionId").ValueKind);
    }

    [Fact]
    public async Task Delete_after_start_returns_the_stopped_message_and_null_data()
    {
        GatewayAccepts(Guid.NewGuid());
        var client = _factory.CreateClient();
        await client.PostAsync(Base, null);

        var r = await client.DeleteAsync(Base);

        var body = await Body(r);
        Assert.Equal(HttpStatusCode.OK, r.StatusCode);
        Assert.Equal("Live session stopped.", body.GetProperty("message").GetString());
        Assert.Equal(JsonValueKind.Null, body.GetProperty("data").ValueKind);
    }

    [Fact]
    public async Task Extend_without_a_session_is_409_not_active()
    {
        var r = await _factory.CreateClient().PostAsync(Base + "/extend", null);
        Assert.Equal(HttpStatusCode.Conflict, r.StatusCode);
        Assert.Equal("LIVE_SESSION_NOT_ACTIVE", (await Body(r)).GetProperty("errors")[0].GetString());
    }

    [Fact]
    public async Task Comma_separated_metrics_are_one_invalid_name()
    {
        var r = await _factory.CreateClient().GetAsync(Base + "/series?metric=OperatingPressurePSI,StrokeRateLeft");

        Assert.Equal(HttpStatusCode.BadRequest, r.StatusCode);
        var errors = (await Body(r)).GetProperty("errors");
        Assert.Equal("LIVE_SESSION_INVALID_PARAMETERS", errors[0].GetString());
        Assert.Equal("'OperatingPressurePSI,StrokeRateLeft' is not a known metric.", errors[1].GetString());
        Assert.StartsWith("Valid metrics: ", errors[errors.GetArrayLength() - 1].GetString());
    }

    [Fact]
    public async Task Repeated_metric_parameters_bind_and_reach_the_session_check()
    {
        var r = await _factory.CreateClient().GetAsync(Base + "/series?metric=OperatingPressurePSI&metric=strokerateleft");

        Assert.Equal(HttpStatusCode.Conflict, r.StatusCode);
        Assert.Equal("LIVE_SESSION_NOT_ACTIVE", (await Body(r)).GetProperty("errors")[0].GetString());
    }

    [Fact]
    public async Task Unparseable_since_is_invalid_parameters()
    {
        var r = await _factory.CreateClient().GetAsync(Base + "/series?since=yesterday");

        Assert.Equal(HttpStatusCode.BadRequest, r.StatusCode);
        Assert.Equal("LIVE_SESSION_INVALID_PARAMETERS", (await Body(r)).GetProperty("errors")[0].GetString());
    }
}
