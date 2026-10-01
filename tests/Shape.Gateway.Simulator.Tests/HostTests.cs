using System.Net;
using System.Net.Http.Json;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc.Testing;

namespace Shape.Gateway.Simulator.Tests;

public class HostTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _client;

    public HostTests(WebApplicationFactory<Program> factory) => _client = factory.CreateClient();

    private Task<HttpResponseMessage> Invoke(string device, string module, string method, object? payload = null) =>
        _client.PostAsJsonAsync($"/twins/{device}/modules/{module}/methods",
            new { methodName = method, payload, responseTimeoutInSeconds = 5 });

    [Fact]
    public async Task Facade_404s_unknown_device_or_module()
    {
        Assert.Equal(HttpStatusCode.NotFound, (await Invoke("nope", "DefaultModeModule", "FastPollingEnable")).StatusCode);
        Assert.Equal(HttpStatusCode.NotFound, (await Invoke("gw-001", "nope", "FastPollingEnable")).StatusCode);
    }

    [Fact]
    public async Task Facade_wraps_module_status_and_payload_and_scenario_api_works()
    {
        var res = await Invoke("gw-001", "DefaultModeModule", "FastPollingEnable", new { sessionDurationMinutes = 10 });
        Assert.Equal(HttpStatusCode.OK, res.StatusCode);
        var body = await res.Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(200, body.GetProperty("status").GetInt32());
        Assert.Equal("accepted", body.GetProperty("payload").GetProperty("result").GetString());

        var state = await _client.GetFromJsonAsync<JsonElement>("/sim/state");
        Assert.True(state.GetProperty("active").GetBoolean());

        var bad = await _client.PostAsJsonAsync("/sim/events", new Dictionary<string, int> { ["Nope"] = 1 });
        Assert.Equal(HttpStatusCode.BadRequest, bad.StatusCode);
        var ok = await _client.PostAsJsonAsync("/sim/events", new Dictionary<string, int> { ["OperatingAlarmActive"] = 5 });
        Assert.Equal(5, (await ok.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("OperatingAlarmActive").GetInt32());
        await _client.PostAsync("/sim/events/clear", null);

        var disable = await (await Invoke("gw-001", "DefaultModeModule", "FastPollingDisable")).Content.ReadFromJsonAsync<JsonElement>();
        Assert.Equal(200, disable.GetProperty("status").GetInt32());
    }
}
