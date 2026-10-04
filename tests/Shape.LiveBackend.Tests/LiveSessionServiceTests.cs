using System.Text.Json;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Sessions;
using static Shape.LiveBackend.Tests.ServiceFixture;

namespace Shape.LiveBackend.Tests;

public sealed class LiveSessionServiceTests : IDisposable
{
    private readonly ServiceFixture _f = new();
    public void Dispose() => _f.Dispose();

    private static string Code(ServiceResult r) => r.Body.Errors![0];

    // ---- device lookup ----

    [Fact]
    public async Task Unknown_serial_is_device_not_found()
    {
        var r = await _f.Service.StartAsync("PUMP-99999", null, default);
        Assert.Equal((404, ErrorCodes.DeviceNotFound), (r.StatusCode, Code(r)));
    }

    [Fact]
    public async Task Device_without_gateway_is_gateway_not_paired()
    {
        using var f = new ServiceFixture(Device with { GatewayId = null });
        var r = await f.Service.GetStatusAsync("PUMP-00417", default);
        Assert.Equal((404, ErrorCodes.GatewayNotPaired), (r.StatusCode, Code(r)));
    }

    // ---- start ----

    [Fact]
    public async Task Start_without_body_sends_config_values_and_records_the_session()
    {
        var id = Guid.NewGuid();
        _f.GatewayAccepts(id);
        _f.Options.PollIntervalSeconds = 2;

        var r = await _f.Service.StartAsync("pump-00417", null, default);

        Assert.Equal(200, r.StatusCode);
        var call = Assert.Single(_f.Gateway.Calls);
        Assert.Equal(("gw-001", "DefaultModeModule", "FastPollingEnable"), (call.DeviceId, call.ModuleId, call.MethodName));
        Assert.Equal(2, call.Payload.GetProperty("pollIntervalSeconds").GetInt32());
        Assert.Equal(10, call.Payload.GetProperty("sessionDurationMinutes").GetInt32());
        var dto = Assert.IsType<LiveSessionDto>(r.Body.Data);
        Assert.Equal(new LiveSessionDto(id, "PUMP-00417", 2, 10, T0, T0.AddMinutes(10), 10, true), dto);
        var row = (await _f.Sessions.GetAsync(id))!;
        Assert.Equal(("local", "gw-001", T0.AddMinutes(10)), (row.StartedByUserId, row.GatewayId, row.ExpiresUtc));
    }

    [Fact]
    public async Task Start_uses_the_values_the_gateway_accepted()
    {
        var id = Guid.NewGuid();
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(200,
            $$"""{"result":"accepted","durationMinutes":15,"intervalSeconds":5,"sessionId":"{{id}}","defaultsApplied":[]}""");

        var r = await _f.Service.StartAsync("PUMP-00417", new StartLiveSessionRequest(1, 15), default);

        var dto = Assert.IsType<LiveSessionDto>(r.Body.Data);
        Assert.Equal((5, 15, T0.AddMinutes(15)), (dto.PollIntervalSeconds, dto.TotalDurationMinutes, dto.ExpiresUtc));
    }

    [Fact]
    public async Task Start_out_of_range_lists_every_field_without_calling_the_gateway()
    {
        var r = await _f.Service.StartAsync("PUMP-00417", new StartLiveSessionRequest(0, 31), default);

        Assert.Equal(400, r.StatusCode);
        Assert.Equal(new[] { ErrorCodes.InvalidParameters, "pollIntervalSeconds must be between 1 and 60.",
            "sessionDurationMinutes must be between 10 and 30." }, r.Body.Errors);
        Assert.Empty(_f.Gateway.Calls);
    }

    [Fact]
    public async Task Start_with_an_open_session_is_already_active_and_names_the_expiry()
    {
        await _f.StartedSessionAsync();

        var r = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((409, ErrorCodes.AlreadyActive), (r.StatusCode, Code(r)));
        Assert.Contains("2026-10-01T10:10:00.0000000Z", r.Body.Message);
        Assert.Empty(_f.Gateway.Calls);
    }

    [Fact]
    public async Task Start_at_fleet_capacity_is_capacity_reached()
    {
        for (var i = 0; i < 5; i++)
            await _f.Sessions.InsertAsync(new LivePollingSessionRecord
            {
                SessionId = Guid.NewGuid(), DeviceGuid = Guid.NewGuid(), DeviceId = $"OTHER-{i}", GatewayId = "gw-x",
                StartedUtc = T0, PollIntervalSeconds = 1, SessionDurationMinutes = 10, ExpiresUtc = T0.AddMinutes(10),
            });

        var r = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((409, ErrorCodes.CapacityReached), (r.StatusCode, Code(r)));
    }

    [Theory]
    [InlineData(409, """{"result":"rejected","reason":"A fast polling session is already active."}""", 409, ErrorCodes.AlreadyActive)]
    [InlineData(400, """{"result":"rejected","reason":"Invalid request.","errors":["'pollIntervalSeconds'=0 is outside the allowed range (1-60)."]}""",
        400, ErrorCodes.InvalidParameters)]
    public async Task Start_maps_module_rejections(int moduleStatus, string payload, int http, string code)
    {
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(moduleStatus, payload);

        var r = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((http, code), (r.StatusCode, Code(r)));
        if (moduleStatus == 400) Assert.Equal("'pollIntervalSeconds'=0 is outside the allowed range (1-60).", r.Body.Errors![1]);
        Assert.Null(await _f.Sessions.GetOpenForDeviceAsync(Device.DeviceGuid, T0));
    }

    [Fact]
    public async Task Start_when_unreachable_is_503_and_records_nothing()
    {
        _f.Gateway.Respond = _ => throw new GatewayUnreachableException("timeout");

        var r = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((503, ErrorCodes.GatewayUnreachable), (r.StatusCode, Code(r)));
        Assert.Equal(0, await _f.Sessions.CountOpenAsync(T0));
    }

    // ---- status ----

    [Fact]
    public async Task Status_without_a_session_is_inactive_with_explicit_nulls()
    {
        var r = await _f.Service.GetStatusAsync("PUMP-00417", default);

        var data = Json.Of(r).GetProperty("data");
        Assert.False(data.GetProperty("active").GetBoolean());
        Assert.Equal(12, data.EnumerateObject().Count());
        Assert.All(data.EnumerateObject().Where(p => p.Name != "active"), p => Assert.Equal(JsonValueKind.Null, p.Value.ValueKind));
    }

    [Fact]
    public async Task Status_before_the_first_tick_has_null_live_fields()
    {
        var id = await _f.StartedSessionAsync();
        _f.Time.Advance(TimeSpan.FromSeconds(30.6));

        var dto = Assert.IsType<LiveSessionStatusDto>((await _f.Service.GetStatusAsync("PUMP-00417", default)).Body.Data);

        Assert.True(dto.Active);
        Assert.Equal(id, dto.SessionId);
        Assert.Equal(569, dto.SecondsRemaining);
        Assert.Null(dto.DeviceStatus);
        Assert.Null(dto.Snapshot);
        Assert.Null(dto.TickCount);
    }

    [Fact]
    public async Task Status_with_a_head_returns_snapshot_and_counters()
    {
        var id = await _f.StartedSessionAsync();
        _f.Reader.Heads[id] = new LiveHeadView(new() { ["P"] = JsonSerializer.SerializeToElement(1) }, 0, T0.AddSeconds(14), 15, 2100,
            T0.AddSeconds(1), T0.AddSeconds(14));
        _f.Time.Advance(TimeSpan.FromMinutes(10) - TimeSpan.FromMilliseconds(400));

        var dto = Assert.IsType<LiveSessionStatusDto>((await _f.Service.GetStatusAsync("PUMP-00417", default)).Body.Data);

        Assert.Equal(0, dto.SecondsRemaining);                       // floored, never negative
        Assert.Equal(0, dto.DeviceStatus);
        Assert.Equal(15, dto.TickCount);
        Assert.Equal(2100, dto.BytesTotal);
        Assert.Equal(T0.AddSeconds(14), dto.SnapshotTimestampUtc);
        Assert.Equal(1, dto.Snapshot!["P"].GetInt32());
    }

    [Fact]
    public async Task Status_table_failure_is_500_with_a_message()
    {
        await _f.StartedSessionAsync();
        _f.Reader.Throw = new Azure.RequestFailedException(503, "table down");

        var r = await _f.Service.GetStatusAsync("PUMP-00417", default);

        Assert.Equal(500, r.StatusCode);
        Assert.False(r.Body.Succeeded);
        Assert.NotNull(r.Body.Message);
    }

    // ---- extend ----

    private void GatewayExtends(int total) => _f.Gateway.Respond = call => FakeDirectMethodClient.Result(200,
        $$"""{"result":"accepted","sessionId":"{{call.Payload.GetProperty("sessionId").GetString()}}","extendedByMinutes":10,"totalDurationMinutes":{{total}}}""");

    [Fact]
    public async Task Extend_measures_the_new_expiry_from_the_original_start()
    {
        var id = await _f.StartedSessionAsync();
        _f.Time.Advance(TimeSpan.FromMinutes(5));
        GatewayExtends(20);

        var r = await _f.Service.ExtendAsync("PUMP-00417", default);

        var call = Assert.Single(_f.Gateway.Calls);
        Assert.Equal("Extend-FastPolling", call.MethodName);
        Assert.Equal(id.ToString(), call.Payload.GetProperty("sessionId").GetString());
        Assert.Equal(10, call.Payload.GetProperty("extendSessionDurationMinutes").GetInt32());
        var dto = Assert.IsType<LiveSessionDto>(r.Body.Data);
        Assert.Equal((T0.AddMinutes(20), 20, 20, true), (dto.ExpiresUtc, dto.TotalDurationMinutes, dto.SessionDurationMinutes, dto.CanExtend));
        var row = (await _f.Sessions.GetAsync(id))!;
        Assert.Equal((T0.AddMinutes(20), 1, T0.AddMinutes(5)), (row.ExpiresUtc, row.ExtensionsUsed, row.LastExtendedUtc));
    }

    [Fact]
    public async Task Extend_to_the_cap_reports_cannot_extend()
    {
        await _f.StartedSessionAsync();
        GatewayExtends(30);

        var dto = Assert.IsType<LiveSessionDto>((await _f.Service.ExtendAsync("PUMP-00417", default)).Body.Data);

        Assert.Equal((30, false), (dto.TotalDurationMinutes, dto.CanExtend));
    }

    [Fact]
    public async Task Extend_past_the_cap_is_extension_limit_reached_with_the_module_reason()
    {
        await _f.StartedSessionAsync();
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(400,
            """{"result":"rejected","reason":"Extending by 10 minutes would exceed the 30-minute total session cap."}""");

        var r = await _f.Service.ExtendAsync("PUMP-00417", default);

        Assert.Equal(400, r.StatusCode);
        Assert.Equal(new[] { ErrorCodes.ExtensionLimitReached, "Extending by 10 minutes would exceed the 30-minute total session cap." },
            r.Body.Errors);
    }

    [Fact]
    public async Task Extend_rejected_by_session_mismatch_is_not_active()
    {
        await _f.StartedSessionAsync();
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(409, """{"result":"rejected","reason":"No fast polling session is active."}""");

        var r = await _f.Service.ExtendAsync("PUMP-00417", default);

        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, Code(r)));
    }

    [Fact]
    public async Task Extend_without_a_session_is_not_active()
    {
        var r = await _f.Service.ExtendAsync("PUMP-00417", default);
        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, Code(r)));
    }

    [Fact]
    public async Task Extend_after_lazy_expiry_is_not_active_without_gateway_call()
    {
        await _f.StartedSessionAsync();
        _f.Time.Advance(TimeSpan.FromMinutes(10));            // ExpiresUtc == now: no longer open

        var r = await _f.Service.ExtendAsync("PUMP-00417", default);
        var stop = await _f.Service.StopAsync("PUMP-00417", default);

        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, Code(r)));
        Assert.Equal((409, ErrorCodes.NotActive), (stop.StatusCode, Code(stop)));
        Assert.Empty(_f.Gateway.Calls);
    }

    // ---- stop ----

    [Theory]
    [InlineData(200)]
    [InlineData(409)]
    public async Task Stop_stamps_manual_stop_whether_or_not_the_gateway_was_still_running(int moduleStatus)
    {
        var id = await _f.StartedSessionAsync();
        _f.Time.Advance(TimeSpan.FromMinutes(2));
        _f.Gateway.Respond = _ => FakeDirectMethodClient.Result(moduleStatus, """{"result":"x"}""");

        var r = await _f.Service.StopAsync("PUMP-00417", default);

        Assert.Equal(200, r.StatusCode);
        Assert.Equal("Live session stopped.", r.Body.Message);
        Assert.Null(r.Body.Data);
        Assert.Equal("FastPollingDisable", Assert.Single(_f.Gateway.Calls).MethodName);
        var row = (await _f.Sessions.GetAsync(id))!;
        Assert.Equal((T0.AddMinutes(2), "ManualStop"), (row.StoppedUtc, row.StopReason));
    }

    [Fact]
    public async Task Stop_when_unreachable_leaves_session_open_and_blocks_start()
    {
        var id = await _f.StartedSessionAsync();
        _f.Gateway.Respond = _ => throw new GatewayUnreachableException("timeout");

        var stop = await _f.Service.StopAsync("PUMP-00417", default);
        var start = await _f.Service.StartAsync("PUMP-00417", null, default);

        Assert.Equal((503, ErrorCodes.GatewayUnreachable), (stop.StatusCode, Code(stop)));
        Assert.Null((await _f.Sessions.GetAsync(id))!.StoppedUtc);
        Assert.Equal((409, ErrorCodes.AlreadyActive), (start.StatusCode, Code(start)));

        _f.Time.Advance(TimeSpan.FromMinutes(10));
        _f.GatewayAccepts(Guid.NewGuid());
        Assert.Equal(200, (await _f.Service.StartAsync("PUMP-00417", null, default)).StatusCode);   // free again after expiry
    }

    [Fact]
    public async Task Stop_without_a_session_is_not_active()
    {
        var r = await _f.Service.StopAsync("PUMP-00417", default);
        Assert.Equal((409, ErrorCodes.NotActive), (r.StatusCode, Code(r)));
    }
}
