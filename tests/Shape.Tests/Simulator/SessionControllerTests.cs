using System.Text.Json;
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class SessionControllerTests
{
    private static JsonElement Body(ControlResult r) => JsonSerializer.SerializeToElement(r.Body);

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("{}")]
    [InlineData("not json")]
    public void Enable_with_missing_or_unparseable_body_uses_defaults(string? raw)
    {
        var result = new SessionController().Enable(raw);

        Assert.Equal(200, result.StatusCode);
        var body = Body(result);
        Assert.Equal("accepted", body.GetProperty("result").GetString());
        Assert.Equal(10, body.GetProperty("durationMinutes").GetInt32());
        Assert.Equal(1, body.GetProperty("intervalSeconds").GetInt32());
        Assert.Equal(result.Session!.SessionId.ToString(), body.GetProperty("sessionId").GetString());
        Assert.Equal(new[] { "sessionDurationMinutes", "pollIntervalSeconds" },
            body.GetProperty("defaultsApplied").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void Enable_uses_supplied_duration_case_insensitively()
    {
        var result = new SessionController().Enable("""{"SESSIONDURATIONMINUTES": 15, "pollIntervalSeconds": 1}""");

        Assert.Equal(200, result.StatusCode);
        Assert.Equal(15, result.Session!.DurationMinutes);
        Assert.Empty(Body(result).GetProperty("defaultsApplied").EnumerateArray());
    }

    [Fact]
    public void Enable_rejects_out_of_range_values_listing_every_error()
    {
        var result = new SessionController().Enable("""{"sessionDurationMinutes": 45, "pollIntervalSeconds": 5}""");

        Assert.Equal(400, result.StatusCode);
        Assert.Null(result.Session);
        var body = Body(result);
        Assert.Equal("rejected", body.GetProperty("result").GetString());
        Assert.Equal("Invalid request.", body.GetProperty("reason").GetString());
        var errors = body.GetProperty("errors").EnumerateArray().Select(e => e.GetString()).ToList();
        Assert.Equal(2, errors.Count);
        Assert.Contains("'sessionDurationMinutes'=45 is outside the allowed range (10-30).", errors);
        Assert.Contains("'pollIntervalSeconds'=5 is not supported; only 1 is allowed.", errors);
    }

    [Fact]
    public void Enable_rejects_non_integer_duration()
    {
        var result = new SessionController().Enable("""{"sessionDurationMinutes": "15"}""");

        Assert.Equal(400, result.StatusCode);
        Assert.Contains("'sessionDurationMinutes' must be an integer.",
            Body(result).GetProperty("errors").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void Enable_while_active_is_409_but_validation_runs_first()
    {
        var controller = new SessionController();
        controller.Enable(null);

        var conflict = controller.Enable(null);
        var invalid = controller.Enable("""{"sessionDurationMinutes": 5}""");

        Assert.Equal(409, conflict.StatusCode);
        Assert.Equal("A fast polling session is already active.", Body(conflict).GetProperty("reason").GetString());
        Assert.Equal(400, invalid.StatusCode);
    }

    [Fact]
    public void Disable_when_idle_is_409()
    {
        var result = new SessionController().Disable();

        Assert.Equal(409, result.StatusCode);
        Assert.Equal("No fast polling session is active.", Body(result).GetProperty("reason").GetString());
    }

    [Fact]
    public void Disable_cancels_and_clears_the_active_session()
    {
        var controller = new SessionController();
        var session = controller.Enable(null).Session!;

        var result = controller.Disable();

        Assert.Equal(200, result.StatusCode);
        Assert.Equal(session.SessionId.ToString(), Body(result).GetProperty("sessionId").GetString());
        Assert.True(session.Cts.IsCancellationRequested);
        Assert.Null(controller.Active);
    }

    [Fact]
    public void Enable_after_disable_starts_new_session_immediately()
    {
        var controller = new SessionController();
        var first = controller.Enable(null).Session!;
        controller.Disable();

        var second = controller.Enable(null);

        Assert.Equal(200, second.StatusCode);
        Assert.NotEqual(first.SessionId, second.Session!.SessionId);
    }

    [Fact]
    public void Clear_only_clears_the_matching_session()
    {
        var controller = new SessionController();
        var first = controller.Enable(null).Session!;
        controller.Disable();
        var second = controller.Enable(null).Session!;

        controller.Clear(first);

        Assert.Same(second, controller.Active);
    }

    [Fact]
    public void Status_reports_active_session()
    {
        var controller = new SessionController();
        Assert.False(JsonSerializer.SerializeToElement(controller.Status()).GetProperty("active").GetBoolean());

        var session = controller.Enable(null).Session!;
        var status = JsonSerializer.SerializeToElement(controller.Status());

        Assert.True(status.GetProperty("active").GetBoolean());
        Assert.Equal(session.SessionId.ToString(), status.GetProperty("sessionId").GetString());
        Assert.Equal(10, status.GetProperty("durationMinutes").GetInt32());
    }
}
