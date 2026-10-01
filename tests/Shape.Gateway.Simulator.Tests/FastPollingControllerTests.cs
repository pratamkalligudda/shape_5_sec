using System.Text.Json;
using Microsoft.Extensions.Time.Testing;
using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class FastPollingControllerTests
{
    private readonly FakeTimeProvider _time = new(DateTimeOffset.Parse("2026-10-01T10:00:00Z"));

    private static JsonElement? P(string? json) => json is null ? null : JsonDocument.Parse(json).RootElement.Clone();
    private static JsonElement B(MethodResult r) => JsonSerializer.SerializeToElement(r.Payload);
    private static List<string?> Errors(MethodResult r) =>
        B(r).GetProperty("errors").EnumerateArray().Select(e => e.GetString()).ToList();

    [Theory]
    [InlineData(null)]
    [InlineData("{}")]
    [InlineData("\"not an object\"")]
    public void Enable_without_parameters_uses_defaults(string? payload)
    {
        var r = new FastPollingController(_time).Invoke("FastPollingEnable", P(payload));

        Assert.Equal(200, r.Status);
        var b = B(r);
        Assert.Equal("accepted", b.GetProperty("result").GetString());
        Assert.Equal(10, b.GetProperty("durationMinutes").GetInt32());
        Assert.Equal(1, b.GetProperty("intervalSeconds").GetInt32());
        Assert.True(Guid.TryParse(b.GetProperty("sessionId").GetString(), out _));
        Assert.Equal(new[] { "sessionDurationMinutes", "pollIntervalSeconds" },
            b.GetProperty("defaultsApplied").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public void Enable_uses_supplied_values_case_insensitively()
    {
        var c = new FastPollingController(_time);
        var r = c.Invoke("FastPollingEnable", P("""{"SESSIONDURATIONMINUTES":15,"pollintervalseconds":5}"""));

        Assert.Equal(200, r.Status);
        Assert.Empty(B(r).GetProperty("defaultsApplied").EnumerateArray());
        Assert.Equal(15, c.Active!.TotalDurationMinutes);
        Assert.Equal(5, c.Active.PollIntervalSeconds);
    }

    [Fact]
    public void Enable_rejects_every_out_of_range_or_non_integer_value()
    {
        var r = new FastPollingController(_time).Invoke("FastPollingEnable",
            P("""{"sessionDurationMinutes":45,"pollIntervalSeconds":"x"}"""));

        Assert.Equal(400, r.Status);
        Assert.Equal("Invalid request.", B(r).GetProperty("reason").GetString());
        Assert.Equal(new[]
        {
            "'sessionDurationMinutes'=45 is outside the allowed range (10-30).",
            "'pollIntervalSeconds' must be an integer.",
        }, Errors(r));
    }

    [Fact]
    public void Enable_validates_before_reporting_already_active()
    {
        var c = new FastPollingController(_time);
        c.Invoke("FastPollingEnable", null);

        Assert.Equal(400, c.Invoke("FastPollingEnable", P("""{"pollIntervalSeconds":0}""")).Status);
        var conflict = c.Invoke("FastPollingEnable", null);
        Assert.Equal(409, conflict.Status);
        Assert.Equal("A fast polling session is already active.", B(conflict).GetProperty("reason").GetString());
    }

    [Fact]
    public void Disable_stops_and_clears_or_409s_when_idle()
    {
        var c = new FastPollingController(_time);
        Assert.Equal(409, c.Invoke("FastPollingDisable", null).Status);

        c.Invoke("FastPollingEnable", null);
        var s = c.Active!;
        var r = c.Invoke("FastPollingDisable", null);

        Assert.Equal(200, r.Status);
        Assert.Equal(s.SessionId.ToString(), B(r).GetProperty("sessionId").GetString());
        Assert.True(s.Stopped.IsCancellationRequested);
        Assert.Null(c.Active);
    }

    [Fact]
    public void Session_expires_after_its_duration()
    {
        var c = new FastPollingController(_time);
        c.Invoke("FastPollingEnable", P("""{"sessionDurationMinutes":10}"""));
        var s = c.Active!;

        _time.Advance(TimeSpan.FromMinutes(10) - TimeSpan.FromSeconds(1));
        Assert.False(s.Expired.IsCancellationRequested);
        _time.Advance(TimeSpan.FromSeconds(1));
        Assert.True(s.Expired.IsCancellationRequested);
    }

    [Fact]
    public void Extend_measures_total_from_original_start()
    {
        var c = new FastPollingController(_time);
        c.Invoke("FastPollingEnable", P("""{"sessionDurationMinutes":10}"""));
        var s = c.Active!;
        _time.Advance(TimeSpan.FromMinutes(5));

        var r = c.Invoke("Extend-FastPolling",
            P($$"""{"sessionId":"{{s.SessionId}}","extendSessionDurationMinutes":10}"""));

        Assert.Equal(200, r.Status);
        Assert.Equal(10, B(r).GetProperty("extendedByMinutes").GetInt32());
        Assert.Equal(20, B(r).GetProperty("totalDurationMinutes").GetInt32());
        _time.Advance(TimeSpan.FromMinutes(15) - TimeSpan.FromSeconds(1)); // T = 19:59
        Assert.False(s.Expired.IsCancellationRequested);
        _time.Advance(TimeSpan.FromSeconds(1));                              // T = 20:00
        Assert.True(s.Expired.IsCancellationRequested);
    }

    [Fact]
    public void Extend_checks_fields_then_session_then_cap()
    {
        var c = new FastPollingController(_time);
        Assert.Equal(400, c.Invoke("Extend-FastPolling", P("""{"sessionId":"nope"}""")).Status);
        Assert.Equal(409, c.Invoke("Extend-FastPolling",
            P($$"""{"sessionId":"{{Guid.NewGuid()}}","extendSessionDurationMinutes":5}""")).Status);

        c.Invoke("FastPollingEnable", P("""{"sessionDurationMinutes":25}"""));
        var id = c.Active!.SessionId;
        var mismatch = c.Invoke("Extend-FastPolling",
            P($$"""{"sessionId":"{{Guid.NewGuid()}}","extendSessionDurationMinutes":5}"""));
        Assert.Equal(409, mismatch.Status);
        Assert.EndsWith("does not match the currently active session.", B(mismatch).GetProperty("reason").GetString());

        var cap = c.Invoke("Extend-FastPolling", P($$"""{"sessionId":"{{id}}","extendSessionDurationMinutes":10}"""));
        Assert.Equal(400, cap.Status);
        Assert.Equal(
            "Extending by 10 min would bring the session's total duration to 35 min, exceeding the 30 min cap (currently 25 min).",
            B(cap).GetProperty("reason").GetString());
        Assert.Equal(25, c.Active!.TotalDurationMinutes);
    }

    [Fact]
    public void Extend_field_errors_list_both_fields()
    {
        var r = new FastPollingController(_time).Invoke("Extend-FastPolling", P("{}"));
        Assert.Equal(new[]
        {
            "'sessionId' is required and must be a GUID.",
            "'extendSessionDurationMinutes' is required and must be a positive integer.",
        }, Errors(r));
    }

    [Fact]
    public void Unknown_method_is_501()
    {
        Assert.Equal(501, new FastPollingController(_time).Invoke("Reboot", null).Status);
    }

    [Fact]
    public void SessionStarted_fires_on_successful_enable_only()
    {
        var c = new FastPollingController(_time);
        var started = new List<Guid>();
        c.SessionStarted += s => started.Add(s.SessionId);

        c.Invoke("FastPollingEnable", null);
        c.Invoke("FastPollingEnable", null);

        Assert.Single(started);
    }

    [Fact]
    public void Clear_only_clears_the_matching_session()
    {
        var c = new FastPollingController(_time);
        c.Invoke("FastPollingEnable", null);
        var first = c.Active!;
        c.Invoke("FastPollingDisable", null);
        c.Invoke("FastPollingEnable", null);
        var second = c.Active!;

        c.Clear(first);

        Assert.Same(second, c.Active);
    }
}
