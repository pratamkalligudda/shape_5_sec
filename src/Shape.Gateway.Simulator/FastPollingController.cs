using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>FastPollingEnable / FastPollingDisable / Extend-FastPolling exactly as the module doc (v3).</summary>
public sealed class FastPollingController(TimeProvider time)
{
    public const int MinDuration = 10, MaxDuration = 30, DefaultDuration = 10;
    public const int MinInterval = 1, MaxInterval = 60, DefaultInterval = 1;
    public const int MaxTotalMinutes = 30;

    private FastPollingSession? _active;

    public FastPollingSession? Active => Volatile.Read(ref _active);

    public event Action<FastPollingSession>? SessionStarted;

    public MethodResult Invoke(string methodName, JsonElement? payload) => methodName switch
    {
        "FastPollingEnable" => Enable(payload),
        "FastPollingDisable" => Disable(),
        "Extend-FastPolling" => Extend(payload),
        _ => new MethodResult(501, new { result = "rejected", reason = $"Method '{methodName}' is not implemented." }),
    };

    public void Clear(FastPollingSession session) => Interlocked.CompareExchange(ref _active, null, session);

    private MethodResult Enable(JsonElement? payload)
    {
        var errors = new List<string>();
        var duration = ReadOptionalInt(payload, "sessionDurationMinutes", MinDuration, MaxDuration, errors, out var durationSupplied);
        var interval = ReadOptionalInt(payload, "pollIntervalSeconds", MinInterval, MaxInterval, errors, out var intervalSupplied);
        if (errors.Count > 0) return Rejected400(errors);

        var session = new FastPollingSession(Guid.NewGuid(), interval ?? DefaultInterval, duration ?? DefaultDuration, time);
        if (Interlocked.CompareExchange(ref _active, session, null) is not null)
            return new MethodResult(409, new { result = "rejected", reason = "A fast polling session is already active." });

        var defaults = new List<string>();
        if (!durationSupplied) defaults.Add("sessionDurationMinutes");
        if (!intervalSupplied) defaults.Add("pollIntervalSeconds");
        SessionStarted?.Invoke(session);
        return new MethodResult(200, new
        {
            result = "accepted",
            durationMinutes = session.TotalDurationMinutes,
            intervalSeconds = session.PollIntervalSeconds,
            sessionId = session.SessionId,
            defaultsApplied = defaults,
        });
    }

    private MethodResult Disable()
    {
        var session = Volatile.Read(ref _active);
        if (session is null || Interlocked.CompareExchange(ref _active, null, session) != session)
            return new MethodResult(409, new { result = "rejected", reason = "No fast polling session is active." });
        session.Stop();
        return new MethodResult(200, new { result = "accepted", sessionId = session.SessionId });
    }

    private MethodResult Extend(JsonElement? payload)
    {
        Guid? sessionId = null;
        int? minutes = null;
        if (payload is { ValueKind: JsonValueKind.Object } obj)
        {
            foreach (var p in obj.EnumerateObject())
            {
                if (p.NameEquals("sessionId") && p.Value.ValueKind == JsonValueKind.String &&
                    Guid.TryParse(p.Value.GetString(), out var g))
                    sessionId = g;
                if (p.NameEquals("extendSessionDurationMinutes") && p.Value.ValueKind == JsonValueKind.Number &&
                    p.Value.TryGetInt32(out var m) && m > 0)
                    minutes = m;
            }
        }
        var errors = new List<string>();
        if (sessionId is null) errors.Add("'sessionId' is required and must be a GUID.");
        if (minutes is null) errors.Add("'extendSessionDurationMinutes' is required and must be a positive integer.");
        if (errors.Count > 0) return Rejected400(errors);

        var session = Active;
        if (session is null)
            return new MethodResult(409, new { result = "rejected", reason = "No fast polling session is active." });
        if (session.SessionId != sessionId)
            return new MethodResult(409, new
            {
                result = "rejected",
                reason = $"Session '{sessionId}' does not match the currently active session.",
            });

        var capReason = session.TryExtend(minutes!.Value, MaxTotalMinutes);
        if (capReason is not null)
            return new MethodResult(400, new { result = "rejected", reason = capReason });

        return new MethodResult(200, new
        {
            result = "accepted",
            sessionId = session.SessionId,
            extendedByMinutes = minutes,
            totalDurationMinutes = session.TotalDurationMinutes,
        });
    }

    private static MethodResult Rejected400(List<string> errors) =>
        new(400, new { result = "rejected", reason = "Invalid request.", errors });

    private static int? ReadOptionalInt(JsonElement? payload, string name, int min, int max, List<string> errors, out bool supplied)
    {
        supplied = false;
        if (payload is not { ValueKind: JsonValueKind.Object } obj) return null;
        foreach (var p in obj.EnumerateObject())
        {
            if (!p.Name.Equals(name, StringComparison.OrdinalIgnoreCase)) continue;
            supplied = true;
            if (p.Value.ValueKind != JsonValueKind.Number || !p.Value.TryGetInt32(out var v))
            {
                errors.Add($"'{name}' must be an integer.");
                return null;
            }
            if (v < min || v > max)
            {
                errors.Add($"'{name}'={v} is outside the allowed range ({min}-{max}).");
                return null;
            }
            return v;
        }
        return null;
    }
}
