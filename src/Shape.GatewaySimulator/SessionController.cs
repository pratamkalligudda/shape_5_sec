using System.Text.Json;

namespace Shape.GatewaySimulator;

public sealed record FastPollingSession(Guid SessionId, int DurationMinutes, DateTimeOffset StartedUtc, CancellationTokenSource Cts);

public sealed record ControlResult(int StatusCode, object Body, FastPollingSession? Session = null);

/// <summary>
/// Mirrors the FastPollingEnable/FastPollingDisable direct methods (spec §5.2). Only one session at a time.
/// Disable clears the slot immediately so a new session can start while the old runner is still flushing.
/// </summary>
public sealed class SessionController
{
    public const int DefaultDurationMinutes = 10;
    public const int MinDurationMinutes = 10;
    public const int MaxDurationMinutes = 30;
    public const int IntervalSeconds = 1;

    private readonly TimeProvider _time;
    private FastPollingSession? _active;

    public SessionController(TimeProvider? time = null) => _time = time ?? TimeProvider.System;

    public FastPollingSession? Active => Volatile.Read(ref _active);

    public ControlResult Enable(string? rawBody)
    {
        var (duration, defaultsApplied, errors) = Validate(rawBody);
        if (errors.Count > 0)
            return new(400, new { result = "rejected", reason = "Invalid request.", errors });

        var session = new FastPollingSession(Guid.NewGuid(), duration, _time.GetUtcNow(), new CancellationTokenSource());
        if (Interlocked.CompareExchange(ref _active, session, null) is not null)
        {
            session.Cts.Dispose();
            return new(409, new { result = "rejected", reason = "A fast polling session is already active." });
        }

        return new(200, new
        {
            result = "accepted",
            durationMinutes = duration,
            intervalSeconds = IntervalSeconds,
            sessionId = session.SessionId,
            defaultsApplied,
        }, session);
    }

    public ControlResult Disable()
    {
        var session = Volatile.Read(ref _active);
        if (session is null || Interlocked.CompareExchange(ref _active, null, session) != session)
            return new(409, new { result = "rejected", reason = "No fast polling session is active." });

        session.Cts.Cancel();
        return new(200, new { result = "accepted", sessionId = session.SessionId });
    }

    public void Clear(FastPollingSession session) => Interlocked.CompareExchange(ref _active, null, session);

    public object Status() => Active is { } s
        ? new { active = true, sessionId = s.SessionId, startedUtc = s.StartedUtc, durationMinutes = s.DurationMinutes }
        : new { active = false };

    private static (int Duration, List<string> DefaultsApplied, List<string> Errors) Validate(string? raw)
    {
        var errors = new List<string>();
        int? duration = null, interval = null;
        bool durationSupplied = false, intervalSupplied = false;

        if (!string.IsNullOrWhiteSpace(raw))
        {
            try
            {
                using var doc = JsonDocument.Parse(raw);
                if (doc.RootElement.ValueKind == JsonValueKind.Object)
                {
                    foreach (var p in doc.RootElement.EnumerateObject())
                    {
                        if (p.Name.Equals("sessionDurationMinutes", StringComparison.OrdinalIgnoreCase))
                        {
                            durationSupplied = true;
                            duration = ReadInt(p, errors);
                        }
                        else if (p.Name.Equals("pollIntervalSeconds", StringComparison.OrdinalIgnoreCase))
                        {
                            intervalSupplied = true;
                            interval = ReadInt(p, errors);
                        }
                    }
                }
            }
            catch (JsonException)
            {
                // Unparseable body is treated as "no parameters supplied" (same as the direct-method contract).
            }
        }

        if (duration is { } d && (d < MinDurationMinutes || d > MaxDurationMinutes))
            errors.Add($"'sessionDurationMinutes'={d} is outside the allowed range ({MinDurationMinutes}-{MaxDurationMinutes}).");
        if (interval is { } i && i != IntervalSeconds)
            errors.Add($"'pollIntervalSeconds'={i} is not supported; only {IntervalSeconds} is allowed.");

        var defaultsApplied = new List<string>();
        if (!durationSupplied) defaultsApplied.Add("sessionDurationMinutes");
        if (!intervalSupplied) defaultsApplied.Add("pollIntervalSeconds");

        return (duration ?? DefaultDurationMinutes, defaultsApplied, errors);
    }

    private static int? ReadInt(JsonProperty p, List<string> errors)
    {
        if (p.Value.ValueKind == JsonValueKind.Number && p.Value.TryGetInt32(out var value)) return value;
        errors.Add($"'{p.Name}' must be an integer.");
        return null;
    }
}
