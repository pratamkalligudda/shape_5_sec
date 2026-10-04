using System.Text.Json;
using Microsoft.Extensions.Options;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Options;

namespace Shape.LiveBackend.Sessions;

/// <summary>
/// Spec §7.2. Every "now" comes from the injected TimeProvider. An open session is
/// StoppedUtc IS NULL AND ExpiresUtc > now — expiry is lazy, nothing marks a row expired.
/// </summary>
public sealed class LiveSessionService
{
    public const string EnableMethod = "FastPollingEnable";
    public const string DisableMethod = "FastPollingDisable";
    public const string ExtendMethod = "Extend-FastPolling";
    public const string StopReasonManual = "ManualStop";
    public const string LocalUser = "local";

    private readonly IDeviceDirectory _devices;
    private readonly ILivePollingSessionRepository _sessions;
    private readonly ILiveSessionReader _live;
    private readonly IDirectMethodClient _gateway;
    private readonly LivePollingOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger<LiveSessionService> _logger;

    public LiveSessionService(IDeviceDirectory devices, ILivePollingSessionRepository sessions, ILiveSessionReader live,
        IDirectMethodClient gateway, IOptions<LivePollingOptions> options, TimeProvider time, ILogger<LiveSessionService> logger)
    {
        _devices = devices;
        _sessions = sessions;
        _live = live;
        _gateway = gateway;
        _options = options.Value;
        _time = time;
        _logger = logger;
    }

    public async Task<ServiceResult> StartAsync(string serial, StartLiveSessionRequest? request, CancellationToken ct)
    {
        if (LookupDevice(_devices, serial, out var device) is { } notFound) return notFound;

        var errors = new List<string>();
        if (request?.PollIntervalSeconds is { } i && (i < 1 || i > 60)) errors.Add("pollIntervalSeconds must be between 1 and 60.");
        if (request?.SessionDurationMinutes is { } d && (d < 10 || d > 30)) errors.Add("sessionDurationMinutes must be between 10 and 30.");
        if (errors.Count > 0) return ServiceResult.Fail(400, ErrorCodes.InvalidParameters, "Invalid live session parameters.", errors);

        var now = _time.GetUtcNow();
        if (await _sessions.GetOpenForDeviceAsync(device.DeviceGuid, now, ct) is { } open)
            return ServiceResult.Fail(409, ErrorCodes.AlreadyActive,
                $"A live session is already active on this device until {SessionSchema.Ts(open.ExpiresUtc)}.");
        if (await _sessions.CountOpenAsync(now, ct) >= _options.MaxConcurrentSessions)
            return ServiceResult.Fail(409, ErrorCodes.CapacityReached, "The maximum number of concurrent live sessions has been reached.");

        var payload = new
        {
            sessionDurationMinutes = request?.SessionDurationMinutes ?? _options.SessionDurationMinutes,
            pollIntervalSeconds = request?.PollIntervalSeconds ?? _options.PollIntervalSeconds,
        };
        DirectMethodResult result;
        try
        {
            result = await InvokeAsync(device, EnableMethod, payload, ct);
        }
        catch (GatewayUnreachableException ex)
        {
            return Unreachable(ex);
        }

        switch (result.Status)
        {
            case 200:
                var p = result.Payload!.Value;
                var duration = p.GetProperty("durationMinutes").GetInt32();
                var session = new LivePollingSessionRecord
                {
                    SessionId = p.GetProperty("sessionId").GetGuid(),
                    DeviceGuid = device.DeviceGuid,
                    DeviceId = device.SerialNumber,
                    GatewayId = device.GatewayId!,
                    StartedUtc = now,
                    StartedByUserId = LocalUser,
                    PollIntervalSeconds = p.GetProperty("intervalSeconds").GetInt32(),
                    SessionDurationMinutes = duration,
                    ExpiresUtc = now.AddMinutes(duration),
                };
                await _sessions.InsertAsync(session, ct);
                _logger.LogInformation("Live session {SessionId} started on {Serial} until {Expires}", session.SessionId, serial, session.ExpiresUtc);
                return ServiceResult.Ok(ToDto(session));
            case 409:
                return ServiceResult.Fail(409, ErrorCodes.AlreadyActive, "The gateway reports a live session is already active.");
            case 400:
                return ServiceResult.Fail(400, ErrorCodes.InvalidParameters, "The gateway rejected the live session parameters.",
                    ModuleErrors(result.Payload));
            default:
                return Unreachable(new GatewayUnreachableException($"{EnableMethod} returned module status {result.Status}."));
        }
    }

    public async Task<ServiceResult> GetStatusAsync(string serial, CancellationToken ct)
    {
        if (LookupDevice(_devices, serial, out var device) is { } notFound) return notFound;
        var now = _time.GetUtcNow();
        var open = await _sessions.GetOpenForDeviceAsync(device.DeviceGuid, now, ct);
        if (open is null) return ServiceResult.Ok(LiveSessionStatusDto.Inactive);

        LiveHeadView? head;
        try
        {
            head = await _live.ReadHeadAsync(open.SessionId, ct);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogError(ex, "Reading the live head of session {SessionId} failed", open.SessionId);
            return new ServiceResult(500, new ApiResponse { Succeeded = false, Message = "Live session data could not be read." });
        }

        var ticked = head is { TickCount: > 0 };
        return ServiceResult.Ok(new LiveSessionStatusDto(
            true, open.SessionId, open.PollIntervalSeconds, open.ExpiresUtc,
            Math.Max(0, (int)Math.Floor((open.ExpiresUtc - now).TotalSeconds)),
            open.TotalDurationMinutes, open.CanExtend,
            ticked ? head!.DeviceStatus : null,
            ticked ? head!.Snapshot : null,
            ticked ? head!.LastSnapshotTimestampUtc : null,
            ticked ? head!.TickCount : null,
            ticked ? head!.BytesTotal : null));
    }

    public async Task<ServiceResult> ExtendAsync(string serial, CancellationToken ct)
    {
        if (LookupDevice(_devices, serial, out var device) is { } notFound) return notFound;
        var now = _time.GetUtcNow();
        var open = await _sessions.GetOpenForDeviceAsync(device.DeviceGuid, now, ct);
        if (open is null) return NotActive();

        DirectMethodResult result;
        try
        {
            result = await InvokeAsync(device, ExtendMethod,
                new { sessionId = open.SessionId, extendSessionDurationMinutes = _options.ExtensionMinutes }, ct);
        }
        catch (GatewayUnreachableException ex)
        {
            return Unreachable(ex);
        }

        switch (result.Status)
        {
            case 200:
                var total = result.Payload!.Value.GetProperty("totalDurationMinutes").GetInt32();
                var extended = open with { ExpiresUtc = open.StartedUtc.AddMinutes(total), ExtensionsUsed = open.ExtensionsUsed + 1, LastExtendedUtc = now };
                await _sessions.UpdateExpiryAsync(open.SessionId, extended.ExpiresUtc, now, ct);
                return ServiceResult.Ok(ToDto(extended));
            case 400 when HasFieldErrors(result.Payload):
                return ServiceResult.Fail(400, ErrorCodes.InvalidParameters, "The gateway rejected the extension request.",
                    ModuleErrors(result.Payload));
            case 400:
                return ServiceResult.Fail(400, ErrorCodes.ExtensionLimitReached,
                    $"The session cannot be extended beyond {LivePollingOptions.MaxTotalDurationMinutes} minutes.", ModuleErrors(result.Payload));
            case 409:
                return ServiceResult.Fail(409, ErrorCodes.NotActive, "The gateway's live session no longer matches this session.");
            default:
                return Unreachable(new GatewayUnreachableException($"{ExtendMethod} returned module status {result.Status}."));
        }
    }

    public async Task<ServiceResult> StopAsync(string serial, CancellationToken ct)
    {
        if (LookupDevice(_devices, serial, out var device) is { } notFound) return notFound;
        var now = _time.GetUtcNow();
        var open = await _sessions.GetOpenForDeviceAsync(device.DeviceGuid, now, ct);
        if (open is null) return NotActive();

        DirectMethodResult result;
        try
        {
            result = await InvokeAsync(device, DisableMethod, null, ct);
        }
        catch (GatewayUnreachableException ex)
        {
            return Unreachable(ex);
        }
        if (result.Status is not (200 or 409))
            return Unreachable(new GatewayUnreachableException($"{DisableMethod} returned module status {result.Status}."));

        await _sessions.MarkStoppedAsync(open.SessionId, now, StopReasonManual, ct);
        _logger.LogInformation("Live session {SessionId} stopped (module status {Status})", open.SessionId, result.Status);
        return ServiceResult.Ok(null, "Live session stopped.");
    }

    internal static ServiceResult? LookupDevice(IDeviceDirectory devices, string serial, out DeviceConfig device)
    {
        device = devices.FindBySerial(serial)!;
        if (device is null) return ServiceResult.Fail(404, ErrorCodes.DeviceNotFound, $"Device '{serial}' was not found.");
        if (string.IsNullOrWhiteSpace(device.GatewayId))
            return ServiceResult.Fail(404, ErrorCodes.GatewayNotPaired, $"Device '{serial}' has no active gateway pairing.");
        return null;
    }

    internal static ServiceResult NotActive() =>
        ServiceResult.Fail(409, ErrorCodes.NotActive, "No live session is active on this device.");

    private Task<DirectMethodResult> InvokeAsync(DeviceConfig device, string method, object? payload, CancellationToken ct) =>
        _gateway.InvokeAsync(device.GatewayId!, _options.ModuleId ?? device.ModuleId, method, payload,
            TimeSpan.FromSeconds(_options.DirectMethodTimeoutSeconds), ct);

    private ServiceResult Unreachable(GatewayUnreachableException ex)
    {
        _logger.LogWarning(ex, "Gateway unreachable");
        return ServiceResult.Fail(503, ErrorCodes.GatewayUnreachable, "The gateway did not respond.");
    }

    private static LiveSessionDto ToDto(LivePollingSessionRecord s) => new(
        s.SessionId, s.DeviceId, s.PollIntervalSeconds, s.TotalDurationMinutes, s.StartedUtc, s.ExpiresUtc, s.TotalDurationMinutes, s.CanExtend);

    private static bool HasFieldErrors(JsonElement? payload) =>
        payload is { ValueKind: JsonValueKind.Object } p && p.TryGetProperty("errors", out var e) && e.ValueKind == JsonValueKind.Array;

    private static List<string> ModuleErrors(JsonElement? payload)
    {
        if (payload is not { ValueKind: JsonValueKind.Object } p) return [];
        if (HasFieldErrors(p)) return p.GetProperty("errors").EnumerateArray().Select(e => e.GetString() ?? e.GetRawText()).ToList();
        return p.TryGetProperty("reason", out var reason) && reason.ValueKind == JsonValueKind.String ? [reason.GetString()!] : [];
    }
}
