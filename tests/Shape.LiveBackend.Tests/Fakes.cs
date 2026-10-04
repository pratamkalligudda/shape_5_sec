using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Options;
using Shape.LiveBackend.Sessions;

namespace Shape.LiveBackend.Tests;

public sealed record MethodCall(string DeviceId, string ModuleId, string MethodName, JsonElement Payload);

public sealed class FakeDirectMethodClient : IDirectMethodClient
{
    public List<MethodCall> Calls { get; } = [];
    public Func<MethodCall, DirectMethodResult> Respond { get; set; } = _ => throw new InvalidOperationException("no response set");

    public Task<DirectMethodResult> InvokeAsync(string deviceId, string moduleId, string methodName, object? payload, TimeSpan timeout,
        CancellationToken ct = default)
    {
        var call = new MethodCall(deviceId, moduleId, methodName, JsonSerializer.SerializeToElement(payload));
        Calls.Add(call);
        return Task.FromResult(Respond(call));
    }

    public static DirectMethodResult Result(int status, string payloadJson) => new(status, JsonDocument.Parse(payloadJson).RootElement.Clone());
}

public sealed class FakeLiveSessionReader : ILiveSessionReader
{
    public Dictionary<Guid, LiveHeadView> Heads { get; } = new();
    public Dictionary<(Guid, string), List<ChangePoint>> Points { get; } = new();
    public Dictionary<Guid, List<LiveEventDto>> Events { get; } = new();
    public Exception? Throw { get; set; }
    public List<string> ParameterReads { get; } = [];

    public Task<LiveHeadView?> ReadHeadAsync(Guid sessionId, CancellationToken ct = default) =>
        Throw is not null ? Task.FromException<LiveHeadView?>(Throw) : Task.FromResult(Heads.GetValueOrDefault(sessionId));

    public Task<IReadOnlyList<ChangePoint>> ReadParameterAsync(Guid sessionId, string parameter, CancellationToken ct = default)
    {
        lock (ParameterReads) ParameterReads.Add(parameter);
        return Task.FromResult<IReadOnlyList<ChangePoint>>(Points.GetValueOrDefault((sessionId, parameter)) ?? []);
    }

    public Task<IReadOnlyList<LiveEventDto>> ReadEventsAsync(Guid sessionId, CancellationToken ct = default) =>
        Task.FromResult<IReadOnlyList<LiveEventDto>>(Events.GetValueOrDefault(sessionId) ?? []);
}

/// <summary>Real SQLite repository on a temp file, fake gateway, fake Table reader, fake clock at T0.</summary>
public sealed class ServiceFixture : IDisposable
{
    public static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    public static readonly DeviceConfig Device =
        new("PUMP-00417", Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), "PREC-NEO30-MARYL-07", "gw-001", "DefaultModeModule");

    private readonly TempDb _db = new();
    public FakeTimeProvider Time { get; } = new(T0);
    public FakeDirectMethodClient Gateway { get; } = new();
    public FakeLiveSessionReader Reader { get; } = new();
    public SqliteLivePollingSessionRepository Sessions { get; }
    public LivePollingOptions Options { get; } = new()
    {
        DefaultSeriesMetrics = ["OperatingPressurePSI", "HydraulicOilTemperatureF", "StrokeRateLeft", "StrokeRateRight"],
    };
    public LiveSessionService Service { get; }

    public ServiceFixture(DeviceConfig? device = null)
    {
        Sessions = new SqliteLivePollingSessionRepository(_db.Path);
        Sessions.EnsureSchemaAsync().GetAwaiter().GetResult();
        Service = new LiveSessionService(new ConfigDeviceDirectory(device ?? Device), Sessions, Reader, Gateway,
            Microsoft.Extensions.Options.Options.Create(Options), Time, NullLogger<LiveSessionService>.Instance);
    }

    /// <summary>Gateway answers FastPollingEnable like the module: accepted with the requested values.</summary>
    public void GatewayAccepts(Guid sessionId) => Gateway.Respond = call => call.MethodName switch
    {
        LiveSessionService.EnableMethod => FakeDirectMethodClient.Result(200,
            $$"""{"result":"accepted","durationMinutes":{{call.Payload.GetProperty("sessionDurationMinutes")}},"intervalSeconds":{{call.Payload.GetProperty("pollIntervalSeconds")}},"sessionId":"{{sessionId}}","defaultsApplied":[]}"""),
        LiveSessionService.DisableMethod => FakeDirectMethodClient.Result(200, $$"""{"result":"accepted","sessionId":"{{sessionId}}"}"""),
        _ => throw new InvalidOperationException(call.MethodName),
    };

    public async Task<Guid> StartedSessionAsync()
    {
        var id = Guid.NewGuid();
        GatewayAccepts(id);
        var r = await Service.StartAsync("PUMP-00417", null, default);
        Assert.Equal(200, r.StatusCode);
        Gateway.Calls.Clear();
        return id;
    }

    public void Dispose() => _db.Dispose();
}

public static class Json
{
    public static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web)
    {
        Converters = { new Shape.LiveBackend.Infrastructure.UtcDateTimeOffsetConverter() },
    };

    public static JsonElement Of(ServiceResult r) => JsonSerializer.SerializeToElement(r.Body, Web);
}
