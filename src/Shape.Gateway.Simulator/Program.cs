using System.Text.Json;
using Shape.Gateway.Simulator;

var options = SimulatorOptions.Parse(args);

var builder = WebApplication.CreateBuilder(new WebApplicationOptions { ContentRootPath = AppContext.BaseDirectory });
builder.WebHost.UseUrls(options.Urls);
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.WithOrigins("http://localhost:5173").AllowAnyHeader().AllowAnyMethod()));
builder.Services.AddHttpClient(Microsoft.Extensions.Options.Options.DefaultName, c => c.Timeout = TimeSpan.FromSeconds(30));
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton(DeviceConfig.Load(DeviceConfig.DefaultPath));
builder.Services.AddSingleton<EventState>();
builder.Services.AddSingleton<FastPollingController>();
builder.Services.AddSingleton<IBatchPublisher>(sp => new EventGridPublisher(
    sp.GetRequiredService<IHttpClientFactory>().CreateClient(), options.FunctionsUrl, sp.GetRequiredService<DeviceConfig>(),
    EventGridPublisher.DefaultRetryDelays, sp.GetRequiredService<ILogger<EventGridPublisher>>()));
var seed = PumpSeed.Load(PumpSeed.DefaultPath);
builder.Services.AddSingleton<Func<PumpDevice>>(() => new PumpDevice(seed, options.RandomSeed));
builder.Services.AddSingleton<SessionRunner>();

var app = builder.Build();
app.UseCors();

var controller = app.Services.GetRequiredService<FastPollingController>();
var runner = app.Services.GetRequiredService<SessionRunner>();
var lifetime = app.Services.GetRequiredService<IHostApplicationLifetime>();
controller.SessionStarted += session => _ = Task.Run(() => runner.RunAsync(session, lifetime.ApplicationStopping));

// IoT Hub REST "invoke module method" facade: the backend's direct-method client talks to this.
app.MapPost("/twins/{deviceId}/modules/{moduleId}/methods",
    async (string deviceId, string moduleId, MethodInvocation body, DeviceConfig device, CancellationToken ct) =>
    {
        if (deviceId != device.GatewayId || moduleId != device.ModuleId)
            return Results.Json(new { message = $"Device '{deviceId}' module '{moduleId}' not found." }, statusCode: 404);
        if (options.Unreachable)
        {
            await Task.Delay(TimeSpan.FromSeconds((body.ResponseTimeoutInSeconds ?? 30) + 1), ct);
            return Results.Json(new { message = "Timed out waiting for the device to respond." }, statusCode: 504);
        }
        var result = controller.Invoke(body.MethodName, body.Payload);
        return Results.Json(new { status = result.Status, payload = result.Payload });
    });

app.MapPost("/sim/events", (Dictionary<string, int> updates, EventState events) =>
    events.TrySet(updates, out var unknown)
        ? Results.Json(events.Snapshot())
        : Results.Json(new { message = "Unknown event parameters.", unknown }, statusCode: 400));

app.MapPost("/sim/events/clear", (EventState events) =>
{
    events.Reset();
    return Results.Json(events.Snapshot());
});

app.MapGet("/sim/state", (EventState events) =>
{
    var s = controller.Active;
    return Results.Json(new
    {
        active = s is not null,
        sessionId = s?.SessionId,
        totalDurationMinutes = s?.TotalDurationMinutes,
        pollIntervalSeconds = s?.PollIntervalSeconds,
        events = events.Snapshot(),
    });
});

app.Run();

public sealed record MethodInvocation(string MethodName, JsonElement? Payload, int? ResponseTimeoutInSeconds);

public partial class Program;
