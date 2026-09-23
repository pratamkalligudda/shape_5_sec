using Microsoft.Extensions.Options;
using Shape.Contracts;
using Shape.GatewaySimulator;

var options = SimulatorOptions.Parse(args);

// Content root = app dir so appsettings.json loads regardless of the working directory.
var builder = WebApplication.CreateBuilder(new WebApplicationOptions { ContentRootPath = AppContext.BaseDirectory });
builder.WebHost.UseUrls(options.Urls);
builder.Services.AddCors(o => o.AddDefaultPolicy(p =>
    p.WithOrigins("http://localhost:5173").AllowAnyHeader().AllowAnyMethod()));
builder.Services.AddHttpClient(Options.DefaultName, c => c.Timeout = TimeSpan.FromSeconds(30));
builder.Services.AddSingleton(options);
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<SessionController>();
builder.Services.AddSingleton<IBatchPublisher>(sp => new EventGridPublisher(
    sp.GetRequiredService<IHttpClientFactory>().CreateClient(),
    options.FunctionsUrl,
    EventGridPublisher.DefaultRetryDelays,
    sp.GetRequiredService<ILogger<EventGridPublisher>>()));
var seed = PumpSeed.LoadDefault();
builder.Services.AddSingleton<Func<PumpDevice>>(() => new PumpDevice(seed, options.RandomSeed));
builder.Services.AddSingleton<SessionRunner>();

var app = builder.Build();
app.UseCors();

app.MapPost("/fast-polling/enable", async (HttpRequest request, SessionController controller, SessionRunner runner,
    IHostApplicationLifetime lifetime) =>
{
    using var reader = new StreamReader(request.Body);
    var result = controller.Enable(await reader.ReadToEndAsync());
    if (result.Session is { } session)
        _ = Task.Run(() => runner.RunAsync(session, lifetime.ApplicationStopping));
    return Results.Json(result.Body, statusCode: result.StatusCode);
});

app.MapPost("/fast-polling/disable", (SessionController controller) =>
{
    var result = controller.Disable();
    return Results.Json(result.Body, statusCode: result.StatusCode);
});

app.MapGet("/fast-polling/status", (SessionController controller) => Results.Json(controller.Status()));

app.Run();
