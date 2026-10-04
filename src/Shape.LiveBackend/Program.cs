using Azure.Data.Tables;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.Gateway;
using Shape.LiveBackend.Infrastructure;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Options;
using Shape.LiveBackend.Series;
using Shape.LiveBackend.Sessions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<LivePollingOptions>(builder.Configuration.GetSection(LivePollingOptions.Section));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IDeviceDirectory>(_ => new ConfigDeviceDirectory(DeviceConfig.Load(DeviceConfig.DefaultPath)));
builder.Services.AddSingleton<ILivePollingSessionRepository>(sp =>
    new SqliteLivePollingSessionRepository(RepoPaths.Resolve(sp.GetRequiredService<IOptions<LivePollingOptions>>().Value.SessionDbPath)));
builder.Services.AddHttpClient<IDirectMethodClient, HttpDirectMethodClient>((sp, http) =>
{
    http.BaseAddress = new Uri(sp.GetRequiredService<IOptions<LivePollingOptions>>().Value.IoTHubUrl);
    http.Timeout = Timeout.InfiniteTimeSpan;   // HttpDirectMethodClient applies responseTimeout + grace itself
});
builder.Services.AddSingleton(sp => new TableServiceClient(sp.GetRequiredService<IOptions<LivePollingOptions>>().Value.TableConnectionString));
builder.Services.AddSingleton<ILiveSessionReader>(sp => new TableLiveSessionReader(sp.GetRequiredService<TableServiceClient>()));
builder.Services.AddScoped<LiveSessionService>();
builder.Services.AddSingleton(_ => MetricRegistry.Load(MetricRegistry.DefaultSeedPath));
builder.Services.AddScoped<LiveSeriesService>();
builder.Services.AddHostedService<SessionSchemaInitializer>();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p
    .WithOrigins(builder.Configuration["Cors:Origin"] ?? "http://localhost:5173").AllowAnyHeader().AllowAnyMethod().AllowCredentials()));
builder.Services.AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.Converters.Add(new UtcDateTimeOffsetConverter()))
    .ConfigureApiBehaviorOptions(o => o.InvalidModelStateResponseFactory = context =>
        new BadRequestObjectResult(ApiResponse.Fail(ErrorCodes.InvalidParameters, "Invalid live session parameters.",
            context.ModelState
                .Where(kv => kv.Value is { Errors.Count: > 0 })
                .Select(kv => $"{kv.Key.TrimStart('$', '.')}: {kv.Value!.Errors[0].ErrorMessage}"))));

var app = builder.Build();

app.UseCors();
app.MapControllers();
app.Run();

public partial class Program;
