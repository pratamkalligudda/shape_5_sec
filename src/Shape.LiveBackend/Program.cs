using Microsoft.Extensions.Options;
using Shape.LiveBackend.Devices;
using Shape.LiveBackend.Infrastructure;
using Shape.LiveBackend.Options;
using Shape.LiveBackend.Sessions;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<LivePollingOptions>(builder.Configuration.GetSection(LivePollingOptions.Section));
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddSingleton<IDeviceDirectory>(_ => new ConfigDeviceDirectory(DeviceConfig.Load(DeviceConfig.DefaultPath)));
builder.Services.AddSingleton<ILivePollingSessionRepository>(sp =>
    new SqliteLivePollingSessionRepository(RepoPaths.Resolve(sp.GetRequiredService<IOptions<LivePollingOptions>>().Value.SessionDbPath)));
builder.Services.AddHostedService<SessionSchemaInitializer>();
builder.Services.AddCors(o => o.AddDefaultPolicy(p => p
    .WithOrigins(builder.Configuration["Cors:Origin"] ?? "http://localhost:5173").AllowAnyHeader().AllowAnyMethod().AllowCredentials()));
builder.Services.AddControllers()
    .AddJsonOptions(o => o.JsonSerializerOptions.Converters.Add(new UtcDateTimeOffsetConverter()));

var app = builder.Build();

app.UseCors();
app.MapControllers();
app.Run();

public partial class Program;
