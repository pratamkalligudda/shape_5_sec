namespace Shape.LiveBackend.Options;

/// <summary>LivePolling:* (spec §7.1). Interval and duration are clamped to the module's ranges on bind.</summary>
public sealed class LivePollingOptions
{
    public const string Section = "LivePolling";
    public const int MaxTotalDurationMinutes = 30;

    private int _pollIntervalSeconds = 1;
    private int _sessionDurationMinutes = 10;

    public int PollIntervalSeconds { get => _pollIntervalSeconds; set => _pollIntervalSeconds = Math.Clamp(value, 1, 60); }
    public int SessionDurationMinutes { get => _sessionDurationMinutes; set => _sessionDurationMinutes = Math.Clamp(value, 10, 30); }
    public int ExtensionMinutes { get; set; } = 10;
    public int MaxConcurrentSessions { get; set; } = 5;
    public string[] DefaultSeriesMetrics { get; set; } = [];
    public string? ModuleId { get; set; }
    public int DirectMethodTimeoutSeconds { get; set; } = 10;
    public string IoTHubUrl { get; set; } = "http://localhost:5100";
    public string SessionDbPath { get; set; } = "data/live-polling.db";
    public string TableConnectionString { get; set; } = "";
}
