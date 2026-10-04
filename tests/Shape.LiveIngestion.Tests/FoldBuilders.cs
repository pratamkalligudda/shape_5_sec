using System.Text.Json;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public static class FoldBuilders
{
    public static readonly DateTimeOffset T0 = DateTimeOffset.Parse("2026-10-01T10:00:00Z");
    public static readonly ResolvedDevice Device = new(Guid.Parse("9e2a4f11-3c7d-4b62-9a10-77c5e0b1d284"), "PUMP-00417");

    public static Dictionary<string, JsonElement> M(string json) =>
        JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());

    public static LiveEntry Base(int sec, string body, string? ev = null) => new(T0.AddSeconds(sec), M(body), null, ev is null ? null : M(ev));
    public static LiveEntry Delta(int sec, string changes = "{}", string? ev = null) => new(T0.AddSeconds(sec), null, M(changes), ev is null ? null : M(ev));

    public static LiveTelemetryBatch Batch(params LiveEntry[] entries) =>
        new(TestEvents.Session, entries[^1].TimestampUtc, "gw-001", 700, entries);

    public static Dictionary<string, JsonElement> Seed() =>
        JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "config", "pump-seed.json")))!;
}
