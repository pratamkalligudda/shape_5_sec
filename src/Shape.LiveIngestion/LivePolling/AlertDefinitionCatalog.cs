using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>Loads once, holds for the life of the process, never caches a failed load.</summary>
public sealed class AlertDefinitionCatalog(string path, ILogger<AlertDefinitionCatalog> logger) : IAlertDefinitionCatalog
{
    public static readonly string DefaultPath = Path.Combine(AppContext.BaseDirectory, "config", "alert-catalog.json");

    private readonly SemaphoreSlim _gate = new(1, 1);
    private IReadOnlyDictionary<string, AlertDefinition>? _loaded;

    public async Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default)
    {
        if (Volatile.Read(ref _loaded) is { } cached) return cached;
        await _gate.WaitAsync(ct);
        try
        {
            if (_loaded is not null) return _loaded;
            await using var stream = File.OpenRead(path);
            var file = await JsonSerializer.DeserializeAsync<CatalogFile>(stream, new JsonSerializerOptions(JsonSerializerDefaults.Web), ct)
                       ?? throw new InvalidDataException($"Alert catalog '{path}' is empty.");
            var map = file.Definitions.ToDictionary(
                d => d.ParameterName,
                d => new AlertDefinition(d.ParameterName, d.AlertCode, d.EventType, d.Domain, d.Severity, d.Description,
                    d.ValueLabels?.ToDictionary(kv => int.Parse(kv.Key), kv => kv.Value)),
                StringComparer.Ordinal);
            logger.LogInformation("Loaded {Count} alert definitions from {Path}", map.Count, path);
            Volatile.Write(ref _loaded, map);
            return map;
        }
        finally
        {
            _gate.Release();
        }
    }

    private sealed record CatalogFile(List<CatalogEntry> Definitions);

    private sealed record CatalogEntry(
        string ParameterName, int AlertCode, string EventType, string Domain, string Severity, string Description,
        string? Action, Dictionary<string, string>? ValueLabels);
}
