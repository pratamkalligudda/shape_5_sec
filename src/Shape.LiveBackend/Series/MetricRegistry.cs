using System.Text.Json;

namespace Shape.LiveBackend.Series;

public sealed record MetricSelection(IReadOnlyList<string> Metrics, IReadOnlyList<string> Errors, string? Message);

/// <summary>
/// Stand-in for dbo.ParameterRegistry (spec §11): the seed's field names plus the synthetic DeviceStatus.
/// Matched case-insensitively, returned in registry casing.
/// </summary>
public sealed class MetricRegistry
{
    public const string DeviceStatus = "DeviceStatus";
    public const int MaxMetrics = 10;
    public static readonly string DefaultSeedPath = Path.Combine(AppContext.BaseDirectory, "config", "pump-seed.json");

    private readonly Dictionary<string, string> _byName;

    public MetricRegistry(IEnumerable<string> fieldNames)
    {
        _byName = fieldNames.Append(DeviceStatus).Distinct(StringComparer.OrdinalIgnoreCase)
            .ToDictionary(n => n, n => n, StringComparer.OrdinalIgnoreCase);
        Names = _byName.Values.Order(StringComparer.Ordinal).ToList();
    }

    public IReadOnlyList<string> Names { get; }

    public static MetricRegistry Load(string seedPath)
    {
        using var doc = JsonDocument.Parse(File.ReadAllText(seedPath));
        return new MetricRegistry(doc.RootElement.EnumerateObject().Select(p => p.Name).ToList());
    }

    public MetricSelection Select(IReadOnlyList<string?>? requested, IReadOnlyList<string> defaults)
    {
        IReadOnlyList<string?> input = requested is { Count: > 0 } ? requested : defaults;
        var chosen = new List<string>();
        var errors = new List<string>();
        var unknown = false;

        foreach (var name in input)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                errors.Add("Metric names must not be blank.");
                unknown = true;
            }
            else if (!_byName.TryGetValue(name, out var canonical))
            {
                errors.Add($"'{name}' is not a known metric.");
                unknown = true;
            }
            else if (!chosen.Contains(canonical))
            {
                chosen.Add(canonical);
            }
        }
        if (chosen.Count > MaxMetrics) errors.Add($"At most {MaxMetrics} metrics may be requested; {chosen.Count} were.");

        if (errors.Count == 0) return new MetricSelection(chosen, [], null);
        errors.Add("Valid metrics: " + string.Join(", ", Names));
        return new MetricSelection([], errors, unknown ? "Unknown metric requested." : "Too many metrics requested.");
    }
}
