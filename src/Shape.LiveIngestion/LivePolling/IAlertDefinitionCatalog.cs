namespace Shape.LiveIngestion.LivePolling;

public sealed record AlertDefinition(
    string ParameterName, int AlertCode, string EventType, string Domain, string Severity, string Description,
    IReadOnlyDictionary<int, string>? ValueLabels);

/// <summary>Alert definitions keyed by ParameterName (09-30 §5.1). Static file here instead of SQL (spec §6).</summary>
public interface IAlertDefinitionCatalog
{
    Task<IReadOnlyDictionary<string, AlertDefinition>> GetAsync(CancellationToken ct = default);
}
