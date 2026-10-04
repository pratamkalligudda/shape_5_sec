using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public static class TestCatalog
{
    public static IReadOnlyDictionary<string, AlertDefinition> Load() =>
        new AlertDefinitionCatalog(AlertDefinitionCatalog.DefaultPath, NullLogger<AlertDefinitionCatalog>.Instance).GetAsync().GetAwaiter().GetResult();
}
