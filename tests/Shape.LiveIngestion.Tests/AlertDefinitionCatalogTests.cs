using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class AlertDefinitionCatalogTests
{
    [Fact]
    public async Task Loads_eighteen_definitions_with_labels_on_the_two_operational_codes_only()
    {
        var defs = await new AlertDefinitionCatalog(AlertDefinitionCatalog.DefaultPath, NullLogger<AlertDefinitionCatalog>.Instance).GetAsync();

        Assert.Equal(18, defs.Count);
        Assert.Equal("Emergency Stop Active", defs["OperatingAlarmActive"].ValueLabels![5]);
        Assert.Equal("Left Overstroke Warning", defs["OperatingWarningActive"].ValueLabels![21]);
        Assert.Equal(2, defs.Values.Count(d => d.ValueLabels is { Count: > 0 }));
        Assert.Equal("status", defs["MotorRunning"].EventType);
        Assert.Equal(40306, defs["WaterFilterAssyAlarm"].AlertCode);
    }

    [Fact]
    public async Task Loads_once()
    {
        var path = Path.GetTempFileName();
        File.Copy(AlertDefinitionCatalog.DefaultPath, path, overwrite: true);
        var catalog = new AlertDefinitionCatalog(path, NullLogger<AlertDefinitionCatalog>.Instance);
        var first = await catalog.GetAsync();
        File.WriteAllText(path, "{ broken");

        Assert.Same(first, await catalog.GetAsync());
    }

    [Fact]
    public async Task Failed_load_is_not_cached()
    {
        var path = Path.Combine(Path.GetTempPath(), $"catalog-{Guid.NewGuid():N}.json");
        var catalog = new AlertDefinitionCatalog(path, NullLogger<AlertDefinitionCatalog>.Instance);
        await Assert.ThrowsAnyAsync<Exception>(() => catalog.GetAsync());

        File.Copy(AlertDefinitionCatalog.DefaultPath, path);
        Assert.Equal(18, (await catalog.GetAsync()).Count);
    }
}
