using System.Text.Json;
using Shape.LiveBackend.Infrastructure;

namespace Shape.LiveBackend.Tests;

public class InfrastructureTests
{
    [Fact]
    public void Repo_root_is_the_directory_holding_the_solution()
    {
        Assert.True(File.Exists(Path.Combine(RepoPaths.Root, "ShapeLive.sln")));
        Assert.Equal(Path.Combine(RepoPaths.Root, "data", "x.db"), RepoPaths.Resolve("data/x.db"));
        Assert.Equal(@"C:\abs\x.db", RepoPaths.Resolve(@"C:\abs\x.db"));
    }

    [Fact]
    public void Dates_are_written_in_utc_with_z()
    {
        var options = new JsonSerializerOptions { Converters = { new UtcDateTimeOffsetConverter() } };
        var json = JsonSerializer.Serialize(DateTimeOffset.Parse("2026-09-28T12:27:00.123+02:00"), options);
        Assert.Equal("\"2026-09-28T10:27:00.1230000Z\"", json);
    }
}
