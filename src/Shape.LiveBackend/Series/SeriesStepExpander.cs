using System.Text.Json;
using Shape.LiveBackend.LiveData;

namespace Shape.LiveBackend.Series;

/// <summary>
/// Spec §7.3. Grid tₖ = FirstTickUtc + k·interval for k = 0..floor((LastTickUtc − FirstTickUtc)/interval).
/// The value at tₖ is the last change point with TimestampUtc &lt; tₖ + interval/2 (absorbs gateway jitter).
/// </summary>
public static class SeriesStepExpander
{
    public static IReadOnlyList<DateTimeOffset> Grid(DateTimeOffset? firstTick, DateTimeOffset? lastTick, int intervalSeconds, DateTimeOffset? since)
    {
        if (firstTick is not { } first || lastTick is not { } last || last < first) return [];
        var interval = TimeSpan.FromSeconds(intervalSeconds);
        var steps = (last - first).Ticks / interval.Ticks;           // integer division = floor for non-negative spans
        var grid = new List<DateTimeOffset>((int)steps + 1);
        for (var k = 0L; k <= steps; k++)
        {
            var t = first + TimeSpan.FromTicks(interval.Ticks * k);
            if (since is null || t >= since) grid.Add(t);
        }
        return grid;
    }

    public static IReadOnlyList<JsonElement?> Expand(IReadOnlyList<DateTimeOffset> grid, IReadOnlyList<ChangePoint> points, int intervalSeconds)
    {
        var half = TimeSpan.FromSeconds(intervalSeconds) / 2;
        var values = new List<JsonElement?>(grid.Count);
        JsonElement? current = null;
        var j = 0;
        foreach (var t in grid)
        {
            while (j < points.Count && points[j].TimestampUtc < t + half) current = points[j++].Value;
            values.Add(current);
        }
        return values;
    }

    public static IReadOnlyList<int> ExpandStatus(IReadOnlyList<DateTimeOffset> grid, IReadOnlyList<ChangePoint> points, int intervalSeconds) =>
        Expand(grid, points, intervalSeconds).Select(v => v is { ValueKind: JsonValueKind.Number } n ? n.GetInt32() : 0).ToList();
}
