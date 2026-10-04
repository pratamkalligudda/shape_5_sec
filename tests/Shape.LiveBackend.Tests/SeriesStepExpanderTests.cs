using System.Text.Json;
using Shape.LiveBackend.LiveData;
using Shape.LiveBackend.Series;

namespace Shape.LiveBackend.Tests;

public class SeriesStepExpanderTests
{
    private static readonly DateTimeOffset First = DateTimeOffset.Parse("2026-10-01T10:00:00.22Z");

    private static ChangePoint P(double seconds, object value) =>
        new(First.AddSeconds(seconds), JsonSerializer.SerializeToElement(value));

    [Fact]
    public void Grid_is_first_tick_plus_k_intervals_up_to_the_last_tick()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(8), 2, null);
        Assert.Equal(new[] { 0, 2, 4, 6, 8 }, grid.Select(t => (int)(t - First).TotalSeconds));
    }

    [Fact]
    public void Grid_is_empty_before_the_first_tick() =>
        Assert.Empty(SeriesStepExpander.Grid(null, null, 1, null));

    [Fact]
    public void Since_keeps_only_grid_points_at_or_after_it()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(4), 1, First.AddSeconds(2.5));
        Assert.Equal(new[] { 3, 4 }, grid.Select(t => (int)Math.Round((t - First).TotalSeconds)));
    }

    [Fact]
    public void Grid_drops_the_newest_tick_when_jitter_shortens_the_span()
    {
        // Five ticks whose last one arrived 2 ms early: floor(3.998) = 3 → four grid points (finding F4).
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(3.998), 1, null);
        Assert.Equal(4, grid.Count);
    }

    [Fact]
    public void A_point_jittered_by_less_than_half_an_interval_lands_on_its_tick()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(3), 1, null);
        var v = SeriesStepExpander.Expand(grid, [P(0, 1), P(0.99, 2), P(3.4, 3)], 1);

        Assert.Equal(new[] { "1", "2", "2", "3" }, v.Select(e => e!.Value.GetRawText()));
    }

    [Fact]
    public void Values_are_null_before_the_first_report()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(2), 1, null);
        var v = SeriesStepExpander.Expand(grid, [P(1, 54000)], 1);

        Assert.Null(v[0]);
        Assert.Equal(54000, v[1]!.Value.GetInt32());
        Assert.Equal(54000, v[2]!.Value.GetInt32());
    }

    [Fact]
    public void A_point_exactly_half_an_interval_late_belongs_to_the_next_tick()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(2), 2, null);
        var v = SeriesStepExpander.Expand(grid, [P(0, "a"), P(1, "b")], 2);

        Assert.Equal(new[] { "\"a\"", "\"b\"" }, v.Select(e => e!.Value.GetRawText()));
    }

    [Fact]
    public void Status_carries_forward_and_is_zero_before_the_first_row()
    {
        var grid = SeriesStepExpander.Grid(First, First.AddSeconds(3), 1, null);
        Assert.Equal(new[] { 0, 2, 2, 0 }, SeriesStepExpander.ExpandStatus(grid, [P(1, 2L), P(3, 0L)], 1));
    }
}
