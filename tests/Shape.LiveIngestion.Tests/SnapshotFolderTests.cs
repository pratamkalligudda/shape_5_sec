using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.LiveIngestion.LivePolling;
using static Shape.LiveIngestion.Tests.FoldBuilders;

namespace Shape.LiveIngestion.Tests;

public class SnapshotFolderTests
{
    private static readonly IReadOnlyDictionary<string, AlertDefinition> Catalog = TestCatalog.Load();

    private static FoldResult Fold(LiveHead? head, LiveTelemetryBatch batch) =>
        SnapshotFolder.Fold(head, batch, Device, Catalog, NullLogger.Instance);

    private const string BaseBody = """{"P":100,"OperatingAlarmActive":0,"OperatingWarningActive":0,"MotorRunning":true}""";
    private const string BaseEvents = """{"OperatingAlarmActive":0,"OperatingWarningActive":0,"MotorRunning":1}""";

    private static FoldResult FirstBatch() => Fold(null, Batch(
        Base(0, BaseBody, BaseEvents),
        Delta(1, """{"P":101}"""),
        Delta(2),
        Delta(3, """{"OperatingWarningActive":21}""", """{"OperatingWarningActive":21}"""),
        Delta(4, """{"P":102}""")));

    [Fact]
    public void First_batch_folds_baseline_and_deltas_into_the_head()
    {
        var r = FirstBatch();

        Assert.Equal(5, r.Applied);
        Assert.True(r.Head.HasBaseline);
        Assert.Equal(102, r.Head.Snapshot["P"].GetInt32());
        Assert.Equal(1, r.Head.DeviceStatus);
        Assert.Equal(5, r.Head.TickCount);
        Assert.Equal(T0, r.Head.FirstTickUtc);
        Assert.Equal(T0.AddSeconds(4), r.Head.LastTickUtc);
        Assert.Equal(T0.AddSeconds(4), r.Head.LastSnapshotTimestampUtc);
        Assert.Equal(700, r.Head.BytesTotal);
        Assert.Equal(Device.DeviceGuid, r.Head.DeviceGuid);
        Assert.Equal("PUMP-00417", r.Head.DeviceId);
    }

    [Fact]
    public void Change_points_are_every_baseline_field_plus_each_changed_field()
    {
        var cps = FirstBatch().ChangePoints.Where(c => c.Parameter != SnapshotFolder.StatusParameter).ToList();

        Assert.Equal(4 + 1 + 1 + 1, cps.Count);   // baseline 4 fields, P@1, warning@3, P@4
        Assert.Contains(cps, c => c.Parameter == "P" && c.TimestampUtc == T0.AddSeconds(1) && c.Value.GetInt32() == 101);
        Assert.DoesNotContain(cps, c => c.TimestampUtc == T0.AddSeconds(2)); // heartbeat writes nothing
    }

    [Fact]
    public void Status_rows_are_written_only_where_status_moves()
    {
        var status = FirstBatch().ChangePoints.Where(c => c.Parameter == SnapshotFolder.StatusParameter).ToList();

        Assert.Equal(new[] { (T0, 0L), (T0.AddSeconds(3), 1L) }, status.Select(s => (s.TimestampUtc, s.Value.GetInt64())));
    }

    [Fact]
    public void Baseline_zeros_write_no_events_and_delta_writes_triggered()
    {
        var events = FirstBatch().Events;

        Assert.Equal(new[] { ("MotorRunning", "active"), ("OperatingWarningActive", "triggered") },
            events.Select(e => (e.Event.Parameter, e.Event.State)));
        Assert.Equal(T0.AddSeconds(3), events[1].TimestampUtc);
        Assert.Equal("Left Overstroke Warning", events[1].Event.Label);
    }

    [Fact]
    public void Broadcast_leads_with_snapshot_then_changes_with_events_only_where_present()
    {
        var b = FirstBatch().Broadcast;

        Assert.Equal(5, b.Count);
        Assert.NotNull(b[0].Snapshot);
        Assert.Null(b[0].Changes);
        Assert.All(b.Skip(1), e => { Assert.Null(e.Snapshot); Assert.NotNull(e.Changes); });
        Assert.Empty(b[2].Changes!);
        Assert.Null(b[1].Events);
        Assert.Equal("triggered", Assert.Single(b[3].Events!).State);
        Assert.Equal(new[] { 0, 0, 0, 1, 1 }, b.Select(e => e.DeviceStatus));
        Assert.Equal("active", Assert.Single(b[0].Events!).State);
    }

    [Fact]
    public void Broadcast_snapshot_is_an_independent_copy()
    {
        var r = FirstBatch();
        r.Broadcast[0].Snapshot!["P"] = JsonSerializer.SerializeToElement(999);
        Assert.Equal(102, r.Head.Snapshot["P"].GetInt32());
    }

    [Fact]
    public void Next_batch_folds_onto_the_stored_head_and_re_materialises_the_snapshot()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Delta(5, """{"P":103}"""), Delta(6), Delta(7), Delta(8), Delta(9)));

        Assert.Equal(5, r.Applied);
        Assert.Equal(10, r.Head.TickCount);
        Assert.Equal(1400, r.Head.BytesTotal);
        Assert.Equal(103, r.Broadcast[0].Snapshot!["P"].GetInt32());
        Assert.Equal(21, r.Broadcast[0].Snapshot!["OperatingWarningActive"].GetInt32());
        Assert.DoesNotContain(r.ChangePoints, c => c.Parameter == SnapshotFolder.StatusParameter);
    }

    [Fact]
    public void Delta_clear_writes_cleared_event_and_status_row()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Delta(5, """{"OperatingWarningActive":0}""", """{"OperatingWarningActive":0}""")));

        var e = Assert.Single(r.Events);
        Assert.Equal("cleared", e.Event.State);
        Assert.Contains(r.ChangePoints, c => c.Parameter == SnapshotFolder.StatusParameter && c.Value.GetInt64() == 0);
    }

    [Fact]
    public void Fully_duplicate_batch_has_no_writes_and_no_broadcast()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Delta(1, """{"P":101}"""), Delta(2), Delta(3), Delta(4)));

        Assert.False(r.HasWrites);
        Assert.Empty(r.Broadcast);
        Assert.Equal(700, r.Head.BytesTotal);
    }

    [Fact]
    public void Overlapping_redelivery_applies_only_new_entries()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Delta(3), Delta(4), Delta(5, """{"P":200}"""), Delta(6)));

        Assert.Equal(2, r.Applied);
        Assert.Equal(T0.AddSeconds(5), r.Broadcast[0].TimestampUtc);
        Assert.Equal(200, r.Broadcast[0].Snapshot!["P"].GetInt32());
    }

    [Fact]
    public void Second_entry_with_same_timestamp_is_skipped()
    {
        var r = Fold(null, Batch(Base(0, BaseBody, BaseEvents), Delta(1, """{"P":1}"""), Delta(1, """{"P":2}""")));

        Assert.Equal(2, r.Applied);
        Assert.Equal(1, r.Head.Snapshot["P"].GetInt32());
        Assert.Single(r.ChangePoints, c => c.Parameter == "P" && c.TimestampUtc == T0.AddSeconds(1));
    }

    [Fact]
    public void Out_of_order_baseline_back_fills_without_overwriting_newer_values()
    {
        // Batch 1 (deltas) arrives before batch 0 (baseline).
        var early = Fold(null, Batch(Delta(5, """{"P":150}"""), Delta(6)));
        Assert.False(early.Head.HasBaseline);

        var r = Fold(early.Head, Batch(Base(0, """{"P":100,"Q":7,"OperatingAlarmActive":5}""", """{"OperatingAlarmActive":5}"""), Delta(1)));

        Assert.True(r.HasWrites);
        Assert.Equal(1, r.Backfilled);
        Assert.Equal(0, r.Applied);
        Assert.True(r.Head.HasBaseline);
        Assert.Equal(150, r.Head.Snapshot["P"].GetInt32());        // newer value kept
        Assert.Equal(7, r.Head.Snapshot["Q"].GetInt32());          // missing field filled
        Assert.Equal(T0.AddSeconds(6), r.Head.LastSnapshotTimestampUtc);
        Assert.Equal(T0, r.Head.FirstTickUtc);
        Assert.Contains(r.ChangePoints, c => c.Parameter == "Q" && c.TimestampUtc == T0);
        Assert.Equal("active", Assert.Single(r.Events).Event.State);
        Assert.Empty(r.Broadcast);
    }

    [Fact]
    public void Second_baseline_mid_session_rebaselines_and_records_active_events()
    {
        var head = FirstBatch().Head;
        var r = Fold(head, Batch(Base(5, """{"P":500,"OperatingAlarmActive":6,"OperatingWarningActive":0,"MotorRunning":true}""",
            """{"OperatingAlarmActive":6,"MotorRunning":1}""")));

        Assert.Equal(500, r.Head.Snapshot["P"].GetInt32());
        Assert.Equal(2, r.Head.DeviceStatus);
        Assert.All(r.Events, e => Assert.Equal("active", e.Event.State));
        Assert.Equal(T0.AddSeconds(5), r.Events[0].TimestampUtc);
    }

    [Fact]
    public void Real_seed_first_batch_exceeds_the_transaction_limit_but_later_batches_fit()
    {
        var seed = Seed();
        var body = JsonSerializer.Serialize(seed);
        var deltaKeys = new[] { "OperatingPressurePSI", "OperatingPressureBar", "StrokeRateLeft", "StrokeRateRight",
            "StrokeTimeLeft", "StrokeTimeRight", "StrokeCounterTW1", "ProportionalOutputValue" };
        string D(int i) => JsonSerializer.Serialize(deltaKeys.ToDictionary(k => k, _ => i));

        var first = Fold(null, Batch(Base(0, body, """{"MotorRunning":1}"""), Delta(1, D(1)), Delta(2, D(2)), Delta(3, D(3)), Delta(4, D(4))));
        var later = Fold(first.Head, Batch(Delta(5, D(5)), Delta(6, D(6)), Delta(7, D(7)), Delta(8, D(8)), Delta(9, D(9))));

        Assert.Equal(73, seed.Count);
        Assert.True(first.EntityCount > 100, $"first batch = {first.EntityCount} entities");   // F2
        Assert.True(later.EntityCount <= 100, $"later batch = {later.EntityCount} entities");
    }
}
