using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class SessionRunnerTests
{
    private sealed class Recorder : IBatchPublisher
    {
        private readonly List<OutgoingBatch> _batches = new();
        public List<OutgoingBatch> Batches { get { lock (_batches) return _batches.ToList(); } }
        public Task<bool> PublishAsync(OutgoingBatch b, CancellationToken ct) { lock (_batches) _batches.Add(b); return Task.FromResult(true); }
    }

    private sealed class Harness
    {
        public FakeTimeProvider Time { get; } = new(DateTimeOffset.Parse("2026-10-01T10:00:00Z"));
        public EventState Events { get; } = new();
        public Recorder Publisher { get; } = new();
        public FastPollingController Controller { get; }
        public SessionRunner Runner { get; }

        public Harness(SimulatorOptions? options = null)
        {
            Controller = new FastPollingController(Time);
            var seed = Seed();
            Runner = new SessionRunner(() => new PumpDevice(seed, 1), Events, Publisher, options ?? new SimulatorOptions(),
                Controller, Time, NullLogger<SessionRunner>.Instance);
        }

        public async Task<(FastPollingSession Session, Task Run)> StartAsync(string payload = "{}")
        {
            Controller.Invoke("FastPollingEnable", JsonDocument.Parse(payload).RootElement.Clone());
            var s = Controller.Active!;
            var run = Runner.RunAsync(s, CancellationToken.None);
            await Task.Delay(20);
            return (s, run);
        }

        public async Task TickAsync(int n)
        {
            for (var i = 0; i < n; i++) { Time.Advance(TimeSpan.FromSeconds(1)); await Task.Delay(20); }
        }

        public async Task UntilAsync(Func<bool> condition)
        {
            var deadline = DateTime.UtcNow.AddSeconds(5);
            while (!condition()) { if (DateTime.UtcNow > deadline) throw new TimeoutException(); await Task.Delay(10); }
        }
    }

    [Fact]
    public async Task First_batch_has_Body_with_full_Event_then_deltas()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync();
        await h.TickAsync(9);
        await h.UntilAsync(() => h.Publisher.Batches.Count == 2);

        var b = h.Publisher.Batches;
        Assert.NotNull(b[0].Entries[0].Body);
        Assert.Equal(18, b[0].Entries[0].Event!.Count);
        Assert.All(b[0].Entries.Skip(1), e => Assert.NotNull(e.Changes));
        Assert.All(b[1].Entries, e => Assert.Null(e.Body));

        h.Controller.Invoke("FastPollingDisable", null);
        await run;
    }

    [Fact]
    public async Task Event_change_appears_on_the_next_entry_only()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync();
        await h.TickAsync(1);
        h.Events.TrySet(new Dictionary<string, int> { ["OperatingAlarmActive"] = 6 }, out _);
        await h.TickAsync(3);
        await h.UntilAsync(() => h.Publisher.Batches.Count == 1);

        var entries = h.Publisher.Batches[0].Entries;
        Assert.Null(entries[1].Event);
        Assert.Equal(new Dictionary<string, int> { ["OperatingAlarmActive"] = 6 }, entries[2].Event);
        Assert.Equal(6, entries[2].Changes!["OperatingAlarmActive"].GetInt32());
        Assert.Null(entries[3].Event);

        h.Controller.Invoke("FastPollingDisable", null);
        await run;
    }

    [Fact]
    public async Task Disable_flushes_partial_batch_and_clears_controller()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync();
        await h.TickAsync(6);
        await h.UntilAsync(() => h.Publisher.Batches.Count == 1);
        h.Controller.Invoke("FastPollingDisable", null);
        await run;

        Assert.Equal(2, h.Publisher.Batches[^1].Entries.Count);
        Assert.Null(h.Controller.Active);
    }

    [Fact]
    public async Task Poll_interval_is_honoured()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync("""{"pollIntervalSeconds":5}""");
        await h.TickAsync(20); // 20 s at 5 s/tick → ticks at 0,5,10,15,20 = 5 entries
        await h.UntilAsync(() => h.Publisher.Batches.Count == 1);

        var ts = h.Publisher.Batches[0].Entries.Select(e => e.TimestampUtc).ToList();
        Assert.Equal(TimeSpan.FromSeconds(5), ts[1] - ts[0]);

        h.Controller.Invoke("FastPollingDisable", null);
        await run;
    }

    [Fact]
    public async Task Alarm_at_start_opens_with_the_alarm_in_the_baseline()
    {
        var h = new Harness(new SimulatorOptions { AlarmAtStart = true });
        var (_, run) = await h.StartAsync();
        await h.TickAsync(4);
        await h.UntilAsync(() => h.Publisher.Batches.Count == 1);

        Assert.Equal(5, h.Publisher.Batches[0].Entries[0].Event!["OperatingAlarmActive"]);

        h.Controller.Invoke("FastPollingDisable", null);
        await run;
    }

    [Fact]
    public async Task Expiry_ends_the_run()
    {
        var h = new Harness();
        var (_, run) = await h.StartAsync("""{"sessionDurationMinutes":10}""");
        h.Time.Advance(TimeSpan.FromMinutes(10));
        await run.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Null(h.Controller.Active);
    }
}
