using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shape.Contracts;
using Shape.GatewaySimulator;
using static Shape.Tests.TestJson;

namespace Shape.Tests.Simulator;

public class SessionRunnerTests
{
    private sealed class RecordingPublisher : IBatchPublisher
    {
        private readonly object _gate = new();
        private readonly List<TelemetryBatch> _batches = new();

        public IReadOnlyList<TelemetryBatch> Batches { get { lock (_gate) return _batches.ToList(); } }

        public Task<bool> PublishAsync(TelemetryBatch batch, CancellationToken ct)
        {
            lock (_gate) _batches.Add(batch);
            return Task.FromResult(true);
        }
    }

    private static async Task Until(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline) throw new TimeoutException("Condition not met");
            await Task.Delay(10);
        }
    }

    private static async Task AdvanceSeconds(FakeTimeProvider time, int seconds)
    {
        for (var i = 0; i < seconds; i++)
        {
            time.Advance(TimeSpan.FromSeconds(1));
            await Task.Delay(20); // let the timer continuation run
        }
    }

    private static (SessionRunner Runner, SessionController Controller, RecordingPublisher Publisher, FakeTimeProvider Time)
        Create(SimulatorOptions? options = null)
    {
        var time = new FakeTimeProvider(DateTimeOffset.Parse("2026-09-23T12:00:00Z"));
        var controller = new SessionController(time);
        var publisher = new RecordingPublisher();
        var seed = Seed();
        var runner = new SessionRunner(() => new PumpDevice(seed, randomSeed: 1), publisher, options ?? new SimulatorOptions(),
            controller, time, NullLogger<SessionRunner>.Instance);
        return (runner, controller, publisher, time);
    }

    [Fact]
    public async Task Publishes_baseline_batch_then_delta_batches_every_five_ticks()
    {
        var (runner, controller, publisher, time) = Create();
        var session = controller.Enable(null).Session!;
        var run = runner.RunAsync(session, CancellationToken.None);

        await AdvanceSeconds(time, 9); // ticks at t=0..9 → 10 samples → 2 batches
        await Until(() => publisher.Batches.Count == 2);

        var batches = publisher.Batches;
        Assert.Equal(new[] { 0L, 1L }, batches.Select(b => b.Properties!.BatchSeq));
        Assert.All(batches, b => Assert.Equal(session.SessionId.ToString(), b.Properties!.SessionId));
        Assert.NotNull(batches[0].Samples![0].Body);
        Assert.All(batches[0].Samples!.Skip(1), s => Assert.NotNull(s.Changes));
        Assert.All(batches[1].Samples!, s => Assert.NotNull(s.Changes));

        controller.Disable();
        await run;
    }

    [Fact]
    public async Task Disable_flushes_partial_batch_and_ends_run()
    {
        var (runner, controller, publisher, time) = Create();
        var session = controller.Enable(null).Session!;
        var run = runner.RunAsync(session, CancellationToken.None);

        await AdvanceSeconds(time, 6); // 7 samples: batch 0 (5) + 2 pending
        await Until(() => publisher.Batches.Count == 1);
        controller.Disable();
        await run;

        var last = publisher.Batches[^1];
        Assert.Equal(1, last.Properties!.BatchSeq);
        Assert.Equal(2, last.Samples!.Count);
    }

    [Fact]
    public async Task Session_expires_after_duration_and_clears_controller()
    {
        var (runner, controller, _, time) = Create();
        var session = controller.Enable("""{"sessionDurationMinutes": 10}""").Session!;
        var run = runner.RunAsync(session, CancellationToken.None);

        time.Advance(TimeSpan.FromMinutes(10) + TimeSpan.FromSeconds(1));
        await run.WaitAsync(TimeSpan.FromSeconds(5));

        Assert.Null(controller.Active);
    }

    [Fact]
    public async Task Drop_every_fault_is_applied()
    {
        var (runner, controller, publisher, time) = Create(new SimulatorOptions { DropEvery = 2 });
        var session = controller.Enable(null).Session!;
        var run = runner.RunAsync(session, CancellationToken.None);

        await AdvanceSeconds(time, 19); // 20 samples → batches 0..3, 1 and 3 dropped
        await Until(() => publisher.Batches.Count == 2);
        controller.Disable();
        await run;

        Assert.Equal(new[] { 0L, 2L }, publisher.Batches.Select(b => b.Properties!.BatchSeq));
    }
}
