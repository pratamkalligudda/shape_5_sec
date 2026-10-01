using System.Threading.Channels;

namespace Shape.Gateway.Simulator;

/// <summary>
/// Runs one session: poll every PollIntervalSeconds, encode, batch 5, publish. Publishing runs on its own consumer so
/// retries never delay sampling, and one failed delivery never ends the session.
/// </summary>
public sealed class SessionRunner(
    Func<PumpDevice> deviceFactory, EventState events, IBatchPublisher publisher, SimulatorOptions options,
    FastPollingController controller, TimeProvider time, ILogger<SessionRunner> logger)
{
    public async Task RunAsync(FastPollingSession session, CancellationToken hostStopping)
    {
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(session.Expired, session.Stopped, hostStopping);
        if (options.AlarmAtStart) events.TrySet(new Dictionary<string, int> { ["OperatingAlarmActive"] = 5 }, out _);

        var device = deviceFactory();
        var encoder = new EntryEncoder();
        var buffer = new BatchBuffer(session.SessionId);
        var faults = new FaultInjector(options.DropEvery, options.Reorder, options.Duplicate);
        var outbox = Channel.CreateUnbounded<OutgoingBatch>(new UnboundedChannelOptions { SingleReader = true });
        var consumer = Task.Run(async () =>
        {
            await foreach (var batch in outbox.Reader.ReadAllAsync())
            {
                try { await publisher.PublishAsync(batch, CancellationToken.None); }
                catch (Exception ex) { logger.LogError(ex, "Publishing batch {Seq} threw; continuing", batch.Seq); }
            }
        });

        logger.LogInformation("Twin: FastPolling Active=true SessionId={SessionId} DurationMinutes={Minutes} IntervalSeconds={Interval}",
            session.SessionId, session.TotalDurationMinutes, session.PollIntervalSeconds);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(session.PollIntervalSeconds), time);
        try
        {
            do
            {
                var now = time.GetUtcNow();
                var ev = events.Snapshot();
                var entry = encoder.Encode(device.Read(ev), ev, now);
                if (buffer.Add(entry, now) is { } batch)
                    foreach (var b in faults.Process(batch)) outbox.Writer.TryWrite(b);
            }
            while (await timer.WaitForNextTickAsync(stop.Token));
        }
        catch (OperationCanceledException)
        {
            // disable, expiry or host shutdown
        }
        finally
        {
            controller.Clear(session);
            if (buffer.Flush(time.GetUtcNow()) is { } last)
                foreach (var b in faults.Process(last)) outbox.Writer.TryWrite(b);
            foreach (var b in faults.Drain()) outbox.Writer.TryWrite(b);
            outbox.Writer.Complete();
            await consumer;
            var reason = session.Stopped.IsCancellationRequested ? "ManualDisable"
                : session.Expired.IsCancellationRequested ? "AutoExpiry" : "HostShutdown";
            logger.LogInformation("Twin: FastPolling Active=false SessionId={SessionId} StopReason={Reason}", session.SessionId, reason);
        }
    }
}
