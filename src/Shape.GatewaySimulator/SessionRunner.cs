using System.Threading.Channels;
using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>
/// Runs one fast-polling session: poll every second, encode, batch, publish. Publishing happens on a separate
/// consumer so slow retries never delay the 1-second sampling.
/// </summary>
public sealed class SessionRunner(
    Func<PumpDevice> deviceFactory,
    IBatchPublisher publisher,
    SimulatorOptions options,
    SessionController controller,
    TimeProvider time,
    ILogger<SessionRunner> logger)
{
    public async Task RunAsync(FastPollingSession session, CancellationToken hostStopping)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(session.Cts.Token, hostStopping);
        using var expiry = new CancellationTokenSource(TimeSpan.FromMinutes(session.DurationMinutes), time);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(linked.Token, expiry.Token);

        var device = deviceFactory();
        var encoder = new DeltaEncoder();
        var buffer = new BatchBuffer(session.SessionId.ToString());
        var faults = new FaultInjector(options.DropEvery, options.Reorder);
        var outbox = Channel.CreateUnbounded<TelemetryBatch>(new UnboundedChannelOptions { SingleReader = true });
        var consumer = Task.Run(async () =>
        {
            await foreach (var batch in outbox.Reader.ReadAllAsync())
                await publisher.PublishAsync(batch, CancellationToken.None);
        });

        logger.LogInformation("Session {SessionId} started ({Minutes} min)", session.SessionId, session.DurationMinutes);
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1), time);
        try
        {
            do
            {
                var sample = encoder.Encode(device.Read(), time.GetUtcNow());
                if (buffer.Add(sample) is { } batch)
                    foreach (var b in faults.Process(batch)) outbox.Writer.TryWrite(b);
            }
            while (await timer.WaitForNextTickAsync(stop.Token));
        }
        catch (OperationCanceledException)
        {
            // disable, expiry, or host shutdown
        }
        finally
        {
            controller.Clear(session);
            if (buffer.Flush() is { } last)
                foreach (var b in faults.Process(last)) outbox.Writer.TryWrite(b);
            foreach (var b in faults.Drain()) outbox.Writer.TryWrite(b);
            outbox.Writer.Complete();
            await consumer;

            var reason = session.Cts.IsCancellationRequested ? "disabled"
                : expiry.IsCancellationRequested ? "expired"
                : "host stopping";
            logger.LogInformation("Session {SessionId} ended: {Reason}", session.SessionId, reason);
        }
    }
}
