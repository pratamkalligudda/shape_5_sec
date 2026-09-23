using Shape.Contracts;

namespace Shape.GatewaySimulator;

public interface IBatchPublisher
{
    /// <returns>true if delivered; false if dropped after all retries.</returns>
    Task<bool> PublishAsync(TelemetryBatch batch, CancellationToken ct);
}
