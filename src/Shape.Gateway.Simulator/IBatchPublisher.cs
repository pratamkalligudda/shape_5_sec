namespace Shape.Gateway.Simulator;

public interface IBatchPublisher
{
    /// <returns>true when delivered; false when dropped after all retries.</returns>
    Task<bool> PublishAsync(OutgoingBatch batch, CancellationToken ct);
}
