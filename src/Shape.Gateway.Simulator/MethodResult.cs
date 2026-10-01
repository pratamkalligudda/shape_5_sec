namespace Shape.Gateway.Simulator;

/// <summary>A direct-method response: the module's status code and JSON payload.</summary>
public sealed record MethodResult(int Status, object Payload);
