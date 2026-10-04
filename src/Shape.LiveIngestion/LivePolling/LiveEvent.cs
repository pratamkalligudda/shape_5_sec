namespace Shape.LiveIngestion.LivePolling;

/// <summary>A resolved event occurrence (09-30 §7.1). Label is display text, null for 16 of 18 parameters.</summary>
public sealed record LiveEvent(string Parameter, long Value, string State, string EventType, string Domain, string Severity, string? Label)
{
    public const string Unknown = "unknown";
}

public static class LiveEventStates
{
    public const string Triggered = "triggered";
    public const string Cleared = "cleared";
    public const string Active = "active";
}
