using System.Text.Json;
using Shape.Contracts;

namespace Shape.GatewaySimulator;

/// <summary>
/// Turns full readings into Body (first reading of a session) or Changes (fields whose value differs from the
/// previous reading, carrying the new value). Create one instance per session.
/// </summary>
public sealed class DeltaEncoder
{
    private Dictionary<string, JsonElement>? _lastSent;

    public Sample Encode(IReadOnlyDictionary<string, JsonElement> reading, DateTimeOffset ts)
    {
        if (_lastSent is null)
        {
            _lastSent = new Dictionary<string, JsonElement>(reading);
            return new Sample { SnapshotTimestampUtc = ts, Body = new Dictionary<string, JsonElement>(reading) };
        }

        var changes = new Dictionary<string, JsonElement>();
        foreach (var (key, value) in reading)
        {
            if (!_lastSent.TryGetValue(key, out var previous) || !JsonValueEquals(previous, value))
                changes[key] = value;
        }

        _lastSent = new Dictionary<string, JsonElement>(reading);
        return new Sample { SnapshotTimestampUtc = ts, Changes = changes };
    }

    // .NET 8 has no JsonElement.DeepEquals; raw-text comparison is exact for the scalar values the device produces.
    private static bool JsonValueEquals(JsonElement a, JsonElement b) =>
        a.ValueKind == b.ValueKind && a.GetRawText() == b.GetRawText();
}
