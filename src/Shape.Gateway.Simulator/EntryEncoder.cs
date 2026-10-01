using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>Body + full Event on a session's first tick; Changes + changed-only Event afterwards. One per session.</summary>
public sealed class EntryEncoder
{
    private Dictionary<string, JsonElement>? _lastSent;
    private Dictionary<string, int>? _lastEvents;

    public GatewayEntry Encode(IReadOnlyDictionary<string, JsonElement> reading, IReadOnlyDictionary<string, int> events, DateTimeOffset ts)
    {
        var current = new Dictionary<string, JsonElement>(reading);
        var currentEvents = new Dictionary<string, int>(events);
        GatewayEntry entry;
        if (_lastSent is null || _lastEvents is null)
        {
            entry = new GatewayEntry { TimestampUtc = ts.UtcDateTime, Body = current, Event = new Dictionary<string, int>(currentEvents) };
        }
        else
        {
            var changes = new Dictionary<string, JsonElement>();
            foreach (var (k, v) in current)
                if (!_lastSent.TryGetValue(k, out var prev) || prev.GetRawText() != v.GetRawText())
                    changes[k] = v;
            var eventChanges = currentEvents
                .Where(kv => !_lastEvents.TryGetValue(kv.Key, out var prev) || prev != kv.Value)
                .ToDictionary(kv => kv.Key, kv => kv.Value);
            entry = new GatewayEntry
            {
                TimestampUtc = ts.UtcDateTime,
                Changes = changes,
                Event = eventChanges.Count > 0 ? eventChanges : null,
            };
        }
        _lastSent = current;
        _lastEvents = currentEvents;
        return entry;
    }
}
