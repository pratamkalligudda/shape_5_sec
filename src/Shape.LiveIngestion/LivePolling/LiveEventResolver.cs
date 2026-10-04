using System.Globalization;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Event block → resolved events (09-30 §5.2, §5.3). Baseline: non-zero → active, zero → nothing.
/// Delta: non-zero → triggered, zero → cleared. Label is null on cleared (mirrors the real implementation, 09-30 §12).
/// </summary>
public static class LiveEventResolver
{
    public static IReadOnlyList<LiveEvent> Resolve(
        IReadOnlyDictionary<string, JsonElement>? eventBlock, bool isBaseline,
        IReadOnlyDictionary<string, AlertDefinition> catalog, ILogger logger)
    {
        if (eventBlock is null || eventBlock.Count == 0) return [];
        var events = new List<LiveEvent>();
        foreach (var (parameter, raw) in eventBlock)
        {
            if (!TryCoerce(raw, out var value))
            {
                logger.LogWarning("Event value for {Parameter} is not an integer ({Raw}); skipped", parameter, raw.GetRawText());
                continue;
            }
            if (isBaseline && value == 0) continue;

            var state = isBaseline ? LiveEventStates.Active : value == 0 ? LiveEventStates.Cleared : LiveEventStates.Triggered;
            catalog.TryGetValue(parameter, out var def);
            if (def is null)
                logger.LogWarning("Event parameter {Parameter} has no alert definition; emitting with unknown metadata", parameter);
            string? label = null;
            if (state != LiveEventStates.Cleared && def?.ValueLabels is { } labels && value is >= int.MinValue and <= int.MaxValue)
                labels.TryGetValue((int)value, out label);

            events.Add(new LiveEvent(parameter, value, state,
                def?.EventType ?? LiveEvent.Unknown, def?.Domain ?? LiveEvent.Unknown, def?.Severity ?? LiveEvent.Unknown, label));
        }
        return events;
    }

    private static bool TryCoerce(JsonElement raw, out long value)
    {
        value = 0;
        switch (raw.ValueKind)
        {
            case JsonValueKind.True: value = 1; return true;
            case JsonValueKind.False: value = 0; return true;
            case JsonValueKind.Number:
                if (raw.TryGetInt64(out value)) return true;
                var d = raw.GetDouble();
                if (d == Math.Floor(d) && d is >= long.MinValue and <= long.MaxValue) { value = (long)d; return true; }
                return false;
            case JsonValueKind.String:
                return long.TryParse(raw.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out value);
            default:
                return false;
        }
    }
}
