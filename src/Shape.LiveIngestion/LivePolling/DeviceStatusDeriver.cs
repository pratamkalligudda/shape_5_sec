using System.Globalization;
using System.Text.Json;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>
/// Device-level status from the folded snapshot (09-20 §7.4, 09-30 §4): 2 Alarm, 1 Warning, 0 Normal.
/// Needs no catalog; MotorRunning never affects it.
/// </summary>
public static class DeviceStatusDeriver
{
    public static int Derive(IReadOnlyDictionary<string, JsonElement> snapshot)
    {
        if (IsSet(snapshot, "OperatingAlarmActive") || IsSet(snapshot, "MaintenanceAlarmActive")) return 2;
        if (IsSet(snapshot, "OperatingWarningActive") || IsSet(snapshot, "MaintenanceWarningActive")) return 1;
        return 0;
    }

    private static bool IsSet(IReadOnlyDictionary<string, JsonElement> s, string key) =>
        s.TryGetValue(key, out var v) && v.ValueKind switch
        {
            JsonValueKind.True => true,
            JsonValueKind.Number => v.GetDouble() != 0,
            JsonValueKind.String => double.TryParse(v.GetString(), NumberStyles.Float, CultureInfo.InvariantCulture, out var d) && d != 0,
            _ => false,
        };
}
