namespace Shape.Gateway.Simulator;

/// <summary>The parameters the gateway's EventModeModule edge-detects. Values are raw register values.</summary>
public static class EventParameters
{
    public static readonly IReadOnlyList<string> All =
    [
        "MotorRunning", "OperatingAlarmActive", "OperatingWarningActive", "MaintenanceAlarmActive",
        "MaintenanceWarningActive", "OilChangeWarning", "OilChangeAlarm", "OilFilterWarning", "OilFilterAlarm",
        "WaterFilterAssyWarning", "WaterFilterAssyAlarm", "TemperatureCheckValveAlarmA", "TemperatureCheckValveAlarmB",
        "TemperatureCheckValveWarningA", "TemperatureCheckValveWarningB", "BoosterPressureFault",
        "HydraulicFilterAssyWarning", "HydraulicFilterAssyAlarm",
    ];

    public static Dictionary<string, int> Initial() => All.ToDictionary(p => p, p => p == "MotorRunning" ? 1 : 0);
}
