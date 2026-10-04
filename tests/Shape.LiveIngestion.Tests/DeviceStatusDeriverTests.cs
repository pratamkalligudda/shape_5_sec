using System.Text.Json;
using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class DeviceStatusDeriverTests
{
    private static Dictionary<string, JsonElement> S(string json) =>
        JsonDocument.Parse(json).RootElement.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.Clone());

    [Theory]
    [InlineData("""{"OperatingAlarmActive":0,"OperatingWarningActive":0,"MaintenanceAlarmActive":false,"MaintenanceWarningActive":false}""", 0)]
    [InlineData("""{"OperatingAlarmActive":0,"OperatingWarningActive":21,"MaintenanceAlarmActive":false}""", 1)]
    [InlineData("""{"MaintenanceWarningActive":true}""", 1)]
    [InlineData("""{"OperatingAlarmActive":5,"OperatingWarningActive":21}""", 2)]
    [InlineData("""{"MaintenanceAlarmActive":true,"OperatingWarningActive":21}""", 2)]
    [InlineData("""{"MaintenanceAlarmActive":1}""", 2)]
    [InlineData("""{}""", 0)]
    public void Alarm_outranks_warning(string json, int expected) => Assert.Equal(expected, DeviceStatusDeriver.Derive(S(json)));

    [Fact]
    public void MotorRunning_never_affects_status()
    {
        Assert.Equal(0, DeviceStatusDeriver.Derive(S("""{"MotorRunning":false,"OilFilterAlarm":true}""")));
    }
}
