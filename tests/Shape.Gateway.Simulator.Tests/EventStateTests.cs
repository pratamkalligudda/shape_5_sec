using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class EventStateTests
{
    [Fact]
    public void Starts_with_motor_running_and_everything_else_zero()
    {
        var s = new EventState().Snapshot();
        Assert.Equal(1, s["MotorRunning"]);
        Assert.All(s.Where(kv => kv.Key != "MotorRunning"), kv => Assert.Equal(0, kv.Value));
    }

    [Fact]
    public void TrySet_applies_known_parameters()
    {
        var state = new EventState();
        Assert.True(state.TrySet(new Dictionary<string, int> { ["OperatingAlarmActive"] = 6 }, out var unknown));
        Assert.Empty(unknown);
        Assert.Equal(6, state.Snapshot()["OperatingAlarmActive"]);
    }

    [Fact]
    public void TrySet_with_unknown_parameter_applies_nothing()
    {
        var state = new EventState();
        Assert.False(state.TrySet(new Dictionary<string, int> { ["OperatingAlarmActive"] = 6, ["Nope"] = 1 }, out var unknown));
        Assert.Equal(new[] { "Nope" }, unknown);
        Assert.Equal(0, state.Snapshot()["OperatingAlarmActive"]);
    }

    [Fact]
    public void Reset_restores_initial_values()
    {
        var state = new EventState();
        state.TrySet(new Dictionary<string, int> { ["MotorRunning"] = 0, ["OilChangeAlarm"] = 1 }, out _);
        state.Reset();
        Assert.Equal(1, state.Snapshot()["MotorRunning"]);
        Assert.Equal(0, state.Snapshot()["OilChangeAlarm"]);
    }
}
