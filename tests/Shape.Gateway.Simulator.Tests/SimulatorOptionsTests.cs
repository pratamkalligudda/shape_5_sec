using Shape.Gateway.Simulator;

namespace Shape.Gateway.Simulator.Tests;

public class SimulatorOptionsTests
{
    [Fact]
    public void Defaults()
    {
        var o = SimulatorOptions.Parse([]);
        Assert.Null(o.RandomSeed);
        Assert.Equal(0, o.DropEvery);
        Assert.False(o.Reorder || o.Duplicate || o.AlarmAtStart || o.Unreachable);
        Assert.Equal(EventGridPublisher.DefaultEndpoint, o.FunctionsUrl);
        Assert.Equal("http://localhost:5100", o.Urls);
    }

    [Fact]
    public void Parses_every_flag()
    {
        var o = SimulatorOptions.Parse(["--seed", "42", "--drop-every", "3", "--reorder", "--duplicate",
            "--alarm-at-start", "--unreachable", "--functions-url", "http://x/hook", "--urls", "http://localhost:6000"]);
        Assert.Equal(42, o.RandomSeed);
        Assert.Equal(3, o.DropEvery);
        Assert.True(o.Reorder && o.Duplicate && o.AlarmAtStart && o.Unreachable);
        Assert.Equal(new Uri("http://x/hook"), o.FunctionsUrl);
        Assert.Equal("http://localhost:6000", o.Urls);
    }

    [Theory]
    [InlineData("--drop-every", "1")]
    [InlineData("--seed", "x")]
    [InlineData("--bogus", "1")]
    public void Rejects_bad_args(string flag, string value) =>
        Assert.Throws<ArgumentException>(() => SimulatorOptions.Parse([flag, value]));

    [Fact]
    public void Host_configuration_args_in_key_value_form_are_left_to_the_host()
    {
        var o = SimulatorOptions.Parse(["--environment=Development", "--contentRoot=/tmp/x", "--seed", "4"]);
        Assert.Equal(4, o.RandomSeed);
    }
}
