using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class SimulatorOptionsTests
{
    [Fact]
    public void Defaults_when_no_args()
    {
        var o = SimulatorOptions.Parse([]);

        Assert.Null(o.RandomSeed);
        Assert.Equal(0, o.DropEvery);
        Assert.False(o.Reorder);
        Assert.Equal(SimulatorOptions.DefaultFunctionsUrl, o.FunctionsUrl);
        Assert.Equal("http://localhost:5100", o.Urls);
    }

    [Fact]
    public void Parses_all_flags()
    {
        var o = SimulatorOptions.Parse(["--seed", "42", "--drop-every", "3", "--reorder", "--functions-url", "http://x:1/hook"]);

        Assert.Equal(42, o.RandomSeed);
        Assert.Equal(3, o.DropEvery);
        Assert.True(o.Reorder);
        Assert.Equal(new Uri("http://x:1/hook"), o.FunctionsUrl);
    }

    [Theory]
    [InlineData("--drop-every", "1")]
    [InlineData("--drop-every", "abc")]
    [InlineData("--seed", "x")]
    [InlineData("--bogus", "1")]
    public void Rejects_invalid_args(string flag, string value)
    {
        Assert.Throws<ArgumentException>(() => SimulatorOptions.Parse([flag, value]));
    }
}
