using System.Text.Json;

namespace Shape.GatewaySimulator;

/// <summary>
/// Stand-in for the Modbus pump. Each <see cref="Read"/> is one 1-second poll. Fields without a generator
/// keep their seed value (spec §5.1).
/// </summary>
public sealed class PumpDevice
{
    private const int FlagHoldTicks = 20;
    private static readonly string[] HourKeys =
        ["OperatingHoursTW1", "OperatingHoursCheckValveAssy_A", "OperatingHoursCheckValveAssy_B", "OperatingHoursMotor"];

    private readonly Dictionary<string, JsonElement> _seed;
    private readonly Random _rng;
    private readonly double _flagProbability;
    private readonly IReadOnlyList<string> _flagKeys;
    private readonly Dictionary<string, int> _flagRemaining = new();
    private long _tick;
    private int _strokeRateLeft, _strokeRateRight, _strokeTimeLeft, _strokeTimeRight, _oilTempF;
    private double _strokeCounter;

    public PumpDevice(IReadOnlyDictionary<string, JsonElement> seed, int? randomSeed = null, double flagProbability = 0.002)
    {
        _seed = new Dictionary<string, JsonElement>(seed);
        _rng = randomSeed is { } s ? new Random(s) : new Random();
        _flagProbability = flagProbability;
        _flagKeys = FlagKeys(_seed.Keys);
        _strokeRateLeft = Math.Clamp(SeedInt("StrokeRateLeft"), 80, 100);
        _strokeRateRight = Math.Clamp(SeedInt("StrokeRateRight"), 80, 100);
        _strokeTimeLeft = Math.Clamp(SeedInt("StrokeTimeLeft"), 5200, 5400);
        _strokeTimeRight = Math.Clamp(SeedInt("StrokeTimeRight"), 5200, 5400);
        _oilTempF = Math.Clamp(SeedInt("HydraulicOilTemperatureF"), 100, 115);
        _strokeCounter = SeedInt("StrokeCounterTW1");
    }

    public static IReadOnlyList<string> FlagKeys(IEnumerable<string> keys) =>
        keys.Where(k => k.Contains("Alarm") || k.Contains("Warning") || k == "BoosterPressureFault").ToList();

    public Dictionary<string, JsonElement> Read()
    {
        var t = _tick++;
        var generated = new Dictionary<string, object>();

        var psi = (int)Math.Round(54000 + 3000 * Math.Sin(2 * Math.PI * t / 30.0) + (_rng.NextDouble() * 300 - 150));
        generated["OperatingPressurePSI"] = psi;
        generated["OperatingPressureBar"] = (int)Math.Round(psi / 14.5038);

        _strokeRateLeft = Walk(_strokeRateLeft, 2, 80, 100);
        _strokeRateRight = Walk(_strokeRateRight, 2, 80, 100);
        _strokeTimeLeft = Walk(_strokeTimeLeft, 10, 5200, 5400);
        _strokeTimeRight = Walk(_strokeTimeRight, 10, 5200, 5400);
        generated["StrokeRateLeft"] = _strokeRateLeft;
        generated["StrokeRateRight"] = _strokeRateRight;
        generated["StrokeTimeLeft"] = _strokeTimeLeft;
        generated["StrokeTimeRight"] = _strokeTimeRight;

        _strokeCounter += (_strokeRateLeft + _strokeRateRight) / 2.0 / 60.0;
        generated["StrokeCounterTW1"] = (long)_strokeCounter;

        if (t > 0 && t % 10 == 0) _oilTempF = Walk(_oilTempF, 1, 100, 115);
        generated["HydraulicOilTemperatureF"] = _oilTempF;
        generated["HydraulicOilTemperatureC"] = (int)Math.Round((_oilTempF - 32) * 5 / 9.0);

        generated["ProportionalOutputValue"] = (int)Math.Round(70 + Math.Clamp((psi - 51000) / 6000.0, 0, 1) * 20);

        foreach (var key in HourKeys)
            if (_seed.ContainsKey(key)) generated[key] = SeedInt(key) + t / 3600;

        foreach (var key in _flagKeys)
        {
            var active = NextFlagState(key);
            generated[key] = _seed[key].ValueKind is JsonValueKind.True or JsonValueKind.False ? active : (active ? 1 : 0);
        }

        generated["MotorRunning"] = true;

        var reading = new Dictionary<string, JsonElement>(_seed);
        foreach (var (key, value) in generated)
            if (reading.ContainsKey(key)) reading[key] = JsonSerializer.SerializeToElement(value);
        return reading;
    }

    private bool NextFlagState(string key)
    {
        var remaining = _flagRemaining.GetValueOrDefault(key);
        if (remaining > 0)
        {
            _flagRemaining[key] = remaining - 1;
            return true;
        }
        if (_rng.NextDouble() < _flagProbability)
        {
            _flagRemaining[key] = FlagHoldTicks - 1;
            return true;
        }
        return false;
    }

    private int Walk(int value, int step, int min, int max) => Math.Clamp(value + _rng.Next(-step, step + 1), min, max);

    private int SeedInt(string key) => _seed[key].GetInt32();
}
