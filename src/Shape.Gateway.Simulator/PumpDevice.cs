using System.Text.Json;

namespace Shape.Gateway.Simulator;

/// <summary>Modbus stand-in: one Read per poll. Event parameters mirror the supplied event values (spec §4.1).</summary>
public sealed class PumpDevice
{
    private static readonly string[] HourKeys =
        ["OperatingHoursTW1", "OperatingHoursCheckValveAssy_A", "OperatingHoursCheckValveAssy_B", "OperatingHoursMotor"];

    private readonly Dictionary<string, JsonElement> _seed;
    private readonly Random _rng;
    private long _tick;
    private int _rateL, _rateR, _timeL, _timeR, _oilF;
    private double _strokes;

    public PumpDevice(IReadOnlyDictionary<string, JsonElement> seed, int? randomSeed = null)
    {
        _seed = new Dictionary<string, JsonElement>(seed);
        _rng = randomSeed is { } s ? new Random(s) : new Random();
        _rateL = Math.Clamp(Int("StrokeRateLeft"), 80, 100);
        _rateR = Math.Clamp(Int("StrokeRateRight"), 80, 100);
        _timeL = Math.Clamp(Int("StrokeTimeLeft"), 5200, 5400);
        _timeR = Math.Clamp(Int("StrokeTimeRight"), 5200, 5400);
        _oilF = Math.Clamp(Int("HydraulicOilTemperatureF"), 100, 115);
        _strokes = Int("StrokeCounterTW1");
    }

    public Dictionary<string, JsonElement> Read(IReadOnlyDictionary<string, int> events)
    {
        var t = _tick++;
        var g = new Dictionary<string, object>();

        var psi = (int)Math.Round(54000 + 3000 * Math.Sin(2 * Math.PI * t / 30.0) + (_rng.NextDouble() * 300 - 150));
        g["OperatingPressurePSI"] = psi;
        g["OperatingPressureBar"] = (int)Math.Round(psi / 14.5038);
        g["StrokeRateLeft"] = _rateL = Walk(_rateL, 2, 80, 100);
        g["StrokeRateRight"] = _rateR = Walk(_rateR, 2, 80, 100);
        g["StrokeTimeLeft"] = _timeL = Walk(_timeL, 10, 5200, 5400);
        g["StrokeTimeRight"] = _timeR = Walk(_timeR, 10, 5200, 5400);
        _strokes += (_rateL + _rateR) / 2.0 / 60.0;
        g["StrokeCounterTW1"] = (long)_strokes;
        if (t > 0 && t % 10 == 0) _oilF = Walk(_oilF, 1, 100, 115);
        g["HydraulicOilTemperatureF"] = _oilF;
        g["HydraulicOilTemperatureC"] = (int)Math.Round((_oilF - 32) * 5 / 9.0);
        g["ProportionalOutputValue"] = (int)Math.Round(70 + Math.Clamp((psi - 51000) / 6000.0, 0, 1) * 20);
        foreach (var key in HourKeys)
            if (_seed.ContainsKey(key)) g[key] = Int(key) + t / 3600;

        foreach (var (name, value) in events)
        {
            if (!_seed.TryGetValue(name, out var seedValue)) continue;
            g[name] = seedValue.ValueKind is JsonValueKind.True or JsonValueKind.False ? value != 0 : value;
        }

        var reading = new Dictionary<string, JsonElement>(_seed);
        foreach (var (k, v) in g)
            if (reading.ContainsKey(k)) reading[k] = JsonSerializer.SerializeToElement(v);
        return reading;
    }

    private int Walk(int v, int step, int min, int max) => Math.Clamp(v + _rng.Next(-step, step + 1), min, max);

    private int Int(string key) => _seed[key].GetInt32();
}
