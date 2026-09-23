namespace Shape.GatewaySimulator;

public sealed record SimulatorOptions
{
    public static readonly Uri DefaultFunctionsUrl =
        new("http://localhost:7071/runtime/webhooks/EventGrid?functionName=IngestTelemetry");

    public int? RandomSeed { get; init; }
    /// <summary>0 = off; N ≥ 2 = drop every Nth batch.</summary>
    public int DropEvery { get; init; }
    public bool Reorder { get; init; }
    public Uri FunctionsUrl { get; init; } = DefaultFunctionsUrl;
    public string Urls { get; init; } = "http://localhost:5100";

    public static SimulatorOptions Parse(string[] args)
    {
        var o = new SimulatorOptions();
        for (var i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--seed": o = o with { RandomSeed = Int(args, ++i, "--seed") }; break;
                case "--drop-every":
                    var n = Int(args, ++i, "--drop-every");
                    if (n < 2) throw new ArgumentException("--drop-every must be >= 2.");
                    o = o with { DropEvery = n };
                    break;
                case "--reorder": o = o with { Reorder = true }; break;
                case "--functions-url": o = o with { FunctionsUrl = new Uri(Value(args, ++i, "--functions-url")) }; break;
                case "--urls": o = o with { Urls = Value(args, ++i, "--urls") }; break;
                default: throw new ArgumentException($"Unknown argument '{args[i]}'.");
            }
        }
        return o;
    }

    private static string Value(string[] args, int i, string flag) =>
        i < args.Length ? args[i] : throw new ArgumentException($"{flag} needs a value.");

    private static int Int(string[] args, int i, string flag) =>
        int.TryParse(Value(args, i, flag), out var v) ? v : throw new ArgumentException($"{flag} needs an integer.");
}
