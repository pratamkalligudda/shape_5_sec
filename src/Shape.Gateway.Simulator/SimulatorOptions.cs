namespace Shape.Gateway.Simulator;

public sealed record SimulatorOptions
{
    public int? RandomSeed { get; init; }
    public int DropEvery { get; init; }
    public bool Reorder { get; init; }
    public bool Duplicate { get; init; }
    public bool AlarmAtStart { get; init; }
    public bool Unreachable { get; init; }
    public Uri FunctionsUrl { get; init; } = EventGridPublisher.DefaultEndpoint;
    public string Urls { get; init; } = "http://localhost:5100";

    public static SimulatorOptions Parse(string[] args)
    {
        var o = new SimulatorOptions();
        for (var i = 0; i < args.Length; i++)
        {
            // --key=value is host configuration (e.g. WebApplicationFactory's --environment=Development); the host reads it.
            if (args[i].StartsWith("--", StringComparison.Ordinal) && args[i].Contains('=')) continue;
            o = args[i] switch
            {
                "--seed" => o with { RandomSeed = Int(args, ++i, "--seed") },
                "--drop-every" => o with { DropEvery = Int(args, ++i, "--drop-every") is >= 2 and var n ? n : throw new ArgumentException("--drop-every must be >= 2.") },
                "--reorder" => o with { Reorder = true },
                "--duplicate" => o with { Duplicate = true },
                "--alarm-at-start" => o with { AlarmAtStart = true },
                "--unreachable" => o with { Unreachable = true },
                "--functions-url" => o with { FunctionsUrl = new Uri(Value(args, ++i, "--functions-url")) },
                "--urls" => o with { Urls = Value(args, ++i, "--urls") },
                _ => throw new ArgumentException($"Unknown argument '{args[i]}'."),
            };
        }
        return o;
    }

    private static string Value(string[] a, int i, string flag) => i < a.Length ? a[i] : throw new ArgumentException($"{flag} needs a value.");
    private static int Int(string[] a, int i, string flag) => int.TryParse(Value(a, i, flag), out var v) ? v : throw new ArgumentException($"{flag} needs an integer.");
}
