using System.Diagnostics;
using Shape.SmokeClient;

var names = args.Length > 0 ? args : Scenarios.DefaultOrder;
await using var smoke = await Scenarios.CreateAsync(SmokeConfig.Default());

var failed = 0;
foreach (var name in names)
{
    Console.Write($"{name,-16}");
    var clock = Stopwatch.StartNew();
    try
    {
        await smoke.RunAsync(name);
        Console.WriteLine($"PASS  {clock.Elapsed.TotalSeconds,5:0.0} s");
    }
    catch (Exception ex)
    {
        failed++;
        Console.WriteLine($"FAIL  {clock.Elapsed.TotalSeconds,5:0.0} s{Environment.NewLine}    {ex.Message}");
    }
}
return failed == 0 ? 0 : 1;
