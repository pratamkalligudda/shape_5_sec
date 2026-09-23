using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR.Client;
using Shape.Contracts;

var functions = Arg("--functions") ?? "http://localhost:7071";
var simulator = Arg("--simulator") ?? "http://localhost:5100";
var count = int.Parse(Arg("--count") ?? "3");
var expectGap = args.Contains("--expect-gap");
var seedKeys = PumpSeed.LoadDefault().Keys.ToHashSet();

var received = Channel.CreateUnbounded<FoldedBatch>();
await using var hub = new HubConnectionBuilder().WithUrl($"{functions}/api").Build();
hub.On<FoldedBatch>("batch", b => received.Writer.TryWrite(b));
await hub.StartAsync();
Console.WriteLine($"Connected to hub via {functions}/api/negotiate");

using var http = new HttpClient();
var enable = await http.PostAsync($"{simulator}/fast-polling/enable", new StringContent("{}", Encoding.UTF8, "application/json"));
if (!enable.IsSuccessStatusCode) return Fail($"enable returned {(int)enable.StatusCode}: {await enable.Content.ReadAsStringAsync()}");
var sessionId = (await enable.Content.ReadFromJsonAsync<JsonElement>()).GetProperty("sessionId").GetString()!;
Console.WriteLine($"Session {sessionId} enabled; waiting for {count} batches");

var batches = new List<FoldedBatch>();
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(count * 5 + 30));
try
{
    while (batches.Count < count)
    {
        var batch = await received.Reader.ReadAsync(timeout.Token);
        if (batch.SessionId != sessionId) continue;
        batches.Add(batch);
        Console.WriteLine($"  batch {batch.BatchSeq}: {batch.Samples.Count} samples, gap={batch.Gap}, partial={batch.Partial}");
    }
}
catch (OperationCanceledException)
{
    return Fail($"timed out with {batches.Count}/{count} batches");
}
finally
{
    await http.PostAsync($"{simulator}/fast-polling/disable", null);
}

if (expectGap)
{
    if (!batches.Any(b => b.Gap)) return Fail("expected at least one batch with gap=true");
}
else
{
    var seqs = batches.Select(b => b.BatchSeq).ToList();
    if (!seqs.SequenceEqual(Enumerable.Range(0, count).Select(i => (long)i))) return Fail($"unexpected batchSeq order {string.Join(",", seqs)}");
    if (batches.Any(b => b.Gap || b.Partial)) return Fail("unexpected gap/partial flag");
    if (batches.Any(b => b.Samples.Count != 5)) return Fail("expected 5 samples per batch");
    var bad = batches.SelectMany(b => b.Samples).FirstOrDefault(s => !s.Values.Keys.ToHashSet().SetEquals(seedKeys));
    if (bad is not null) return Fail($"sample at {bad.Ts} does not carry all {seedKeys.Count} fields");
}

Console.WriteLine("SMOKE PASS");
return 0;

string? Arg(string name)
{
    var i = Array.IndexOf(args, name);
    return i >= 0 && i + 1 < args.Length ? args[i + 1] : null;
}

static int Fail(string message)
{
    Console.Error.WriteLine($"SMOKE FAIL: {message}");
    return 1;
}
