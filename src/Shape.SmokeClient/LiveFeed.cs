using System.IO.Compression;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR.Client;

namespace Shape.SmokeClient;

/// <summary>negotiate → connect → join-device, then every liveTelemetryUpdate decoded and contract-checked.</summary>
public sealed class LiveFeed : IAsyncDisposable
{
    public const string UserId = "smoke";
    private static readonly string[] EntryKeys = ["timestampUtc", "deviceStatus", "snapshot", "changes", "events"];

    private readonly HubConnection _connection;
    private readonly Channel<JsonElement> _incoming = Channel.CreateUnbounded<JsonElement>();

    private LiveFeed(HubConnection connection) => _connection = connection;

    public List<string> ContractViolations { get; } = [];

    public static async Task<LiveFeed> ConnectAsync(string negotiatorUrl, string deviceGuid)
    {
        using var http = new HttpClient();
        using var negotiate = new HttpRequestMessage(HttpMethod.Post, $"{negotiatorUrl}/api/negotiate");
        negotiate.Headers.Add("x-user-id", UserId);
        var info = JsonDocument.Parse(await (await http.SendAsync(negotiate)).EnsureSuccessStatusCode().Content.ReadAsStringAsync()).RootElement;
        var token = info.GetProperty("accessToken").GetString();

        var connection = new HubConnectionBuilder()
            .WithUrl(info.GetProperty("url").GetString()!, o => o.AccessTokenProvider = () => Task.FromResult(token))
            .WithAutomaticReconnect()
            .Build();
        var feed = new LiveFeed(connection);
        connection.On<JsonElement>("liveTelemetryUpdate", feed.OnEnvelope);
        await connection.StartAsync();

        using var join = new HttpRequestMessage(HttpMethod.Post, $"{negotiatorUrl}/api/join-device?deviceId={deviceGuid}");
        join.Headers.Add("x-user-id", UserId);
        (await http.SendAsync(join)).EnsureSuccessStatusCode();
        return feed;
    }

    /// <summary>Discards anything already received, so a step only sees batches that follow it.</summary>
    public void Drain()
    {
        while (_incoming.Reader.TryRead(out _)) { }
    }

    public async Task<List<JsonElement>> NextBatchesAsync(int count, TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var batches = new List<JsonElement>();
        try
        {
            while (batches.Count < count) batches.Add(await _incoming.Reader.ReadAsync(cts.Token));
        }
        catch (OperationCanceledException)
        {
            throw new SmokeFailure($"received {batches.Count} of {count} SignalR batches within {timeout.TotalSeconds:0} s");
        }
        return batches;
    }

    public async Task<JsonElement> WaitForEntryAsync(Func<JsonElement, bool> predicate, TimeSpan timeout, string what)
    {
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            while (true)
            {
                var batch = await _incoming.Reader.ReadAsync(cts.Token);
                foreach (var entry in batch.GetProperty("entries").EnumerateArray())
                    if (predicate(entry)) return entry;
            }
        }
        catch (OperationCanceledException)
        {
            throw new SmokeFailure($"no SignalR entry with {what} within {timeout.TotalSeconds:0} s");
        }
    }

    public static JsonElement Decode(JsonElement envelope)
    {
        if (envelope.GetProperty("encoding").GetString() != "gzip+base64")
            throw new SmokeFailure($"unexpected envelope encoding {envelope.GetProperty("encoding")}");
        using var gz = new GZipStream(new MemoryStream(Convert.FromBase64String(envelope.GetProperty("payload").GetString()!)),
            CompressionMode.Decompress);
        return JsonDocument.Parse(gz).RootElement.Clone();
    }

    private void OnEnvelope(JsonElement envelope)
    {
        var batch = Decode(envelope);
        Check(batch);
        _incoming.Writer.TryWrite(batch);
    }

    /// <summary>The frontend contract's per-entry rules (integration doc, "Decoded batch").</summary>
    private void Check(JsonElement batch)
    {
        DateTimeOffset? previous = null;
        var i = 0;
        foreach (var e in batch.GetProperty("entries").EnumerateArray())
        {
            var keys = e.EnumerateObject().Select(p => p.Name).ToArray();
            foreach (var missing in EntryKeys.Except(keys)) ContractViolations.Add($"entry {i}: missing key '{missing}'");
            var hasSnapshot = e.TryGetProperty("snapshot", out var s) && s.ValueKind == JsonValueKind.Object;
            var hasChanges = e.TryGetProperty("changes", out var c) && c.ValueKind == JsonValueKind.Object;
            if (hasSnapshot == hasChanges) ContractViolations.Add($"entry {i}: exactly one of snapshot/changes must be non-null");
            if (i == 0 && !hasSnapshot) ContractViolations.Add("entry 0 has no snapshot");
            if (!e.TryGetProperty("deviceStatus", out var st) || st.ValueKind != JsonValueKind.Number || !st.TryGetInt32(out var sv) || sv is < 0 or > 2)
                ContractViolations.Add($"entry {i}: deviceStatus is not 0/1/2");
            var ts = e.GetProperty("timestampUtc").GetDateTimeOffset();
            if (previous is not null && ts <= previous) ContractViolations.Add($"entry {i}: timestamps not ascending");
            previous = ts;
            i++;
        }
    }

    public ValueTask DisposeAsync() => _connection.DisposeAsync();
}
