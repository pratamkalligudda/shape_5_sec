using System.Net.Http.Json;
using System.Text.Json;
using Azure.Data.Tables;
using Microsoft.Data.Sqlite;

namespace Shape.SmokeClient;

/// <summary>Spec §10 end-to-end list. Each scenario throws SmokeFailure with the first broken expectation.</summary>
public sealed class Scenarios : IAsyncDisposable
{
    public static readonly string[] DefaultOrder = ["basic", "events", "extend", "errors", "stop"];

    private readonly SmokeConfig _c;
    private readonly LiveFeed _feed;
    private readonly HttpClient _http = new() { Timeout = TimeSpan.FromSeconds(60) };

    private Scenarios(SmokeConfig c, LiveFeed feed)
    {
        _c = c;
        _feed = feed;
    }

    public static async Task<Scenarios> CreateAsync(SmokeConfig c) => new(c, await LiveFeed.ConnectAsync(c.NegotiatorUrl, c.DeviceGuid));

    private string Base => $"{_c.BackendUrl}/api/v1/devices/{_c.Serial}/live-session";

    public Task RunAsync(string name) => name switch
    {
        "basic" => Basic(),
        "events" => Events(),
        "extend" => Extend(),
        "errors" => Errors(),
        "stop" => Stop(),
        "alarm-at-start" => AlarmAtStart(),
        "faults" => Faults(),
        "unreachable" => Unreachable(),
        _ => throw new ArgumentException($"Unknown scenario '{name}'."),
    };

    // 1. Start → 3 batches → GET snapshot + tickCount ≥ 15 → /series grid matches ticks.
    private async Task Basic()
    {
        var sessionId = await EnsureSessionAsync();
        var batches = await _feed.NextBatchesAsync(3, TimeSpan.FromSeconds(45));
        Check(batches.All(b => b.GetProperty("sessionId").GetString() == sessionId), "a SignalR batch carried another sessionId");
        Check(batches.All(b => b.GetProperty("deviceGuid").GetString() == _c.DeviceGuid), "a SignalR batch carried another deviceGuid");

        var status = Data(await Expect(HttpMethod.Get, Base, 200));
        Check(status.GetProperty("snapshot").ValueKind == JsonValueKind.Object, "GET snapshot is null after 3 batches");
        Check(status.GetProperty("deviceStatus").ValueKind == JsonValueKind.Number, "GET deviceStatus is not a number");
        Check(status.GetProperty("tickCount").GetInt64() >= 15, $"tickCount {status.GetProperty("tickCount")} < 15");

        var series = Data(await Expect(HttpMethod.Get, Base + "/series", 200));
        var t = series.GetProperty("t").GetArrayLength();
        var ticks = series.GetProperty("tickCount").GetInt64();
        Check(t == ticks || t == ticks - 1, $"grid length {t} vs tickCount {ticks} (F4 allows one short)");
        Check(series.GetProperty("status").GetArrayLength() == t, "status[] length differs from t[]");
        foreach (var s in series.GetProperty("series").EnumerateArray())
            Check(s.GetProperty("v").GetArrayLength() == t, $"{s.GetProperty("metric")} length differs from t[]");
        Check(_feed.ContractViolations.Count == 0, "contract: " + string.Join("; ", _feed.ContractViolations));
    }

    // 2. Alarm 5 → triggered with label on SignalR and /series, status 2; alarm 0 → cleared, status 0.
    private async Task Events()
    {
        await EnsureSessionAsync();
        _feed.Drain();

        await SimAsync("/sim/events", new Dictionary<string, int> { ["OperatingAlarmActive"] = 5 });
        var on = await _feed.WaitForEntryAsync(e => HasEvent(e, "OperatingAlarmActive", "triggered"), TimeSpan.FromSeconds(30), "an alarm trigger");
        var trigger = EventOf(on, "OperatingAlarmActive");
        Check(trigger.GetProperty("label").GetString() == "Emergency Stop Active", $"label was {trigger.GetProperty("label")}");
        Check(trigger.GetProperty("value").GetInt64() == 5, "value was not 5");
        Check(on.GetProperty("deviceStatus").GetInt32() == 2, $"deviceStatus {on.GetProperty("deviceStatus")} on the trigger tick");
        Check(SeriesEvents(Data(await Expect(HttpMethod.Get, Base + "/series", 200)))
            .Any(e => Is(e, "OperatingAlarmActive", "triggered") && e.GetProperty("label").GetString() == "Emergency Stop Active"),
            "/series has no labelled trigger");

        await SimAsync("/sim/events", new Dictionary<string, int> { ["OperatingAlarmActive"] = 0 });
        var off = await _feed.WaitForEntryAsync(e => HasEvent(e, "OperatingAlarmActive", "cleared"), TimeSpan.FromSeconds(30), "an alarm clear");
        Check(EventOf(off, "OperatingAlarmActive").GetProperty("label").ValueKind == JsonValueKind.Null, "label on cleared is not null");
        Check(off.GetProperty("deviceStatus").GetInt32() == 0, $"deviceStatus {off.GetProperty("deviceStatus")} after clear");
        Check(SeriesEvents(Data(await Expect(HttpMethod.Get, Base + "/series", 200))).Any(e => Is(e, "OperatingAlarmActive", "cleared")),
            "/series has no clear");
    }

    // 3. Extend → +20; again → 30 and canExtend false; again → 400 EXTENSION_LIMIT_REACHED.
    private async Task Extend()
    {
        await EnsureSessionAsync();
        var first = Data(await Expect(HttpMethod.Post, Base + "/extend", 200));
        var started = first.GetProperty("startedUtc").GetDateTimeOffset();
        Check(first.GetProperty("expiresUtc").GetDateTimeOffset() == started.AddMinutes(20), "first extend: expiresUtc ≠ startedUtc + 20 min");
        Check(first.GetProperty("canExtend").GetBoolean(), "first extend: canExtend false");

        var second = Data(await Expect(HttpMethod.Post, Base + "/extend", 200));
        Check(second.GetProperty("totalDurationMinutes").GetInt32() == 30, "second extend: total ≠ 30");
        Check(!second.GetProperty("canExtend").GetBoolean(), "second extend: canExtend still true");

        var third = await Expect(HttpMethod.Post, Base + "/extend", 400, "LIVE_SESSION_EXTENSION_LIMIT_REACHED");
        Check(third.GetProperty("errors").GetArrayLength() >= 2, "third extend: no module reason in errors[1]");
    }

    // 7a. Second start while active → 409; unknown device → 404; bad metric → 400.
    private async Task Errors()
    {
        await EnsureSessionAsync();
        await Expect(HttpMethod.Post, Base, 409, "LIVE_SESSION_ALREADY_ACTIVE");
        await Expect(HttpMethod.Get, $"{_c.BackendUrl}/api/v1/devices/NO-SUCH-PUMP/live-session", 404, "DEVICE_NOT_FOUND");
        await Expect(HttpMethod.Get, Base + "/series?metric=OperatingPressurePSI,StrokeRateLeft", 400, "LIVE_SESSION_INVALID_PARAMETERS");
    }

    // 4. Stop → /series 409 → within two timer runs the partition is gone and the row has PurgedUtc + counters.
    private async Task Stop()
    {
        var sessionId = await EnsureSessionAsync();
        var stopped = await Expect(HttpMethod.Delete, Base, 200);
        Check(stopped.GetProperty("message").GetString() == "Live session stopped.", "stop message");
        await Expect(HttpMethod.Get, Base + "/series", 409, "LIVE_SESSION_NOT_ACTIVE");
        Check(!Data(await Expect(HttpMethod.Get, Base, 200)).GetProperty("active").GetBoolean(), "GET still active after stop");

        var deadline = DateTime.UtcNow.AddSeconds(_c.CleanupWaitSeconds);
        (string? Purged, long? Ticks, long? Bytes) row;
        while (true)
        {
            row = ReadRow(sessionId);
            if (row.Purged is not null || DateTime.UtcNow > deadline) break;
            await Task.Delay(2000);
        }
        Check(row.Purged is not null, $"PurgedUtc still null after {_c.CleanupWaitSeconds} s");
        Check(row.Ticks > 0 && row.Bytes > 0, $"counters not harvested (TickCount {row.Ticks}, BytesTotal {row.Bytes})");
        await Task.Delay(5000);   // a late partial-batch flush must not resurrect the partition (finding F6 if it does)
        Check(await PartitionRowsAsync(sessionId) == 0, "the partition still has rows after purge");
    }

    // 5. --alarm-at-start → status[0] = 2 and one active OperatingAlarmActive event.
    private async Task AlarmAtStart()
    {
        _feed.Drain();
        await EnsureSessionAsync();
        await _feed.NextBatchesAsync(1, TimeSpan.FromSeconds(30));

        var series = Data(await Expect(HttpMethod.Get, Base + "/series", 200));
        Check(series.GetProperty("status")[0].GetInt32() == 2, $"status[0] = {series.GetProperty("status")[0]}");
        var active = SeriesEvents(series).Where(e => Is(e, "OperatingAlarmActive", "active")).ToList();
        Check(active.Count == 1, $"{active.Count} active OperatingAlarmActive events");
        Check(active[0].GetProperty("label").GetString() == "Emergency Stop Active", "active event label");
        await Expect(HttpMethod.Delete, Base, 200);
    }

    // 6. --duplicate --reorder → no duplicate entries or events; grid evenly spaced.
    private async Task Faults()
    {
        _feed.Drain();
        await EnsureSessionAsync();
        var batches = await _feed.NextBatchesAsync(4, TimeSpan.FromSeconds(60));

        var stamps = batches.SelectMany(b => b.GetProperty("entries").EnumerateArray())
            .Select(e => e.GetProperty("timestampUtc").GetDateTimeOffset()).ToList();
        Check(stamps.Zip(stamps.Skip(1)).All(p => p.First < p.Second), "broadcast entries repeat or go backwards");

        var series = Data(await Expect(HttpMethod.Get, Base + "/series", 200));
        var t = series.GetProperty("t").EnumerateArray().Select(x => x.GetDateTimeOffset()).ToList();
        var interval = TimeSpan.FromSeconds(series.GetProperty("pollIntervalSeconds").GetInt32());
        Check(t.Zip(t.Skip(1)).All(p => p.Second - p.First == interval), "grid is not evenly spaced");
        var events = SeriesEvents(series).Select(e => (e.GetProperty("timestampUtc").GetString(), e.GetProperty("parameter").GetString())).ToList();
        Check(events.Count == events.Distinct().Count(), "duplicate event rows");
        Console.Write($"[grid {t.Count}, tickCount {series.GetProperty("tickCount")}] ");   // reordered batches are skipped: see F7
        await Expect(HttpMethod.Delete, Base, 200);
    }

    // 7b. --unreachable → 503 GATEWAY_UNREACHABLE and nothing recorded.
    private async Task Unreachable()
    {
        await Expect(HttpMethod.Post, Base, 503, "GATEWAY_UNREACHABLE");
        Check(!Data(await Expect(HttpMethod.Get, Base, 200)).GetProperty("active").GetBoolean(), "a session was recorded");
    }

    // ---- helpers ----

    private async Task<string> EnsureSessionAsync()
    {
        var status = Data(await Expect(HttpMethod.Get, Base, 200));
        if (status.GetProperty("active").GetBoolean()) return status.GetProperty("sessionId").GetString()!;
        var started = Data(await Expect(HttpMethod.Post, Base, 200, body: new { pollIntervalSeconds = 1, sessionDurationMinutes = 10 }));
        return started.GetProperty("sessionId").GetString()!;
    }

    private async Task<JsonElement> Expect(HttpMethod method, string url, int status, string? code = null, object? body = null)
    {
        using var request = new HttpRequestMessage(method, url);
        if (body is not null) request.Content = JsonContent.Create(body);
        using var response = await _http.SendAsync(request);
        var text = await response.Content.ReadAsStringAsync();
        Check((int)response.StatusCode == status, $"{method} {url} → {(int)response.StatusCode}, expected {status}: {text}");
        var json = JsonDocument.Parse(text).RootElement.Clone();
        if (code is not null)
            Check(json.GetProperty("errors")[0].GetString() == code, $"{method} {url} → errors[0] {json.GetProperty("errors")[0]}, expected {code}");
        return json;
    }

    /// <summary>Dictionary keys are sent verbatim; an anonymous object would be camelCased by PostAsJsonAsync.</summary>
    private async Task SimAsync(string path, Dictionary<string, int> body) =>
        (await _http.PostAsJsonAsync(_c.SimulatorUrl + path, body)).EnsureSuccessStatusCode();

    private (string? Purged, long? Ticks, long? Bytes) ReadRow(string sessionId)
    {
        using var c = new SqliteConnection($"Data Source={_c.DbPath};Mode=ReadOnly");
        c.Open();
        using var cmd = c.CreateCommand();
        cmd.CommandText = "SELECT PurgedUtc, TickCount, BytesTotal FROM LivePollingSession WHERE SessionId = $id";
        cmd.Parameters.AddWithValue("$id", sessionId);
        using var r = cmd.ExecuteReader();
        if (!r.Read()) throw new SmokeFailure($"no SQLite row for session {sessionId}");
        return (r.IsDBNull(0) ? null : r.GetString(0), r.IsDBNull(1) ? null : r.GetInt64(1), r.IsDBNull(2) ? null : r.GetInt64(2));
    }

    private async Task<int> PartitionRowsAsync(string sessionId)
    {
        var table = new TableServiceClient(_c.TableConnectionString).GetTableClient("livesessions");
        var n = 0;
        await foreach (var _ in table.QueryAsync<TableEntity>(TableClient.CreateQueryFilter($"PartitionKey eq {sessionId}"), select: ["RowKey"]))
            n++;
        return n;
    }

    private static JsonElement Data(JsonElement body) => body.GetProperty("data");

    private static IEnumerable<JsonElement> SeriesEvents(JsonElement series) => series.GetProperty("events").EnumerateArray();

    private static bool Is(JsonElement e, string parameter, string state) =>
        e.GetProperty("parameter").GetString() == parameter && e.GetProperty("state").GetString() == state;

    private static bool HasEvent(JsonElement entry, string parameter, string state) =>
        entry.GetProperty("events") is { ValueKind: JsonValueKind.Array } events && events.EnumerateArray().Any(e => Is(e, parameter, state));

    private static JsonElement EventOf(JsonElement entry, string parameter) =>
        entry.GetProperty("events").EnumerateArray().First(e => e.GetProperty("parameter").GetString() == parameter);

    private static void Check(bool ok, string message)
    {
        if (!ok) throw new SmokeFailure(message);
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _feed.DisposeAsync();
    }
}
