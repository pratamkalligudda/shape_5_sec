using System.Text.Json;
using Azure;
using Azure.Data.Tables;

namespace Shape.LiveIngestion.LivePolling;

/// <summary>Entity shapes of spec §5.6. The backend reader (Task 12) decodes the same columns.</summary>
public static class LiveRowMapper
{
    public static string PartitionKey(Guid sessionId) => sessionId.ToString();

    public static TableEntity Head(string pk, LiveHead h)
    {
        var e = new TableEntity(pk, LiveSessionKeys.HeadRowKey)
        {
            ["DeviceGuid"] = h.DeviceGuid,
            ["DeviceId"] = h.DeviceId,
            ["Snapshot"] = JsonSerializer.Serialize(h.Snapshot),
            ["HasBaseline"] = h.HasBaseline,
            ["TickCount"] = h.TickCount,
            ["BytesTotal"] = h.BytesTotal,
        };
        if (h.DeviceStatus is { } s) e["DeviceStatus"] = s;
        if (h.LastSnapshotTimestampUtc is { } l) e["LastSnapshotTimestampUtc"] = l;
        if (h.FirstTickUtc is { } f) e["FirstTickUtc"] = f;
        if (h.LastTickUtc is { } t) e["LastTickUtc"] = t;
        return e;
    }

    public static LiveHead ReadHead(TableEntity e) => new()
    {
        DeviceGuid = e.GetGuid("DeviceGuid") ?? Guid.Empty,
        DeviceId = e.GetString("DeviceId") ?? "",
        Snapshot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(e.GetString("Snapshot") ?? "{}")!,
        DeviceStatus = e.GetInt32("DeviceStatus"),
        LastSnapshotTimestampUtc = e.GetDateTimeOffset("LastSnapshotTimestampUtc"),
        HasBaseline = e.GetBoolean("HasBaseline") ?? false,
        TickCount = e.GetInt64("TickCount") ?? 0,
        BytesTotal = e.GetInt64("BytesTotal") ?? 0,
        FirstTickUtc = e.GetDateTimeOffset("FirstTickUtc"),
        LastTickUtc = e.GetDateTimeOffset("LastTickUtc"),
        ETag = e.ETag == default ? null : e.ETag.ToString(),
    };

    public static TableEntity ChangePoint(string pk, ChangePointWrite c)
    {
        var e = new TableEntity(pk, LiveSessionKeys.ChangeRowKey(c.Parameter, c.TimestampUtc)) { ["TimestampUtc"] = c.TimestampUtc };
        var v = c.Value;
        switch (v.ValueKind)
        {
            case JsonValueKind.True or JsonValueKind.False: e["V"] = v.GetBoolean(); e["T"] = "b"; break;
            case JsonValueKind.Number when v.TryGetInt64(out var i): e["V"] = i; e["T"] = "i"; break;
            case JsonValueKind.Number: e["V"] = v.GetDouble(); e["T"] = "d"; break;
            case JsonValueKind.String: e["V"] = v.GetString(); e["T"] = "s"; break;
            case JsonValueKind.Null: e["T"] = "n"; break;
            default: e["V"] = v.GetRawText(); e["T"] = "s"; break;
        }
        return e;
    }

    public static TableEntity Event(string pk, LiveEventWrite w)
    {
        var ev = w.Event;
        var e = new TableEntity(pk, LiveSessionKeys.EventRowKey(ev.Parameter, w.TimestampUtc))
        {
            ["Parameter"] = ev.Parameter,
            ["Value"] = ev.Value,
            ["State"] = ev.State,
            ["EventType"] = ev.EventType,
            ["Domain"] = ev.Domain,
            ["Severity"] = ev.Severity,
            ["TimestampUtc"] = w.TimestampUtc,
        };
        if (ev.Label is not null) e["Label"] = ev.Label;
        return e;
    }

    internal static ETag ToETag(string etag) => new(etag);
}
