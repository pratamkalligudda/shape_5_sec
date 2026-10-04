using Shape.LiveIngestion.LivePolling;

namespace Shape.LiveIngestion.Tests;

public class LiveEventGridDecoderTests
{
    private const string Entries = """
    [ { "TimestampUtc": "2026-10-01T10:00:01Z", "Body": { "A": 1, "B": true }, "Event": { "OperatingAlarmActive": 0 } },
      { "TimestampUtc": "2026-10-01T10:00:02Z", "Changes": { "A": 2 } },
      { "TimestampUtc": "2026-10-01T10:00:03Z", "Changes": {}, "Event": { "OperatingAlarmActive": 5 } } ]
    """;

    [Fact]
    public void Decodes_a_fast_polling_batch()
    {
        var r = LiveEventGridDecoder.Decode(TestEvents.Build(Entries));

        Assert.Equal(DecodeStatus.Ok, r.Status);
        var b = r.Batch!;
        Assert.Equal(TestEvents.Session, b.SessionId);
        Assert.Equal("gw-001", b.GatewayId);
        Assert.Equal(DateTimeOffset.Parse("2026-10-01T10:00:05Z"), b.BatchSentUtc);
        Assert.True(b.CompressedBytes > 0);
        Assert.Equal(3, b.Entries.Count);
        Assert.Equal(1, b.Entries[0].Body!["A"].GetInt32());
        Assert.Equal(0, b.Entries[0].Event!["OperatingAlarmActive"].GetInt32());
        Assert.Null(b.Entries[1].Event);
        Assert.Empty(b.Entries[2].Changes!);
        Assert.Equal(5, b.Entries[2].Event!["OperatingAlarmActive"].GetInt32());
    }

    [Fact]
    public void Accepts_an_event_array_with_one_event()
    {
        Assert.Equal(DecodeStatus.Ok, LiveEventGridDecoder.Decode($"[{TestEvents.Build(Entries)}]").Status);
    }

    [Fact]
    public void Other_telemetry_types_are_not_fast_polling()
    {
        Assert.Equal(DecodeStatus.NotFastPolling, LiveEventGridDecoder.Decode(TestEvents.Build(Entries, telemetryType: "regular")).Status);
    }

    [Fact]
    public void Event_that_is_not_an_object_is_ignored_but_entry_kept()
    {
        var r = LiveEventGridDecoder.Decode(TestEvents.Build("""[{ "TimestampUtc": "2026-10-01T10:00:01Z", "Changes": { "A": 1 }, "Event": [1,2] }]"""));
        Assert.Equal(DecodeStatus.Ok, r.Status);
        Assert.Null(r.Batch!.Entries[0].Event);
    }

    [Theory]
    [InlineData("not base64!!")]
    [InlineData("aGVsbG8=")] // valid base64, not gzip
    public void Corrupt_body_is_invalid(string body)
    {
        var r = LiveEventGridDecoder.Decode(TestEvents.Build("[]", rawBody: body));
        Assert.Equal(DecodeStatus.Invalid, r.Status);
        Assert.False(string.IsNullOrEmpty(r.Reason));
    }

    [Theory]
    [InlineData("""[{ "TimestampUtc": "2026-10-01T10:00:01Z" }]""")]                                         // neither
    [InlineData("""[{ "TimestampUtc": "2026-10-01T10:00:01Z", "Body": {}, "Changes": {} }]""")]             // both
    [InlineData("""[{ "Changes": {} }]""")]                                                                  // no timestamp
    [InlineData("""[]""")]                                                                                   // empty
    public void Malformed_entries_are_invalid(string entries)
    {
        Assert.Equal(DecodeStatus.Invalid, LiveEventGridDecoder.Decode(TestEvents.Build(entries)).Status);
    }

    [Fact]
    public void Garbage_event_json_is_invalid()
    {
        Assert.Equal(DecodeStatus.Invalid, LiveEventGridDecoder.Decode("{not json").Status);
    }
}
