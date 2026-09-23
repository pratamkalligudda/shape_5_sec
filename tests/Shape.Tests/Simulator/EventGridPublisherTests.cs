using System.Net;
using System.Text.Json;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.Contracts;
using Shape.GatewaySimulator;

namespace Shape.Tests.Simulator;

public class EventGridPublisherTests
{
    // Captures everything at send time: the publisher disposes the request afterwards.
    public sealed record Captured(HttpMethod Method, Uri? Uri, string? AegEventType, string? MediaType, string Body);

    private sealed class StubHandler(params object[] outcomes) : HttpMessageHandler
    {
        private int _call;
        public List<Captured> Requests { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Requests.Add(new Captured(
                request.Method,
                request.RequestUri,
                request.Headers.TryGetValues("aeg-event-type", out var v) ? v.Single() : null,
                request.Content!.Headers.ContentType?.MediaType,
                await request.Content.ReadAsStringAsync(ct)));
            var outcome = outcomes[Math.Min(_call++, outcomes.Length - 1)];
            if (outcome is Exception ex) throw ex;
            return new HttpResponseMessage((HttpStatusCode)outcome);
        }
    }

    private static readonly Uri Endpoint = new("http://localhost:7071/runtime/webhooks/EventGrid?functionName=IngestTelemetry");

    private static TelemetryBatch Batch(long seq = 0) => new()
    {
        Properties = new BatchProperties { SessionId = "s1", BatchSeq = seq, BatchStartUtc = DateTimeOffset.UnixEpoch },
        Samples = [new Sample { SnapshotTimestampUtc = DateTimeOffset.UnixEpoch, Changes = new() }],
    };

    private static EventGridPublisher Publisher(StubHandler handler, int retries = 3) =>
        new(new HttpClient(handler), Endpoint, Enumerable.Repeat(TimeSpan.Zero, retries).ToArray(),
            NullLogger<EventGridPublisher>.Instance);

    [Fact]
    public async Task Posts_event_grid_array_with_notification_header()
    {
        var handler = new StubHandler(HttpStatusCode.OK);

        var ok = await Publisher(handler).PublishAsync(Batch(4), CancellationToken.None);

        Assert.True(ok);
        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal(Endpoint, request.Uri);
        Assert.Equal("Notification", request.AegEventType);
        Assert.Equal("application/json", request.MediaType);

        using var doc = JsonDocument.Parse(request.Body);
        var ev = Assert.Single(doc.RootElement.EnumerateArray());
        Assert.Equal("Shape.Telemetry.FastPollingBatch", ev.GetProperty("eventType").GetString());
        Assert.Equal("pumps/pump-001", ev.GetProperty("subject").GetString());
        Assert.Equal("1.0", ev.GetProperty("dataVersion").GetString());
        Assert.False(string.IsNullOrEmpty(ev.GetProperty("id").GetString()));
        Assert.True(ev.TryGetProperty("eventTime", out _));
        Assert.Equal(4, ev.GetProperty("data").GetProperty("Properties").GetProperty("batchSeq").GetInt64());
        Assert.Equal("s1", ev.GetProperty("data").GetProperty("Properties").GetProperty("session-id").GetString());
    }

    [Fact]
    public async Task Retries_on_failure_status_then_succeeds()
    {
        var handler = new StubHandler(HttpStatusCode.InternalServerError, HttpStatusCode.OK);

        Assert.True(await Publisher(handler).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Retries_on_connection_error()
    {
        var handler = new StubHandler(new HttpRequestException("refused"), HttpStatusCode.OK);

        Assert.True(await Publisher(handler).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Retries_on_http_timeout()
    {
        // HttpClient reports its own timeout as TaskCanceledException, not HttpRequestException.
        var handler = new StubHandler(new TaskCanceledException("timeout"), HttpStatusCode.OK);

        Assert.True(await Publisher(handler).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(2, handler.Requests.Count);
    }

    [Fact]
    public async Task Gives_up_after_all_retries_and_returns_false()
    {
        var handler = new StubHandler(HttpStatusCode.ServiceUnavailable);

        Assert.False(await Publisher(handler, retries: 3).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(4, handler.Requests.Count); // 1 attempt + 3 retries
    }

    [Fact]
    public void Default_retry_delays_are_1_2_4_seconds()
    {
        Assert.Equal(new[] { TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(4) },
            EventGridPublisher.DefaultRetryDelays);
    }
}
