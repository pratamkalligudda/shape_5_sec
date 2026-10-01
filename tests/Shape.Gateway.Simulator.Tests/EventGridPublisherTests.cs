using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Shape.Gateway.Simulator;
using static Shape.Gateway.Simulator.Tests.TestConfig;

namespace Shape.Gateway.Simulator.Tests;

public class EventGridPublisherTests
{
    public sealed record Captured(string? AegEventType, string? MediaType, string Body);

    private sealed class Stub(params object[] outcomes) : HttpMessageHandler
    {
        private int _n;
        public List<Captured> Requests { get; } = new();

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage r, CancellationToken ct)
        {
            Requests.Add(new Captured(
                r.Headers.TryGetValues("aeg-event-type", out var v) ? v.Single() : null,
                r.Content?.Headers.ContentType?.MediaType,
                await r.Content!.ReadAsStringAsync(ct)));
            var o = outcomes[Math.Min(_n++, outcomes.Length - 1)];
            if (o is Exception ex) throw ex;
            return new HttpResponseMessage((HttpStatusCode)o);
        }
    }

    private static EventGridPublisher P(Stub s) => new(new HttpClient(s), EventGridPublisher.DefaultEndpoint, Device(),
        [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero], NullLogger<EventGridPublisher>.Instance);

    private static OutgoingBatch Batch() => new(Guid.NewGuid(), 0, DateTime.UtcNow,
        [new GatewayEntry { TimestampUtc = DateTime.UtcNow, Changes = new() }]);

    [Fact]
    public async Task Posts_notification_with_json_body()
    {
        var s = new Stub(HttpStatusCode.Accepted);
        Assert.True(await P(s).PublishAsync(Batch(), CancellationToken.None));
        var r = Assert.Single(s.Requests);
        Assert.Equal("Notification", r.AegEventType);
        Assert.Equal("application/json", r.MediaType);
        Assert.Contains("Microsoft.Devices.DeviceTelemetry", r.Body);
    }

    [Fact]
    public async Task Retries_failure_status_connection_error_and_timeout()
    {
        var s = new Stub(HttpStatusCode.InternalServerError, new HttpRequestException("refused"),
            new TaskCanceledException("timeout"), HttpStatusCode.OK);
        Assert.True(await P(s).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(4, s.Requests.Count);
    }

    [Fact]
    public async Task Gives_up_after_three_retries()
    {
        var s = new Stub(HttpStatusCode.ServiceUnavailable);
        Assert.False(await P(s).PublishAsync(Batch(), CancellationToken.None));
        Assert.Equal(4, s.Requests.Count);
    }
}
