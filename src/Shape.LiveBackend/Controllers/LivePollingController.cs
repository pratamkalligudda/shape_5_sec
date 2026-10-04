using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.ModelBinding;
using Shape.LiveBackend.Api;
using Shape.LiveBackend.Sessions;

namespace Shape.LiveBackend.Controllers;

[ApiController]
[Route("api/v1/devices/{serialNumber}/live-session")]
public sealed class LivePollingController(LiveSessionService service) : ControllerBase
{
    [HttpPost]
    public async Task<IActionResult> Start(string serialNumber,
        [FromBody(EmptyBodyBehavior = EmptyBodyBehavior.Allow)] StartLiveSessionRequest? request, CancellationToken ct) =>
        From(await service.StartAsync(serialNumber, request, ct));

    [HttpGet]
    public async Task<IActionResult> Status(string serialNumber, CancellationToken ct) =>
        From(await service.GetStatusAsync(serialNumber, ct));

    [HttpPost("extend")]
    public async Task<IActionResult> Extend(string serialNumber, CancellationToken ct) =>
        From(await service.ExtendAsync(serialNumber, ct));

    [HttpDelete]
    public async Task<IActionResult> Stop(string serialNumber, CancellationToken ct) =>
        From(await service.StopAsync(serialNumber, ct));

    private static ObjectResult From(ServiceResult r) => new(r.Body) { StatusCode = r.StatusCode };
}
