<#
.SYNOPSIS  End-to-end smoke run (spec §10): REST + negotiator + SignalR against the running stack.
.PARAMETER SkipInfra  Azurite, the SignalR emulator and the Functions host are already running.
#>
param([switch]$SkipInfra)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

if ($SkipInfra) {
    dotnet build (Join-Path $root 'ShapeLive.sln') -v q -nologo
    if ($LASTEXITCODE -ne 0) { throw 'Build failed.' }
} else {
    & (Join-Path $PSScriptRoot 'run-local.ps1') -Infra
}

function Wait-Port([int]$Port, [int]$Seconds = 90, [switch]$Free) {
    $deadline = (Get-Date).AddSeconds($Seconds)
    while ((Get-Date) -lt $deadline) {
        $client = [System.Net.Sockets.TcpClient]::new()
        try { $open = $client.ConnectAsync('127.0.0.1', $Port).Wait(500) } catch { $open = $false } finally { $client.Dispose() }
        if ($open -ne [bool]$Free) { return }
        Start-Sleep -Milliseconds 500
    }
    throw "Port $Port did not become $(if ($Free) { 'free' } else { 'ready' }) within $Seconds s."
}

# Start the built DLLs directly: killing `dotnet run` leaves its child process holding the port.
function Start-App([string]$Project, [string[]]$AppArgs) {
    $bin = Join-Path $root "src/$Project/bin/Debug/net8.0"
    Start-Process dotnet -WorkingDirectory $bin -ArgumentList (@("$Project.dll") + $AppArgs) -PassThru -WindowStyle Minimized
}

Wait-Port 10102; Wait-Port 8888; Wait-Port 7071
$backend = Start-App 'Shape.LiveBackend' @()
Wait-Port 5080

$groups = @(
    @{ Flags = @('--seed', '42');                              Scenarios = @('basic', 'events', 'extend', 'errors', 'stop') },
    @{ Flags = @('--seed', '42', '--alarm-at-start');          Scenarios = @('alarm-at-start') },
    @{ Flags = @('--seed', '42', '--duplicate', '--reorder');  Scenarios = @('faults') },
    @{ Flags = @('--seed', '42', '--unreachable');             Scenarios = @('unreachable') }
)

$failed = 0
try {
    foreach ($g in $groups) {
        Write-Host "`n== simulator $($g.Flags -join ' ')"
        $sim = Start-App 'Shape.Gateway.Simulator' $g.Flags
        try {
            Wait-Port 5100
            dotnet run --no-build --project (Join-Path $root 'src/Shape.SmokeClient') -- @($g.Scenarios)
            if ($LASTEXITCODE -ne 0) { $failed++ }
        } finally {
            Stop-Process -Id $sim.Id -Force
            Wait-Port 5100 -Free
        }
    }
} finally {
    Stop-Process -Id $backend.Id -Force
}

if ($failed -gt 0) { Write-Host "`n$failed group(s) failed" -ForegroundColor Red; exit 1 }
Write-Host "`nAll smoke scenarios passed" -ForegroundColor Green
