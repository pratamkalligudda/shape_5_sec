<#
.SYNOPSIS  End-to-end smoke test. Requires `scripts/run-local.ps1 -Infra` to be running (no simulator on :5100).
#>
$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent
Set-Location $root

dotnet build Shape5Sec.sln -v q | Out-Null
$simDll = Join-Path $root 'src/Shape.GatewaySimulator/bin/Debug/net8.0/Shape.GatewaySimulator.dll'
$client = Join-Path $root 'src/Shape.SmokeClient/bin/Debug/net8.0/Shape.SmokeClient.dll'

function Wait-Url([string]$Url, [string]$Method = 'Get', [int]$TimeoutSec = 90) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try { Invoke-WebRequest -Uri $Url -Method $Method -UseBasicParsing -TimeoutSec 5 | Out-Null; return }
        catch { Start-Sleep -Seconds 1 }
    }
    throw "Timed out waiting for $Url"
}

function Invoke-Scenario([string[]]$SimArgs, [string[]]$ClientArgs) {
    $simLog = [IO.Path]::GetTempFileName()
    $sim = Start-Process dotnet -ArgumentList (@($simDll, '--seed', '42') + $SimArgs) -PassThru -WindowStyle Hidden `
        -RedirectStandardOutput $simLog
    try {
        Wait-Url 'http://localhost:5100/fast-polling/status'
        dotnet $client @ClientArgs
        if ($LASTEXITCODE -ne 0) { throw "Smoke client failed for simulator args '$SimArgs'" }
    }
    finally {
        Stop-Process -Id $sim.Id -ErrorAction SilentlyContinue
    }
    # Every delivery must succeed first time, including stale batches the Function answers with no SignalR message.
    $failures = Select-String -Path $simLog -Pattern 'got HTTP|failed:|timed out|Dropping batch'
    if ($failures) { throw "Simulator reported delivery failures for '$SimArgs':`n$($failures.Line -join "`n")" }
}

Wait-Url 'http://localhost:7071/api/negotiate' -Method Post

Write-Host '== Scenario 1: clean run =='
Invoke-Scenario @() @('--count', '3')

Write-Host '== Scenario 2: dropped batches =='
Invoke-Scenario @('--drop-every', '2') @('--count', '3', '--expect-gap')

Write-Host '== Scenario 3: reordered batches (stale deliveries return no push) =='
Invoke-Scenario @('--reorder') @('--count', '3', '--expect-gap')

Write-Host 'ALL SMOKE SCENARIOS PASSED'
