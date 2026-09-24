<#
.SYNOPSIS  Starts the local stack, each process in its own window.
.PARAMETER Infra  Start only Azurite, the SignalR emulator and the Functions host (used by smoke.ps1).
#>
param([switch]$Infra)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

# Build once, up front. Letting `func start` and `dotnet run` build in parallel windows makes them fight over
# the shared Shape.Contracts.dll (CS2012 "being used by another process").
Write-Host 'Building solution...'
dotnet build (Join-Path $root 'Shape5Sec.sln') -v q -nologo
if ($LASTEXITCODE -ne 0) { throw 'Build failed; stop any running simulator/func/test processes and retry.' }

function Start-Window([string]$Title, [string]$Dir, [string]$Command) {
    # -NoProfile: a broken user profile must not break the stack.
    Start-Process pwsh -WorkingDirectory $Dir -ArgumentList '-NoProfile', '-NoExit', '-Command', "`$Host.UI.RawUI.WindowTitle = '$Title'; $Command"
}

Start-Window 'azurite' $root 'azurite --silent --location .azurite --blobPort 10100 --queuePort 10101 --tablePort 10102'
Start-Window 'signalr-emulator' $root 'asrs-emulator start'
Start-Window 'functions' (Join-Path $root 'src/Shape.Functions') 'func start'

if (-not $Infra) {
    Start-Window 'simulator' $root 'dotnet run --no-build --project src/Shape.GatewaySimulator -- --seed 42'
    Start-Window 'web' (Join-Path $root 'web') 'npm run dev'
    Write-Host 'Dashboard: http://localhost:5173  (click Enable to start a session)'
}
