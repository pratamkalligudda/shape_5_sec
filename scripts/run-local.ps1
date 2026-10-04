<#
.SYNOPSIS  Starts the local stack, each process in its own window.
.PARAMETER Infra  Start only Azurite, the SignalR emulator and the Functions host (smoke.ps1 starts the rest itself).
#>
param([switch]$Infra)

$ErrorActionPreference = 'Stop'
$root = Split-Path $PSScriptRoot -Parent

# Build once, up front: parallel builds from several windows fight over obj/ (CS2012 "being used by another process").
Write-Host 'Building solution...'
dotnet build (Join-Path $root 'ShapeLive.sln') -v q -nologo
if ($LASTEXITCODE -ne 0) { throw 'Build failed; stop any running simulator/backend/func/test processes and retry.' }

function Start-Window([string]$Title, [string]$Dir, [string]$Command) {
    # -NoProfile: a broken user profile must not break the stack.
    Start-Process pwsh -WorkingDirectory $Dir -ArgumentList '-NoProfile', '-NoExit', '-Command', "`$Host.UI.RawUI.WindowTitle = '$Title'; $Command"
}

Start-Window 'azurite' $root 'azurite --silent --location .azurite --blobPort 10100 --queuePort 10101 --tablePort 10102'
Start-Window 'signalr-emulator' $root 'asrs-emulator start'
Start-Window 'functions' (Join-Path $root 'src/Shape.LiveIngestion') 'func start'

if (-not $Infra) {
    Start-Window 'backend' $root 'dotnet run --no-build --project src/Shape.LiveBackend'
    Start-Window 'simulator' $root 'dotnet run --no-build --project src/Shape.Gateway.Simulator -- --seed 42'
    Start-Window 'web' (Join-Path $root 'web') 'npm run dev'
    Write-Host 'Validation UI: http://localhost:5173'
}
