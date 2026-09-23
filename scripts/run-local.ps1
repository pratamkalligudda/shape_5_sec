<#
.SYNOPSIS  Starts the local stack, each process in its own window.
.PARAMETER Infra  Start only Azurite, the SignalR emulator and the Functions host (used by smoke.ps1).
#>
param([switch]$Infra)

$root = Split-Path $PSScriptRoot -Parent

function Start-Window([string]$Title, [string]$Dir, [string]$Command) {
    Start-Process pwsh -WorkingDirectory $Dir -ArgumentList '-NoExit', '-Command', "`$Host.UI.RawUI.WindowTitle = '$Title'; $Command"
}

Start-Window 'azurite' $root 'azurite --silent --location .azurite --blobPort 10100 --queuePort 10101 --tablePort 10102'
Start-Window 'signalr-emulator' $root 'asrs-emulator start'
Start-Window 'functions' (Join-Path $root 'src/Shape.Functions') 'func start'

if (-not $Infra) {
    Start-Window 'simulator' $root 'dotnet run --project src/Shape.GatewaySimulator -- --seed 42'
    Start-Window 'web' (Join-Path $root 'web') 'npm run dev'
    Write-Host 'Dashboard: http://localhost:5173  (click Enable to start a session)'
}
