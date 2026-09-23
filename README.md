# Shape 5-sec batched telemetry — local simulation

Gateway simulator → Event Grid–shaped webhook → Azure Function (fold snapshot + deltas, Azurite Table state)
→ Azure SignalR Emulator → React dashboard replaying 1 sample/second.

Design: `docs/superpowers/specs/2026-09-23-5sec-batch-telemetry-design.md`

## Prerequisites

- .NET 8 SDK, Azure Functions Core Tools v4, Node 20+, PowerShell 7
- `npm install -g azurite`
- `dotnet tool install -g Microsoft.Azure.SignalR.Emulator`
- `cd web; npm install`

## Run

```powershell
./scripts/run-local.ps1
```
Open http://localhost:5173 and click **Enable**.

Simulator flags: `--seed N`, `--drop-every N` (N ≥ 2), `--reorder`, `--functions-url URL`.

## Test

```powershell
dotnet test tests/Shape.Tests --filter "Category!=Integration"   # unit
azurite --silent --location .azurite --blobPort 10100 --queuePort 10101 --tablePort 10102   # then, in another shell:
dotnet test tests/Shape.Tests --filter "Category=Integration"     # Azurite-backed
cd web; npm test                                                    # replay queue
./scripts/run-local.ps1 -Infra; ./scripts/smoke.ps1                # end-to-end
```

## Ports

simulator 5100 · Functions 7071 · SignalR emulator 8888 · Azurite 10100–10102 (10000/10002 are taken by SCADA services on the dev machine) · Vite 5173
