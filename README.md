# Shape live polling — validation harness

Local stand-ins for the live polling gateway module, ingestion function and backend API, plus a validation UI,
used to validate the backend design before its contract goes to the frontend team.
Design: `docs/superpowers/specs/2026-10-01-live-polling-validation-harness-design.md` · Findings: `docs/findings.md`
· Reference contracts: `docs/reference/`.

## Prerequisites

- .NET SDK 8.0.425 (`global.json`), PowerShell 7
- Node.js 20+ (`web/`, Azurite): `npm i -g azurite`
- Azure Functions Core Tools 4.x (4.9 tested)
- Azure SignalR emulator: `dotnet tool install -g Microsoft.Azure.SignalR.Emulator`

## Run

```powershell
pwsh -NoProfile -File scripts/run-local.ps1      # everything, one window each; UI at http://localhost:5173
pwsh -NoProfile -File scripts/smoke.ps1          # end-to-end scenarios (starts its own simulator/backend)
```

| Process | Port |
|---|---|
| Gateway simulator (IoT Hub facade, `/sim/*`) | 5100 |
| Backend API | 5080 |
| Functions host (processor, negotiator, cleanup) | 7071 |
| SignalR emulator | 8888 |
| Azurite blob/queue/table | 10100/10101/10102 |
| Validation UI | 5173 |

Simulator flags: `--seed N`, `--alarm-at-start`, `--drop-every N`, `--reorder`, `--duplicate`, `--unreachable`.

## Test

```powershell
dotnet test --filter "Category!=Integration"     # unit tests, no infrastructure
dotnet test                                       # everything; Azurite must be running on 10100–10102
cd web; npm test                                  # frontend unit tests
```
