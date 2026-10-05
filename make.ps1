#!/usr/bin/env pwsh
# PowerShell stand-in for `make`, since Windows has no make by default.
#   ./make.ps1 up      ./make.ps1 test     ./make.ps1 evaluate
# Targets match the Makefile one-for-one.
param([Parameter(Position = 0)][string]$Target = "help")

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
$DevImage = "pfs-dev"

function Build-Dev {
    docker build -q -f "$Root/Dockerfile.dev" -t $DevImage $Root | Out-Null
}
# Mounts the repo into the dev image and runs a command inside it.
function Invoke-Dev {
    param([string[]]$Cmd, [string[]]$ExtraArgs = @())
    Build-Dev
    docker run --rm @ExtraArgs -v "${Root}:/w" -w /w $DevImage @Cmd
}

switch ($Target) {
    "up" {
        docker compose up -d
        Write-Host ""
        Write-Host "Kafka UI   http://localhost:8082"
        Write-Host "Flink UI   http://localhost:8081"
        Write-Host "Grafana    http://localhost:3000  (admin/admin)"
    }
    "stop"     { docker compose stop }
    "start"    {
        docker compose start
        Write-Host ""
        Write-Host "Kafka UI   http://localhost:8082"
        Write-Host "Flink UI   http://localhost:8081"
        Write-Host "Grafana    http://localhost:3000  (admin/admin)"
    }
    # Removes containers and the network. Data survives in named volumes, so
    # `up` restores the alert history. Use `stop` to merely pause.
    "down"     { docker compose down }
    "logs"     { docker compose logs -f --tail=100 }
    "ps"       { docker compose ps -a }
    "produce"  {
        docker compose up -d --force-recreate producer
        docker compose logs -f --tail=50 producer
    }
    "test"     { Invoke-Dev -Cmd @("python", "-m", "pytest") }
    "lint"     { Invoke-Dev -Cmd @("ruff", "check", ".") }
    "evaluate" {
        Invoke-Dev -Cmd @("python", "scripts/evaluate.py") `
                   -ExtraArgs @("--network", "payments-fraud-streaming_default")
    }
    "clean"    { docker compose down -v }
    default {
        Write-Host "Targets: up stop start down logs ps produce test lint evaluate clean"
        Write-Host ""
        Write-Host "  stop  pauses containers      start  resumes them"
        Write-Host "  down  removes containers, KEEPS data (up restores it)"
        Write-Host "  clean removes containers AND deletes data"
    }
}
