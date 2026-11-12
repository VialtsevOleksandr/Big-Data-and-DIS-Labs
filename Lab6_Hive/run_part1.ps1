param([string]$RunId = (Get-Date -Format "yyyyMMdd_HHmmss"))

# Частина 1: піднімає Hive, виконує scripts/part1.sh у контейнері й зводить метрики.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

foreach ($file in "data\transactions.csv", "data\late_day.csv") {
    if (-not (Test-Path -LiteralPath $file)) { throw "Немає $file — спочатку запустіть генератор" }
}

& docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "Не вдалося запустити Hive" }

& docker exec -e RUN_ID=$RunId lab6-hive bash /lab/scripts/part1.sh
if ($LASTEXITCODE -ne 0) { throw "Частина 1 завершилася з помилкою. Логи: evidence\part1\$RunId" }

& python collect_metrics.py "evidence\part1\$RunId"
