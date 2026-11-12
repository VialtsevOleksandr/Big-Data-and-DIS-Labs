param([string]$RunId = (Get-Date -Format "yyyyMMdd_HHmmss"))

# Частина 2: піднімає Hive, виконує scripts/part2.sh у контейнері й зводить метрики.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

& docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "Не вдалося запустити Hive" }

& docker exec -e RUN_ID=$RunId lab6-hive bash /lab/scripts/part2.sh
if ($LASTEXITCODE -ne 0) { throw "Частина 2 завершилася з помилкою. Логи: evidence\part2\$RunId" }

& python collect_metrics.py "evidence\part2\$RunId"
