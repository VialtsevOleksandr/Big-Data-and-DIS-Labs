param([string]$RunId = (Get-Date -Format "yyyyMMdd_HHmmss"))

# Частина 4: піднімає Hive, виконує scripts/part4.sh у контейнері й зводить метрики.
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

& docker compose up -d
if ($LASTEXITCODE -ne 0) { throw "Не вдалося запустити Hive" }

& docker exec -e RUN_ID=$RunId lab6-hive bash /lab/scripts/part4.sh
if ($LASTEXITCODE -ne 0) { throw "Частина 4 завершилася з помилкою. Логи: evidence\part4\$RunId" }

& python collect_metrics.py "evidence\part4\$RunId"
