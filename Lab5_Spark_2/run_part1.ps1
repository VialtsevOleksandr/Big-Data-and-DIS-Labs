param()

$ErrorActionPreference = "Stop"
$composeFile = Join-Path $PSScriptRoot "..\Lab5_Spark_1\docker-compose.yml"
$inputFile = Join-Path $PSScriptRoot "data\transactions.csv"
if (-not (Test-Path -LiteralPath $inputFile)) {
    throw "Спочатку згенеруйте $inputFile"
}

$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "_" + [guid]::NewGuid().ToString("N").Substring(0, 6)
$resultDir = Join-Path $PSScriptRoot "evidence\part1\$runId"
New-Item -ItemType Directory -Path $resultDir -Force | Out-Null

& docker compose -f $composeFile up -d history
if ($LASTEXITCODE -ne 0) { throw "Не вдалося запустити History Server" }

# Те саме середовище, що й у Lab5; нові файли доступні через /work2.
$dockerArgs = @(
    "compose", "-f", $composeFile, "run", "--rm",
    "--volume", "${PSScriptRoot}:/work2", "spark",
    "/opt/spark/bin/spark-submit", "/work2/part1.py",
    "/work2/data/transactions.csv",
    "--output", "/work2/evidence/part1/$runId/results.json"
)
# Windows PowerShell 5.1 представляє звичайний stderr Java як ErrorRecord.
# Логуємо його, а успіх визначаємо за кодом завершення Docker.
try {
    $ErrorActionPreference = "Continue"
    & docker @dockerArgs 2>&1 | Tee-Object -FilePath (Join-Path $resultDir "console.log")
    $sparkExitCode = $LASTEXITCODE
}
finally {
    $ErrorActionPreference = "Stop"
}
if ($sparkExitCode -ne 0) { throw "Spark завершився з кодом $sparkExitCode. Лог: $resultDir\console.log" }

Write-Host "Результати: $resultDir"
Write-Host "History Server: http://localhost:18080"
