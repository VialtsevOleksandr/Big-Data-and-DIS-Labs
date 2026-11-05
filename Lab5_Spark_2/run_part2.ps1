param(
    [string]$Category = "books"
)

$ErrorActionPreference = "Stop"
$composeFile = Join-Path $PSScriptRoot "..\Lab5_Spark_1\docker-compose.yml"
$inputFile = Join-Path $PSScriptRoot "data\transactions.csv"
if (-not (Test-Path -LiteralPath $inputFile)) {
    throw "Спочатку згенеруйте $inputFile"
}

$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "_" + [guid]::NewGuid().ToString("N").Substring(0, 6)
$resultDir = Join-Path $PSScriptRoot "evidence\part2\$runId"
New-Item -ItemType Directory -Path $resultDir -Force | Out-Null

& docker compose -f $composeFile up -d history
if ($LASTEXITCODE -ne 0) { throw "Не вдалося запустити History Server" }

$dockerArgs = @(
    "compose", "-f", $composeFile, "run", "--rm",
    "--volume", "${PSScriptRoot}:/work2", "spark",
    "/opt/spark/bin/spark-submit", "/work2/part2.py",
    "/work2/data/transactions.csv", "--data-dir", "/work2/data",
    "--category", $Category,
    "--output-dir", "/work2/evidence/part2/$runId"
)
# У Windows PowerShell 5.1 stderr Java може надходити як ErrorRecord.
# Визначаємо успіх за кодом завершення Docker, а весь вивід зберігаємо.
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
