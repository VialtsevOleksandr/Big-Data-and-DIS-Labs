param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("a", "b", "c")]
    [string]$Variant
)

$ErrorActionPreference = "Stop"
$Variant = $Variant.ToLowerInvariant()
$composeFile = Join-Path $PSScriptRoot "..\Lab5_Spark_1\docker-compose.yml"
foreach ($relativePath in @("data\tx.parquet", "data\users.csv")) {
    if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot $relativePath))) {
        throw "Відсутній $relativePath. Спочатку підготуйте дані та виконайте частину 2"
    }
}
$runId = (Get-Date -Format "yyyyMMdd_HHmmss") + "_" + [guid]::NewGuid().ToString("N").Substring(0, 6)
$resultDir = Join-Path $PSScriptRoot "evidence\part3\$Variant\$runId"
New-Item -ItemType Directory -Path $resultDir -Force | Out-Null

& docker compose -f $composeFile up -d history
if ($LASTEXITCODE -ne 0) { throw "Не вдалося запустити History Server" }

$dockerArgs = @(
    "compose", "-f", $composeFile, "run", "--rm",
    "--volume", "${PSScriptRoot}:/work2", "spark",
    "/opt/spark/bin/spark-submit", "/work2/part3.py", "--variant", $Variant,
    "--transactions", "/work2/data/tx.parquet", "--users", "/work2/data/users.csv",
    "--output-dir", "/work2/evidence/part3/$Variant/$runId"
)
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
Write-Host "Після запусків a, b, c виконайте: python $PSScriptRoot\compare_part3.py"
