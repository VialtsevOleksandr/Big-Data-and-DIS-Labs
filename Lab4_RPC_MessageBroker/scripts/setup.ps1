. "$PSScriptRoot\common.ps1"
Push-Location $LabRoot
try {
    if (-not (Test-Path -LiteralPath $LabPython)) {
        python -m venv .venv
        if ($LASTEXITCODE -ne 0) { throw 'Cannot create Python environment.' }
    }
    & $LabPython -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { throw 'Python dependency installation failed.' }
    & $LabPython scripts/generate_python.py
    if ($LASTEXITCODE -ne 0) { throw 'Python code generation failed.' }
    $dotnetExe = Get-LabDotnet
    foreach ($schemaVersion in @('v1', 'v2', 'v2_breaking')) {
        & $dotnetExe build client/OrderClient.csproj --configuration Release "-p:SchemaVersion=$schemaVersion"
        if ($LASTEXITCODE -ne 0) { throw "C# build failed: $schemaVersion" }
    }
    Write-Host 'Ready. Use scripts/start-server.ps1 and scripts/start-client.ps1 in two terminals.'
} finally {
    Pop-Location
}
