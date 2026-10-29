. "$PSScriptRoot\common.ps1"
$schemaVersion = if ($env:DIS_SCHEMA) { $env:DIS_SCHEMA } else { 'v1' }
if ($schemaVersion -notin @('v1','v2','v2_breaking')) { throw 'Invalid DIS_SCHEMA.' }
$clientDll = Join-Path $LabRoot "client\bin\Release\$schemaVersion\net10.0\OrderClient.dll"
if (-not (Test-Path -LiteralPath $clientDll)) { throw 'Run .\scripts\setup.ps1 first.' }
$dotnetExe = Get-LabDotnet
& $dotnetExe $clientDll @args
exit $LASTEXITCODE
