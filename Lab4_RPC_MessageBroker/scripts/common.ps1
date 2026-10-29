$ErrorActionPreference = 'Stop'
$LabRoot = Split-Path -Parent $PSScriptRoot
$WorkspaceRoot = Split-Path -Parent $LabRoot
$LabPython = Join-Path $LabRoot '.venv\Scripts\python.exe'
$env:DOTNET_CLI_TELEMETRY_OPTOUT = '1'
$env:DOTNET_NOLOGO = '1'
$env:DOTNET_CLI_HOME = Join-Path $WorkspaceRoot '.cache\dotnet-home'
$env:NUGET_PACKAGES = Join-Path $WorkspaceRoot '.cache\nuget'

function Get-LabDotnet {
    $localSdk = Join-Path $WorkspaceRoot '.cache\dotnet\dotnet.exe'
    if (Test-Path -LiteralPath $localSdk) { return $localSdk }
    $installed = Get-Command dotnet -ErrorAction SilentlyContinue
    if ($installed -and (& $installed.Source --list-sdks)) { return $installed.Source }
    throw 'Install .NET SDK 10: https://dotnet.microsoft.com/download/dotnet/10.0 (runtime alone is insufficient).'
}

function Assert-LabPython {
    if (-not (Test-Path -LiteralPath $LabPython)) {
        throw 'Run .\scripts\setup.ps1 first.'
    }
}
