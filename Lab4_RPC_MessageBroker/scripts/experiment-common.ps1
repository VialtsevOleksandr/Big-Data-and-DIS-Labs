. "$PSScriptRoot\common.ps1"

function Invoke-LabExperiment {
    param(
        [Parameter(Mandatory)][string]$Step,
        [Parameter(Mandatory)][string]$Module,
        [string[]]$PythonArguments = @(),
        [switch]$NeedsDotnet
    )
    Assert-LabPython
    $env:PYTHONUTF8 = '1'
    $env:PYTHONIOENCODING = 'utf-8'
    [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
    if ($NeedsDotnet) { $env:DIS_DOTNET = Get-LabDotnet }
    $runStamp = [DateTime]::UtcNow.ToString('yyyyMMddTHHmmssfffZ')
    $runSuffix = [Guid]::NewGuid().ToString('N').Substring(0, 6)
    $outputDirectory = Join-Path $LabRoot "results\${Step}_${runStamp}_${runSuffix}"
    New-Item -ItemType Directory -Path $outputDirectory -ErrorAction Stop | Out-Null
    Push-Location $LabRoot
    $transcribing = $false
    try {
        Start-Transcript -Path (Join-Path $outputDirectory 'console.txt') | Out-Null
        $transcribing = $true
        Write-Host "STEP: $Step"
        Write-Host "RESULT DIRECTORY: $outputDirectory"
        $hardware = [ordered]@{ collected_utc = [DateTime]::UtcNow.ToString('o') }
        try {
            $processors = @(Get-CimInstance Win32_Processor -ErrorAction Stop)
            $computer = Get-CimInstance Win32_ComputerSystem -ErrorAction Stop
            $system = Get-CimInstance Win32_OperatingSystem -ErrorAction Stop
            $hardware.cpu = @($processors | Select-Object Name, NumberOfCores, NumberOfLogicalProcessors)
            $hardware.ram_bytes = [long]$computer.TotalPhysicalMemory
            $hardware.os = "$($system.Caption) $($system.Version)"
            Write-Host "CPU: $(($processors.Name -join ', ').Trim())"
            Write-Host ('RAM: {0:F2} GiB' -f ($computer.TotalPhysicalMemory / 1GB))
        } catch {
            $hardware.collection_error = $_.Exception.Message
            Write-Host "Hardware info could not be read; see hardware.json."
        }
        $hardware | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath (Join-Path $outputDirectory 'hardware.json') -Encoding utf8
        [ordered]@{ module = $Module; arguments = $PythonArguments; python = $LabPython } |
            ConvertTo-Json -Depth 5 | Set-Content -LiteralPath (Join-Path $outputDirectory 'invocation.json') -Encoding utf8
        if ($NeedsDotnet) {
            & $env:DIS_DOTNET --info | Out-File -LiteralPath (Join-Path $outputDirectory 'dotnet-info.txt') -Encoding utf8
            if ($LASTEXITCODE -ne 0) { throw '.NET SDK info failed.' }
        }
        & $LabPython -u -m $Module --output-dir $outputDirectory @PythonArguments 2>&1 | Out-Host
        if ($LASTEXITCODE -ne 0) {
            throw "Step $Step failed. Logs and any partial results are in $outputDirectory"
        }
        Write-Host ""
        Write-Host 'DONE. Take a screenshot of the final summary above.'
        Write-Host "Full console log and result files: $outputDirectory"
    } finally {
        if ($transcribing) { Stop-Transcript | Out-Null }
        Pop-Location
    }
}
