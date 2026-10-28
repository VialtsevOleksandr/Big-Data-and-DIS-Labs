param(
    [ValidateRange(3, 1000)][int]$Repeats = 3,
    [string]$InputPath = '/data/transactions.csv',
    [string]$ResultsDirectory = 'evidence/part3',
    [string]$AppNamePrefix = 'Lab5_Part3',
    [switch]$PauseForScreenshots,
    [switch]$Resume
)

$ErrorActionPreference = 'Stop'
$utf8Encoding = New-Object System.Text.UTF8Encoding($false)
[Console]::OutputEncoding = $utf8Encoding
$OutputEncoding = $utf8Encoding

# Усі результати мають потрапити у змонтовану папку лабораторної.
if ([IO.Path]::IsPathRooted($ResultsDirectory)) {
    throw 'ResultsDirectory має бути відносним шляхом усередині лабораторної.'
}
$labRoot = [IO.Path]::GetFullPath($PSScriptRoot)
$resultsRoot = [IO.Path]::GetFullPath((Join-Path $labRoot $ResultsDirectory))
if (-not $resultsRoot.StartsWith($labRoot + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'ResultsDirectory виходить за межі лабораторної.'
}
$containerResults = '/work/' + $resultsRoot.Substring($labRoot.Length + 1).Replace('\', '/')
New-Item -ItemType Directory -Path $resultsRoot -Force | Out-Null

function Invoke-DockerLogged {
    param([string[]]$DockerArguments, [string]$LogPath, [string]$ContinuePath)

    # Запускаємо Docker окремим процесом: він не перехоплює клавіатуру
    # PowerShell, тому Read-Host може прийняти Enter під час паузи Spark.
    $startInfo = New-Object System.Diagnostics.ProcessStartInfo
    $startInfo.FileName = (Get-Command docker -ErrorAction Stop).Source
    $quotedArguments = foreach ($argument in $DockerArguments) {
        $escapedArgument = [regex]::Replace($argument, '(\\*)"', '$1$1\"')
        $escapedArgument = [regex]::Replace($escapedArgument, '(\\+)$', '$1$1')
        '"' + $escapedArgument + '"'
    }
    $startInfo.Arguments = $quotedArguments -join ' '
    $startInfo.WorkingDirectory = $labRoot
    $startInfo.UseShellExecute = $false
    $startInfo.CreateNoWindow = $true
    $startInfo.RedirectStandardOutput = $true
    $startInfo.RedirectStandardError = $true
    $startInfo.StandardOutputEncoding = $utf8Encoding
    $startInfo.StandardErrorEncoding = $utf8Encoding
    $dockerProcess = New-Object System.Diagnostics.Process
    $dockerProcess.StartInfo = $startInfo
    $writer = New-Object System.IO.StreamWriter($LogPath, $false, $utf8Encoding)
    try {
        $dockerProcess.Start() | Out-Null
        $streams = @($dockerProcess.StandardOutput, $dockerProcess.StandardError)
        $pendingReads = @($streams[0].ReadLineAsync(), $streams[1].ReadLineAsync())
        while ($null -ne $pendingReads[0] -or $null -ne $pendingReads[1]) {
            for ($streamIndex = 0; $streamIndex -lt 2; $streamIndex++) {
                $pendingRead = $pendingReads[$streamIndex]
                if ($null -ne $pendingRead -and $pendingRead.IsCompleted) {
                    $line = $pendingRead.GetAwaiter().GetResult()
                    if ($null -eq $line) {
                        $pendingReads[$streamIndex] = $null
                        continue
                    }
                    $writer.WriteLine($line)
                    $writer.Flush()
                    Write-Host $line
                    if ($line.StartsWith('PAUSE_FOR_SCREENSHOTS:')) {
                        Read-Host 'Збережіть Timeline та Executors у http://localhost:4040. Потім натисніть Enter' | Out-Null
                        [IO.File]::WriteAllText($ContinuePath, "continue`n", $utf8Encoding)
                    }
                    $pendingReads[$streamIndex] = $streams[$streamIndex].ReadLineAsync()
                }
            }
            Start-Sleep -Milliseconds 25
        }
        $dockerProcess.WaitForExit()
        return $dockerProcess.ExitCode
    }
    finally {
        $writer.Dispose()
        $dockerProcess.Dispose()
    }
}

Push-Location $labRoot
try {
    $dockerCpuText = & docker info --format '{{.NCPU}}'
    if ($LASTEXITCODE -ne 0) { throw 'Docker недоступний. Запустіть Docker Desktop.' }
    $hardware = [ordered]@{
        host_logical_processors = [Environment]::ProcessorCount
        docker_cpus = [int]($dockerCpuText | Select-Object -Last 1)
    }
    $hardware | ConvertTo-Json | Set-Content -LiteralPath (Join-Path $resultsRoot 'hardware.json') -Encoding UTF8
    Write-Host "Host CPUs: $($hardware.host_logical_processors); Docker CPUs: $($hardware.docker_cpus)"
    $rows = @()

    # Конфігурації запускаються послідовно, щоб не конкурувати за CPU і диск.
    foreach ($blockBytes in @(33554432, 134217728)) {
        foreach ($cores in @(1, 2, 4)) {
            $blockMiB = [int]($blockBytes / 1MB)
            $configId = "local${cores}_${blockMiB}MiB"
            $logPath = Join-Path $resultsRoot "$configId.log"
            $jsonPath = Join-Path $resultsRoot "$configId.json"
            $reuseResult = $Resume -and (Test-Path -LiteralPath $jsonPath)
            if ($reuseResult) {
                $result = Get-Content -LiteralPath $jsonPath -Raw -Encoding UTF8 | ConvertFrom-Json
                if ($result.master -ne "local[$cores]" -or $result.block_bytes -ne $blockBytes -or
                    $result.input -ne $InputPath -or @($result.runs).Count -ne ($Repeats + 1)) {
                    throw "Збережений результат $jsonPath не відповідає параметрам запуску."
                }
                Write-Host "Reusing completed result: $configId"
            }
            else {
                $submitArgs = @(
                    'compose', 'run', '--rm', '--no-TTY', '--interactive=false', '--service-ports', 'spark',
                    'spark-submit', '--master', "local[$cores]",
                    '--conf', "spark.hadoop.fs.local.block.size=$blockBytes",
                    'my_part3.py', $InputPath, '--repeats', "$Repeats",
                    '--app-name', "${AppNamePrefix}_$configId",
                    '--output', "$containerResults/$configId.json"
                )
                if ($PauseForScreenshots -and $cores -eq 4) {
                    $submitArgs += '--pause'
                }
                Write-Host "Running $configId"
                $continuePath = [IO.Path]::ChangeExtension($jsonPath, 'continue')
                $dockerExitCode = Invoke-DockerLogged -DockerArguments $submitArgs -LogPath $logPath -ContinuePath $continuePath
                if ($dockerExitCode -ne 0) {
                    throw "Конфігурація $configId завершилася з кодом $dockerExitCode. Лог: $logPath"
                }
                $result = Get-Content -LiteralPath $jsonPath -Raw -Encoding UTF8 | ConvertFrom-Json
            }
            $row = [ordered]@{
                master = $result.master
                block_mib = $blockMiB
                input_partitions = $result.runs[1].input_partitions
                tasks_per_job = $result.runs[1].tasks
                reading_tasks = $result.runs[1].reading_tasks
                warmup_seconds = $result.runs[0].seconds
            }
            for ($repeat = 1; $repeat -le $Repeats; $repeat++) {
                $row["repeat_${repeat}_seconds"] = $result.runs[$repeat].seconds
            }
            $row['median_seconds'] = $result.median_seconds
            $row['application_id'] = $result.application_id
            $rows += [pscustomobject]$row
            $rows | Export-Csv -LiteralPath (Join-Path $resultsRoot 'results.csv') -NoTypeInformation -Encoding UTF8
        }
    }
    $rows | Format-Table master, block_mib, input_partitions, tasks_per_job, median_seconds -AutoSize
    Write-Host "Results: $resultsRoot"
}
finally {
    Pop-Location
}
