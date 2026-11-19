# Спільні функції скриптів лабораторної. Підключається так:  . "$PSScriptRoot\common.ps1"

$ErrorActionPreference = "Stop"
try { [Console]::OutputEncoding = [System.Text.Encoding]::UTF8 } catch { }
Set-Location $PSScriptRoot

$SingleCompose = "docker-compose.single.yml"
$MultiCompose = "docker-compose.multi.yml"
$SingleNodes = "repa-pg1,repa-pg2,repa-pg3"
$MultiNodes = "repb-pg1,repb-pg2"

function Step([string]$Text) {
    Write-Host ""
    Write-Host "=== $Text ===" -ForegroundColor Cyan
}

# Запускає docker і зупиняє скрипт, якщо команда завершилася з помилкою.
# Docker і Python пишуть прогрес і попередження в stderr. Windows PowerShell 5.1 за
# ErrorActionPreference = Stop вважає такий рядок помилкою, тому тут stderr просто друкується,
# а про успіх судимо лише за кодом виходу.
function Invoke-Docker {
    $saved = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & docker @args 2>&1 | ForEach-Object { Write-Host "$_" }
        $code = $LASTEXITCODE
    }
    finally {
        $ErrorActionPreference = $saved
    }
    if ($code -ne 0) { throw "docker $($args -join ' ') завершилася з кодом $code" }
}

# python -m <модуль> у контейнері застосунку конфігурації A / B.
function AppA { Invoke-Docker exec repa-app python -m @args }
function AppB { Invoke-Docker exec repb-app python -m @args }

# Поточний час за годинником контейнера (секунди Unix). Усі контейнери мають спільний годинник,
# тому ці позначки можна порівнювати з часом у журналі застосунку.
function Get-Epoch([string]$App = "repa-app") {
    $value = & docker exec $App date +%s.%N
    if ($LASTEXITCODE -ne 0) { throw "Не вдалося отримати час із контейнера $App" }
    return "$value".Trim()
}

# Зберігає журнал вузла, починаючи з моменту $Since (секунди Unix), у файл відносно каталогу лабораторної.
function Save-Logs([string]$Container, [string]$Since, [string]$Dir, [string]$Name) {
    $folder = "results\" + $Dir.Replace("/", "\")
    New-Item -ItemType Directory -Force -Path $folder | Out-Null
    $file = "$folder\$Name"
    $seconds = $Since.Split(".")[0]
    cmd /c "docker logs --timestamps --since $seconds $Container > $file 2>&1"
    Write-Host "  журнал $Container -> $file"
}

function Wait-Seconds([int]$Seconds, [string]$What) {
    $end = (Get-Date).AddSeconds($Seconds)
    while ($true) {
        $left = [int][Math]::Ceiling(($end - (Get-Date)).TotalSeconds)
        if ($left -le 0) { break }
        Write-Host ("  {0}: лишилося {1} с" -f $What, $left)
        Start-Sleep -Seconds ([Math]::Min(60, $left))
    }
}

function Wait-PgReady([string]$Container, [int]$TimeoutSec = 180) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        cmd /c "docker exec $Container pg_isready -h 127.0.0.1 -U postgres -d notes -q >nul 2>&1"
        if ($LASTEXITCODE -eq 0) { return }
        Start-Sleep -Seconds 1
    }
    throw "Вузол $Container не почав приймати з'єднання за $TimeoutSec с"
}

# Запускає навантаження у фоні й чекає перших підтверджених записів.
function Start-Load {
    param([string]$App, [string]$Dir, [int]$Rate = 100, [string]$Hosts = "",
          [int]$BodySize = 200, [int]$AckTimeout = 0)
    $folder = "results/$Dir"
    New-Item -ItemType Directory -Force -Path $folder | Out-Null
    foreach ($name in "journal.csv", "summary.json", "load_error.log", "STOP") {
        if (Test-Path "$folder/$name") { Remove-Item "$folder/$name" -Force }
    }
    $loadArgs = @("exec", "-d", $App, "python", "-m", "notes.load", "--dir", "/results/$Dir",
                  "--rate", "$Rate", "--body-size", "$BodySize", "--ack-timeout", "$AckTimeout")
    if ($Hosts) { $loadArgs += @("--hosts", $Hosts) }
    Invoke-Docker @loadArgs

    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        if (Test-Path "$folder/load_error.log") {
            throw "Навантаження впало:`n$(Get-Content "$folder/load_error.log" -Raw)"
        }
        if ((Test-Path "$folder/journal.csv") -and ((Get-Item "$folder/journal.csv").Length -gt 300)) {
            Write-Host "  навантаження працює: $Rate записів/с, журнал $folder/journal.csv"
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Навантаження не почало писати за 60 с (див. $folder/load.log)"
}

# Зупиняє навантаження файлом STOP і чекає, доки воно запише summary.json.
function Stop-Load([string]$App, [string]$Dir, [int]$TimeoutSec = 180) {
    $folder = "results/$Dir"
    Invoke-Docker exec $App touch "/results/$Dir/STOP"
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if (Test-Path "$folder/summary.json") {
            $s = Get-Content "$folder/summary.json" -Raw | ConvertFrom-Json
            Write-Host ("  навантаження зупинено: спроб {0}, підтверджено {1}, помилок {2}, без лідера {3}, пропущено тактів {4}" -f `
                $s.attempts, $s.ok, $s.error, $s.no_leader, $s.skipped_ticks)
            return
        }
        Start-Sleep -Milliseconds 500
    }
    throw "Навантаження не зупинилося за $TimeoutSec с (див. $folder/load.log)"
}

# Файл у папці data\ лабораторної -> його шлях у контейнері застосунку (/data/...).
function ConvertTo-ContainerDataPath([string]$File) {
    if (-not (Test-Path -LiteralPath $File -PathType Leaf)) { throw "Немає файлу $File" }
    $full = (Resolve-Path -LiteralPath $File).Path
    $root = (Join-Path $PSScriptRoot "data").TrimEnd("\") + "\"
    if (-not $full.StartsWith($root, [StringComparison]::OrdinalIgnoreCase)) {
        throw "Файл $File має лежати в папці data\ лабораторної: лише вона видна контейнеру як /data"
    }
    return "/data/" + $full.Substring($root.Length).Replace("\", "/")
}

# Чекає появи файлу, який пише фоновий процес у контейнері; при тайм-ауті показує його журнал.
function Wait-File([string]$Path, [int]$TimeoutSec, [string]$LogPath = "") {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        if ((Test-Path $Path) -and ((Get-Item $Path).Length -gt 0)) { return }
        Start-Sleep -Milliseconds 200
    }
    $log = if ($LogPath -and (Test-Path $LogPath)) { Get-Content $LogPath -Raw } else { "" }
    throw "Файл $Path не з'явився за $TimeoutSec с. $log"
}

# Аварійна зупинка навантаження в блоках finally: помилки ігноруються.
function Stop-LoadQuiet([string]$App, [string]$Dir) {
    cmd /c "docker exec $App touch /results/$Dir/STOP >nul 2>&1"
}

# Перевіряє початковий стан конфігурації A: pg1 лідер, pg2 і pg3 отримують WAL потоком.
function Assert-SingleTopology {
    try {
        AppA notes.admin wait-streaming --leader repa-pg1 --names "pg2,pg3" --timeout 60
    } catch {
        throw "Очікуваний стан: repa-pg1 лідер, pg2 і pg3 у стані streaming. Запустіть .\setup_single.ps1. ($_)"
    }
}
