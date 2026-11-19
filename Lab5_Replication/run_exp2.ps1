param(
    [int]$Writes = 10000,        # записів у кожному запуску
    [int]$Warmup = 1000,         # прогрівальних записів перед вимірами
    [int]$Runs = 3,              # запусків на режим
    [int]$FailRate = 100,        # записів/с під час досліду з від'єднанням синхронного фоловера
    [int]$FailSeconds = 60,      # на скільки від'єднати синхронний фоловер
    [int]$AckTimeoutSec = 0,     # 0 = застосунок чекає підтвердження скільки завгодно; N = скасовує запит через N с
    [switch]$SkipLatency,
    [switch]$SkipFailure
)

# Експеримент 2: синхронна й асинхронна реплікація (конфігурація A).
. "$PSScriptRoot\common.ps1"

Assert-SingleTopology
$tStart = Get-Epoch

try {
    if (-not $SkipLatency) {
        Step "1. Обидва фоловери асинхронні"
        AppA notes.admin reset --node repa-pg1 synchronous_standby_names
        AppA notes.admin wait-streaming --leader repa-pg1 --names "pg2,pg3"
        AppA notes.exp2 --label async --writes $Writes --warmup $Warmup --runs $Runs --leader repa-pg1 `
            --dir /results/exp2/async

        Step "2. Один синхронний фоловер: synchronous_standby_names = 'FIRST 1 (pg2)'"
        AppA notes.admin set --node repa-pg1 synchronous_standby_names "FIRST 1 (pg2)"
        AppA notes.admin wait-streaming --leader repa-pg1 --names "pg2,pg3" --sync-names "pg2"
        AppA notes.exp2 --label sync --writes $Writes --warmup $Warmup --runs $Runs --leader repa-pg1 `
            --dir /results/exp2/sync
    }

    if (-not $SkipFailure) {
        $dir = "exp2/sync_failure"
        $res = "/results/$dir"
        Step "3. Синхронний фоловер pg2 зникає з мережі на $FailSeconds с"
        AppA notes.admin set --node repa-pg1 synchronous_standby_names "FIRST 1 (pg2)"
        AppA notes.admin wait-streaming --leader repa-pg1 --names "pg2,pg3" --sync-names "pg2"
        Start-Load -App repa-app -Dir $dir -Rate $FailRate -Hosts "repa-pg1" -AckTimeout $AckTimeoutSec
        Wait-Seconds 10 "записи до збою"

        $t0 = Get-Epoch
        Write-Host "  команда збою: docker network disconnect repa-repl repa-pg2"
        Invoke-Docker network disconnect repa-repl repa-pg2

        $half = [int]($FailSeconds / 2)
        Wait-Seconds $half "pg2 від'єднано"
        # Що в цей момент робить лідер: чи є запити, які чекають синхронного фоловера,
        # і скільки нотаток видно на кожному вузлі.
        AppA notes.admin activity --node repa-pg1 --out "$res/activity_during_partition.json"
        AppA notes.admin counts --nodes $SingleNodes --out "$res/counts_during_partition.json"
        AppA notes.admin status --nodes "repa-pg1" --out "$res/status_during_partition.json"
        Wait-Seconds ($FailSeconds - $half) "pg2 від'єднано"

        $t1 = Get-Epoch
        Write-Host "  відновлення: docker network connect repa-repl repa-pg2"
        Invoke-Docker network connect repa-repl repa-pg2
        AppA notes.admin wait-streaming --leader repa-pg1 --names "pg2,pg3" --sync-names "pg2" --timeout 120
        Wait-Seconds 10 "записи після відновлення"
        Stop-Load -App repa-app -Dir $dir

        AppA notes.report --dir $res --t0 $t0 --t1 $t1
        AppA notes.admin wait-sync --leader repa-pg1
        AppA notes.verify --journal "$res/journal.csv" --nodes $SingleNodes --out "$res/verify.json"
        Save-Logs repa-pg1 $t0 $dir "pg1.log"
        Save-Logs repa-pg2 $t0 $dir "pg2.log"
    }
}
finally {
    Step "Прибирання: мережу pg2 відновлено, реплікація знову асинхронна"
    cmd /c "docker network connect repa-repl repa-pg2 >nul 2>&1"   # якщо вже під'єднано, помилка ігнорується
    Stop-LoadQuiet repa-app "exp2/sync_failure"
    AppA notes.admin reset --node repa-pg1 synchronous_standby_names
}

Step "Експеримент 2 завершено. Результати: results\exp2"
