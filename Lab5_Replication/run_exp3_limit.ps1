param(
    [int]$LimitMB = 32,        # max_slot_wal_keep_size: скільки WAL лідер зберігає для відсталого фоловера
    [int]$Rate = 200,          # записів на секунду; одне з'єднання застосунку тягне приблизно 250/с
    [int]$BodySize = 2500,     # розмір тексту нотатки в байтах: ~0,5 МіБ WAL/с, межу досягнуто за 1–2 хв
    [int]$MaxSeconds = 900,    # скільки щонайбільше чекати втрати слота
    [switch]$NoReseed          # не відновлювати фоловер наприкінці
)

# Експеримент 3, пошук межі: журнал лідера обмежено, фоловер pg2 після розриву вже не наздоганяє.
. "$PSScriptRoot\common.ps1"

$dir = "exp3/limit_${LimitMB}mb"
$res = "/results/$dir"

Assert-SingleTopology
AppA notes.admin wait-sync --leader repa-pg1

try {
    Step "1. Обмеження журналу: max_slot_wal_keep_size = ${LimitMB}MB на лідері"
    AppA notes.admin set --node repa-pg1 max_slot_wal_keep_size "${LimitMB}MB"
    AppA notes.admin slots --node repa-pg1 --out "$res/slots_before.json"

    Step "2. Навантаження $Rate записів/с по $BodySize байтів і розрив зв'язку з pg2"
    Start-Load -App repa-app -Dir $dir -Rate $Rate -Hosts "repa-pg1" -BodySize $BodySize
    Wait-Seconds 10 "записи до розриву"
    $t0 = Get-Epoch
    Write-Host "  команда збою: docker network disconnect repa-repl repa-pg2"
    Invoke-Docker network disconnect repa-repl repa-pg2

    Step "3. Спостереження за слотом pg2_slot, доки лідер не видалить потрібний фоловеру WAL"
    AppA notes.admin watch-slot --node repa-pg1 --slot pg2_slot --interval 10 --timeout $MaxSeconds `
        --out "$res/slot_history.json"
    AppA notes.admin counts --nodes "repa-pg1,repa-pg2" --out "$res/counts_before_reconnect.json"

    Step "4. Відновлення зв'язку: чи наздожене фоловер"
    $t1 = Get-Epoch
    Write-Host "  відновлення: docker network connect repa-repl repa-pg2"
    Invoke-Docker network connect repa-repl repa-pg2
    Wait-Seconds 30 "спроби pg2 під'єднатися до лідера"
    Stop-Load -App repa-app -Dir $dir

    AppA notes.admin status --out "$res/status_after_reconnect.json"
    AppA notes.admin slots --node repa-pg1 --out "$res/slots_after_reconnect.json"
    AppA notes.admin counts --nodes "repa-pg1,repa-pg2" --out "$res/counts_after_reconnect.json"
    AppA notes.report --dir $res --t0 $t0 --t1 $t1
    Save-Logs repa-pg1 $t0 $dir "pg1.log"
    Save-Logs repa-pg2 $t0 $dir "pg2.log"

    if (-not $NoReseed) {
        Step "5. Повернення фоловера: нова повна копія з лідера (pg_basebackup)"
        Write-Host "  команди: docker compose rm -sf pg2; docker volume rm repa-pg2-data; docker compose up -d pg2"
        $watch = [Diagnostics.Stopwatch]::StartNew()
        $t2 = Get-Epoch
        Invoke-Docker compose -f $SingleCompose rm -sf pg2
        Invoke-Docker volume rm repa-pg2-data
        Invoke-Docker compose -f $SingleCompose up -d --wait pg2
        AppA notes.admin wait-streaming --leader repa-pg1 --names "pg2,pg3" --timeout 600
        AppA notes.admin wait-sync --leader repa-pg1 --names "pg2,pg3"
        $watch.Stop()
        Write-Host ("  фоловер pg2 відновлено з нуля за {0:N1} с" -f $watch.Elapsed.TotalSeconds)
        AppA notes.admin compare --nodes "repa-pg1,repa-pg2" --out "$res/compare_after_reseed.json"
        AppA notes.admin slots --node repa-pg1 --out "$res/slots_after_reseed.json"
        Save-Logs repa-pg2 $t2 $dir "pg2_reseed.log"
    }
}
finally {
    Step "Прибирання: мережу pg2 відновлено, обмеження журналу знято"
    cmd /c "docker network connect repa-repl repa-pg2 >nul 2>&1"
    Stop-LoadQuiet repa-app $dir
    AppA notes.admin reset --node repa-pg1 max_slot_wal_keep_size
}

Step "Пошук межі завершено. Результати: results\exp3\limit_${LimitMB}mb"
