param(
    [int[]]$Seconds = @(300, 600, 900),   # тривалості розриву: 5, 10 і 15 хвилин
    [int]$Rate = 100,                     # записів на секунду протягом усього досліду
    [int]$BaselineSeconds = 15
)

# Експеримент 3: розрив зв'язку з фоловером pg2 (конфігурація A, асинхронно, журнал не обмежено).
# Для швидкої перевірки, що все працює:  .\run_exp3.ps1 -Seconds 30
. "$PSScriptRoot\common.ps1"

Assert-SingleTopology
AppA notes.admin reset --node repa-pg1 synchronous_standby_names
AppA notes.admin reset --node repa-pg1 max_slot_wal_keep_size

foreach ($sec in $Seconds) {
    $dir = "exp3/partition_${sec}s"
    $res = "/results/$dir"
    Step "Розрив на $sec с: навантаження $Rate записів/с"
    AppA notes.admin wait-sync --leader repa-pg1
    Start-Load -App repa-app -Dir $dir -Rate $Rate -Hosts "repa-pg1"

    try {
        Wait-Seconds $BaselineSeconds "записи до розриву"
        AppA notes.admin counts --nodes "repa-pg1,repa-pg2" --out "$res/counts_before.json"

        $t0 = Get-Epoch
        Write-Host "  команда збою: docker network disconnect repa-repl repa-pg2"
        Invoke-Docker network disconnect repa-repl repa-pg2
        Wait-Seconds $sec "pg2 від'єднано, лідер приймає записи"

        # Стан перед відновленням: різниця кількості нотаток і скільки WAL лідер тримає для pg2.
        AppA notes.admin counts --nodes "repa-pg1,repa-pg2" --out "$res/counts_before_reconnect.json"
        AppA notes.admin slots --node repa-pg1 --out "$res/slots_before_reconnect.json"

        # Вимір наздоганяння запускається ДО відновлення мережі: на одній машині фоловер
        # наздоганяє за частки секунди, і запізнілий вимір цього вже не застав би.
        $folder = "results/$dir"
        foreach ($name in "catchup.json", "catchup.log", "catchup_ready", "reconnected_at") {
            if (Test-Path "$folder/$name") { Remove-Item "$folder/$name" -Force }
        }
        Invoke-Docker exec -d repa-app sh -c ("python -m notes.admin catchup --leader repa-pg1 --follower repa-pg2 " +
            "--ready-file $res/catchup_ready --since-file $res/reconnected_at --out $res/catchup.json > $res/catchup.log 2>&1")
        Wait-File "$folder/catchup_ready" 60 "$folder/catchup.log"

        $t1 = Get-Epoch
        Set-Content -Path "$folder/reconnected_at" -Value $t1 -Encoding ascii
        Write-Host "  відновлення: docker network connect repa-repl repa-pg2"
        Invoke-Docker network connect repa-repl repa-pg2

        # Фоловер має наздогнати сам: жодних команд на вузлах, лише спостереження.
        Wait-File "$folder/catchup.json" 1800 "$folder/catchup.log"
        Get-Content "$folder/catchup.log"
        Wait-Seconds 10 "записи після наздоганяння"
        Stop-Load -App repa-app -Dir $dir
    }
    finally {
        # На випадок помилки посеред досліду: повернути мережу й зупинити навантаження.
        cmd /c "docker network connect repa-repl repa-pg2 >nul 2>&1"
        Stop-LoadQuiet repa-app $dir
    }

    AppA notes.admin wait-sync --leader repa-pg1 --names "pg2,pg3"
    AppA notes.report --dir $res --t0 $t0 --t1 $t1
    AppA notes.verify --journal "$res/journal.csv" --nodes "repa-pg1,repa-pg2" --out "$res/verify.json"
    AppA notes.admin compare --nodes "repa-pg1,repa-pg2" --out "$res/compare.json"
    AppA notes.admin slots --node repa-pg1 --out "$res/slots_after.json"
    Save-Logs repa-pg1 $t0 $dir "pg1.log"
    Save-Logs repa-pg2 $t0 $dir "pg2.log"
}

Step "Експеримент 3 завершено. Результати: results\exp3\partition_*"
