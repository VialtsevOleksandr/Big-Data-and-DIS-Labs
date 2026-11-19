param(
    [int]$Rate = 150,               # записів на секунду (одне з'єднання тягне приблизно 250/с)
    [int]$BaselineSeconds = 20,     # скільки писати до відмови лідера
    [int]$DetectDelaySeconds = 0,   # пауза між відмовою і початком failover («час, поки помітили»)
    [int]$AfterSeconds = 20,        # скільки писати на нового лідера до повернення старого
    [string]$RunName = "run1"
)

# Експеримент 4: відмова лідера (конфігурація A, асинхронно), ручний failover.
# Після цього скрипта лідером є pg2 або pg3. Щоб повторити будь-який експеримент,
# спочатку знову запустіть .\setup_single.ps1.
. "$PSScriptRoot\common.ps1"

$dir = "exp4/$RunName"
$res = "/results/$dir"

Assert-SingleTopology
AppA notes.admin reset --node repa-pg1 synchronous_standby_names
AppA notes.admin wait-sync --leader repa-pg1

Step "1. Навантаження $Rate записів/с; застосунок сам шукає лідера серед трьох вузлів"
Start-Load -App repa-app -Dir $dir -Rate $Rate -Hosts $SingleNodes
Wait-Seconds $BaselineSeconds "записи до відмови"

Step "2. Відмова лідера"
$tKill = Get-Epoch
Write-Host "  команда збою: docker kill repa-pg1"
Invoke-Docker kill repa-pg1
if ($DetectDelaySeconds -gt 0) { Wait-Seconds $DetectDelaySeconds "відмову ще не помічено" }

Step "3. Failover: новим лідером стає фоловер, який отримав найбільше WAL"
AppA notes.failover promote --candidates "repa-pg2,repa-pg3" --all-nodes $SingleNodes --out "$res/promote.json"
$promote = Get-Content "results/$dir/promote.json" -Raw | ConvertFrom-Json
$new = $promote.new_leader
$followerNames = @("pg1")
foreach ($other in $promote.other_followers) {
    # Інший фоловер перемикається на нового лідера: новий primary_conninfo і перечитування конфігурації.
    $name = $other.Split("-")[-1]
    $followerNames += $name
    Invoke-Docker exec -u postgres $other bash /opt/replication/set-upstream.sh $new "${name}_slot"
    AppA notes.admin reload --node $other
}
Write-Host "  новий лідер: $new"
Wait-Seconds $AfterSeconds "записи на нового лідера"
AppA notes.failover roles --out "$res/roles_after_promote.json"

Step "4. Старий лідер запускається знову"
Write-Host "  команда: docker start repa-pg1"
Invoke-Docker start repa-pg1
Wait-PgReady repa-pg1
# У якій ролі він піднявся і чи приймають записи два вузли одночасно.
AppA notes.failover roles --out "$res/roles_old_leader_back.json"
AppA notes.failover probe-write --node repa-pg1 --out "$res/probe_write_old_leader.json"
Wait-Seconds 5 "обидва вузли працюють"
Stop-Load -App repa-app -Dir $dir

Step "5. Простій і втрачені записи (звірка журналу застосунку з вузлами)"
AppA notes.report --dir $res --t0 $tKill
AppA notes.verify --journal "$res/journal.csv" --nodes $SingleNodes --out "$res/verify_before_rewind.json"
AppA notes.admin compare --nodes "repa-pg1,$new" --out "$res/compare_before_rewind.json"
Save-Logs repa-pg1 $tKill $dir "pg1_before_rewind.log"

Step "6. Старий лідер стає фоловером нового: pg_rewind"
Invoke-Docker stop repa-pg1
AppA notes.admin checkpoint --node $new
$tRewind = Get-Epoch
# Вивід pg_rewind (точка розходження, скільки даних скопійовано) зберігається у файл.
cmd /c "docker compose -f $SingleCompose run --rm --no-deps --entrypoint bash pg1 /opt/replication/rewind.sh $new pg1_slot > results\exp4\$RunName\rewind.log 2>&1"
$rewindCode = $LASTEXITCODE
Get-Content "results\exp4\$RunName\rewind.log"
if ($rewindCode -ne 0) { throw "pg_rewind завершився з кодом $rewindCode (див. results\exp4\$RunName\rewind.log)" }
Invoke-Docker start repa-pg1
Wait-PgReady repa-pg1
AppA notes.admin drop-slots --node repa-pg1
AppA notes.admin wait-streaming --leader $new --names ($followerNames -join ",") --timeout 300
AppA notes.admin wait-sync --leader $new

Step "7. Підсумковий стан"
AppA notes.failover roles --out "$res/roles_final.json"
AppA notes.verify --journal "$res/journal.csv" --nodes $SingleNodes --out "$res/verify_final.json"
AppA notes.admin compare --nodes $SingleNodes --out "$res/compare_final.json"
Save-Logs repa-pg1 $tRewind $dir "pg1_after_rewind.log"
Save-Logs repa-pg2 $tKill $dir "pg2.log"
Save-Logs repa-pg3 $tKill $dir "pg3.log"

Step "Експеримент 4 завершено. Результати: results\exp4\$RunName. Лідер тепер $new; для повтору запустіть .\setup_single.ps1"
