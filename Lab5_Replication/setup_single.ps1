param(
    [int]$Notes = 100000,   # скільки нотаток згенерувати до підключення другого фоловера
    [int]$Seed = 42,        # seed генератора
    [string]$DataFile = "", # готовий CSV із generate_data.py у папці data\; тоді -Notes і -Seed не діють
    [int]$Rate = 100,       # записів на секунду, поки підключається другий фоловер
    [int]$WarmupSeconds = 10
)

# Конфігурація A з нуля: лідер pg1 + фоловер pg2, генерація даних, потім другий фоловер pg3
# додається до лідера, який уже працює і приймає записи.
# УВАГА: скрипт видаляє томи попереднього запуску конфігурації A.
. "$PSScriptRoot\common.ps1"

$dir = "setup/add_follower"
$res = "/results/$dir"
# Файл перевіряється до того, як буде видалено попередній кластер.
$containerFile = if ($DataFile) { ConvertTo-ContainerDataPath $DataFile } else { "" }

Step "1. Чистий старт: лідер pg1, фоловер pg2, застосунок"
Invoke-Docker compose -f $SingleCompose --profile late down -v --remove-orphans
New-Item -ItemType Directory -Force -Path "results" | Out-Null
$tStart = [string]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds() - 60)
Invoke-Docker compose -f $SingleCompose up -d --build --wait pg1 pg2 app
AppA notes.admin wait-streaming --leader repa-pg1 --names "pg2" --timeout 180

if ($containerFile) {
    Step "2. Завантаження початкових даних із файлу $DataFile"
    AppA notes.generate --load $containerFile --host repa-pg1 --manifest /results/setup/generate.json
} else {
    Step "2. Генерація початкових даних: $Notes нотаток, seed $Seed"
    AppA notes.generate --notes $Notes --seed $Seed --host repa-pg1 --manifest /results/setup/generate.json
}
AppA notes.admin wait-sync --leader repa-pg1 --timeout 300

Step "3. Навантаження $Rate записів/с на лідера"
Start-Load -App repa-app -Dir $dir -Rate $Rate -Hosts "repa-pg1"
Wait-Seconds $WarmupSeconds "прогрів перед підключенням pg3"
AppA notes.admin counts --nodes "repa-pg1,repa-pg2" --out "$res/counts_before_pg3.json"

Step "4. Підключення другого фоловера pg3 до лідера, що приймає записи"
$t0 = Get-Epoch
Invoke-Docker compose -f $SingleCompose --profile late up -d --wait pg3
AppA notes.admin wait-streaming --leader repa-pg1 --names "pg2,pg3" --timeout 600
$t1 = Get-Epoch
Write-Host "  pg3 підключено: від старту контейнера до стану streaming минуло $([Math]::Round([double]$t1 - [double]$t0, 1)) с"
Wait-Seconds 5 "навантаження після підключення"

Step "5. Зупинка навантаження і звірка"
Stop-Load -App repa-app -Dir $dir
AppA notes.admin wait-sync --leader repa-pg1 --names "pg2,pg3"
# window.acked — скільки записів лідер підтвердив, поки pg3 копіював дані й наздоганяв.
AppA notes.report --dir $res --t0 $t0 --t1 $t1
AppA notes.verify --journal "$res/journal.csv" --nodes $SingleNodes --out "$res/verify.json"
AppA notes.admin compare --nodes $SingleNodes --out "$res/compare.json"
AppA notes.admin status --out "$res/status.json"
AppA notes.admin slots --node repa-pg1 --out "$res/slots.json"

Save-Logs repa-pg1 $tStart $dir "pg1.log"
Save-Logs repa-pg2 $tStart $dir "pg2.log"
Save-Logs repa-pg3 $tStart $dir "pg3.log"

Step "Конфігурація A готова. Результати: results\setup"
