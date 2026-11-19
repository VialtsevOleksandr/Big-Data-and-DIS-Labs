param(
    [int]$Notes = 2000,   # початкових нотаток (пишуться на pg1 і розходяться реплікацією на pg2)
    [int]$Seed = 42,
    [string]$DataFile = ""   # готовий CSV із generate_data.py у папці data\; тоді -Notes і -Seed не діють
)

# Конфігурація B з нуля: два лідери з двосторонньою логічною реплікацією.
# УВАГА: скрипт видаляє томи попереднього запуску конфігурації B.
. "$PSScriptRoot\common.ps1"

$containerFile = if ($DataFile) { ConvertTo-ContainerDataPath $DataFile } else { "" }

Step "1. Чистий старт: лідери pg1 і pg2, застосунок"
Invoke-Docker compose -f $MultiCompose down -v --remove-orphans
New-Item -ItemType Directory -Force -Path "results" | Out-Null
$tStart = [string]([DateTimeOffset]::UtcNow.ToUnixTimeSeconds() - 60)
Invoke-Docker compose -f $MultiCompose up -d --build --wait pg1 pg2 app

Step "2. Підписки в обидва боки (CREATE SUBSCRIPTION ... origin = none)"
AppB notes.multi setup

Step "3. Генератор id: pg1 видає непарні, pg2 — парні"
AppB notes.multi ids --mode oddeven

if ($containerFile) {
    Step "4. Початкові дані з файлу $DataFile на pg1"
    AppB notes.generate --load $containerFile --host repb-pg1 --manifest /results/setup_multi/generate.json
} else {
    Step "4. Початкові дані: $Notes нотаток на pg1, seed $Seed"
    AppB notes.generate --notes $Notes --seed $Seed --host repb-pg1 --manifest /results/setup_multi/generate.json
}

Step "5. Перевірка: дані однакові на обох лідерах"
AppB notes.admin compare --nodes $MultiNodes --until-equal 60 --out /results/setup_multi/compare.json
AppB notes.multi status --out /results/setup_multi/status.json
Save-Logs repb-pg1 $tStart "setup_multi" "pg1.log"
Save-Logs repb-pg2 $tStart "setup_multi" "pg2.log"

Step "Конфігурація B готова. Результати: results\setup_multi"
