param(
    [ValidateSet("oddeven", "naive")]
    [string]$IdMode = "oddeven",     # oddeven: лідери видають непарні/парні id; naive: однакові послідовності
    [int]$PartitionSeconds = 300,    # тривалість розриву між лідерами
    [int]$Inserts = 20,              # нових нотаток на кожному лідері під час розриву
    [int]$SettleSeconds = 60,        # скільки чекати обміну змінами після відновлення
    [switch]$Resolve                 # після спостереження зняти зупинку реплікації (для режиму naive)
)

# Експеримент 5: конфлікти записів між двома лідерами (конфігурація B).
# Зазвичай запускається двічі:  .\run_exp5.ps1 -IdMode naive -Resolve   і   .\run_exp5.ps1 -IdMode oddeven
. "$PSScriptRoot\common.ps1"

$dir = "exp5/$IdMode"
$res = "/results/$dir"

Step "1. Перевірка реплікації в обидва боки й підготовка нотаток (режим id: $IdMode)"
AppB notes.exp5 prepare --id-mode $IdMode --dir $res
AppB notes.admin compare --nodes $MultiNodes --until-equal 30 --out "$res/compare_before.json"

try {
    Step "2. Розрив зв'язку між лідерами на $PartitionSeconds с"
    $t0 = Get-Epoch "repb-app"
    Write-Host "  команда збою: docker network disconnect repb-repl repb-pg2"
    Invoke-Docker network disconnect repb-repl repb-pg2
    Wait-Seconds 15 "вузли помічають втрату зв'язку"

    Step "3. Конфліктні зміни на обох лідерах"
    AppB notes.exp5 conflict --dir $res --inserts $Inserts
    $rest = $PartitionSeconds - 20
    if ($rest -gt 0) { Wait-Seconds $rest "лідери працюють окремо" }

    Step "4. Відновлення зв'язку"
    Write-Host "  відновлення: docker network connect repb-repl repb-pg2"
    Invoke-Docker network connect repb-repl repb-pg2
}
catch {
    cmd /c "docker network connect repb-repl repb-pg2 >nul 2>&1"
    throw
}

Step "5. Що фактично сталося"
AppB notes.exp5 observe --dir $res --settle $SettleSeconds
Save-Logs repb-pg1 $t0 $dir "pg1.log"
Save-Logs repb-pg2 $t0 $dir "pg2.log"

if ($Resolve) {
    Step "6. Зняття зупинки реплікації: пропуск змін, які не вдалося застосувати"
    $t2 = Get-Epoch "repb-app"
    AppB notes.exp5 resolve --dir $res
    AppB notes.exp5 observe --dir $res --settle 20 --name observe_after_resolve
    Save-Logs repb-pg1 $t2 $dir "pg1_after_resolve.log"
    Save-Logs repb-pg2 $t2 $dir "pg2_after_resolve.log"
}

Step "Експеримент 5 ($IdMode) завершено. Результати: results\exp5\$IdMode"
