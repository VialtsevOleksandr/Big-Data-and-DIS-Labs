param(
    [int]$Iterations = 10000,   # пар «запис на лідера + читання з фоловера» у кожній фазі
    [string]$Delay = "1s",      # штучна затримка відтворення на фоловері pg2
    [int]$RawWaitMs = 10        # read-after-write: скільки читання чекає фоловера, перш ніж піти на лідера
)

# Експеримент 1: відставання реплікації і читання власних записів (конфігурація A, асинхронно).
# Чотири фази: без затримки і з затримкою, у кожній — звичайне читання (naive) і read-after-write (raw).
. "$PSScriptRoot\common.ps1"

Assert-SingleTopology
$tStart = Get-Epoch
AppA notes.admin reset --node repa-pg1 synchronous_standby_names
AppA notes.admin reset --node repa-pg2 recovery_min_apply_delay

try {
    Step "1. Без затримки, читання з фоловера"
    AppA notes.exp1 --mode naive --iterations $Iterations --leader repa-pg1 --follower repa-pg2 `
        --dir /results/exp1/1_naive_nodelay

    Step "2. Без затримки, read-after-write"
    AppA notes.exp1 --mode raw --iterations $Iterations --leader repa-pg1 --follower repa-pg2 `
        --raw-wait-ms $RawWaitMs --dir /results/exp1/2_raw_nodelay

    Step "3. Затримка відтворення $Delay на pg2 (recovery_min_apply_delay), читання з фоловера"
    AppA notes.admin set --node repa-pg2 recovery_min_apply_delay $Delay
    AppA notes.exp1 --mode naive --iterations $Iterations --leader repa-pg1 --follower repa-pg2 `
        --dir /results/exp1/3_naive_delay

    Step "4. Затримка $Delay, read-after-write"
    AppA notes.exp1 --mode raw --iterations $Iterations --leader repa-pg1 --follower repa-pg2 `
        --raw-wait-ms $RawWaitMs --dir /results/exp1/4_raw_delay
}
finally {
    Step "Прибирання: затримку знято"
    AppA notes.admin reset --node repa-pg2 recovery_min_apply_delay
}

AppA notes.admin wait-sync --leader repa-pg1
Save-Logs repa-pg2 $tStart "exp1" "pg2.log"
Step "Експеримент 1 завершено. Результати: results\exp1\*\summary.json"
