#!/usr/bin/env bash
# Частина 2: партиції за днем, наївний запис і DISTRIBUTE BY, partition pruning.
set -euo pipefail
source /lab/scripts/common.sh
init_run part2
NAIVE=$WH/tx_day_naive
DAY=$WH/tx_day
D=2026-02-15

# Скільки каталогів-партицій, файлів даних (без службових .crc) і який розмір.
dir_stats() {
  echo "Каталогів партицій: $(find "$1" -mindepth 1 -maxdepth 1 -type d | wc -l)"
  echo "Файлів даних:       $(find "$1" -type f ! -name '.*' | wc -l)"
  du -sh "$1"; du -sb "$1"
}
export -f dir_stats

wait_hive
hive_sql 00_settings

# Кроки 1–2: наївний запис і запис із DISTRIBUTE BY dt.
hive_sql 01_naive
step_sh 01_naive_dir "ls $NAIVE && dir_stats $NAIVE && ls -la $NAIVE/dt=$D"
hive_sql 02_distribute
step_sh 02_distribute_dir "ls $DAY && dir_stats $DAY && ls -la $DAY/dt=$D"

# Кроки 3–4: кількість файлів за INPUT__FILE__NAME і рядки без дати.
hive_sql 03_files_default_partition
step_sh 03_default_partition_dir "ls -la $DAY/dt=__HIVE_DEFAULT_PARTITION__"

# Крок 5: виручка за категоріями за день D з трьома фільтрами.
hive_sql 04_filter_dt
hive_sql 05_filter_to_date
hive_sql 06_filter_month_day

# Крок 6: той самий запит без фільтра на обох таблицях.
hive_sql 07_full_tx_day
hive_sql 08_full_tx_day_naive

echo "Частина 2 готова: evidence/part2/$RUN_ID"
