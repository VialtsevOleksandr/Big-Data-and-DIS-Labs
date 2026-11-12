#!/usr/bin/env bash
# Частина 1: таблиця над брудними файлами, статистика, ORC, Tez і MapReduce.
set -euo pipefail
source /lab/scripts/common.sh
init_run part1
RAW=$WH/tx_raw

wait_hive
hive_sql 00_env
step_sh 00_data "ls -l /data && wc -l /data/transactions.csv /data/late_day.csv"

# Чистий старт. DROP external-таблиці не видаляє файлів, тому каталог
# прибираємо вручну, інакше повторний LOAD подвоїть дані.
hive_sql 01_reset
step_sh 01_reset_dir "rm -rf $RAW && ls -la $WH"

# Кроки 1–2: tx_raw, LOAD DATA, підрахунки.
hive_sql 02_tx_raw
step_sh 02_tx_raw_dir "ls -la $RAW && du -sh $RAW && du -sb $RAW"

# Крок 3: tx_str над тим самим каталогом, дефекти, DROP і каталог після нього.
hive_sql 03_tx_str
step_sh 03_after_drop_tx_str "ls -la $RAW"

# Крок 4: статистика.
hive_sql 04_analyze

# Крок 5: файл повз Hive.
step_sh 05_cp_late_day "cp /data/late_day.csv $RAW/ && ls -la $RAW"
hive_sql 05_late_day
step_sh 05_rm_late_day "rm $RAW/late_day.csv && ls -la $RAW"

# Крок 6: ORC.
hive_sql 06_tx_orc
step_sh 06_tx_orc_dir "ls -la $WH/tx_orc && du -sh $WH/tx_orc && du -sb $WH/tx_orc"

# Крок 7: категорія-лідер дня на Tez і на MapReduce.
hive_sql 07_leader_tez
hive_sql 08_leader_mr

echo "Частина 1 готова: evidence/part1/$RUN_ID"
