#!/usr/bin/env bash
# Частина 4: ACID-таблиця, UPDATE/DELETE над незмінними файлами, compaction. U = 1823.
set -euo pipefail
source /lab/scripts/common.sh
init_run part4
ACID=$WH/tx_acid

wait_hive
hive_sql 01_create_fill
step_sh 01_dir_after_fill "ls -laR $ACID"

hive_sql 02_update
step_sh 02_dir_after_update "ls -laR $ACID"

hive_sql 03_delete
step_sh 03_dir_after_delete "ls -laR $ACID"

hive_sql 04_ten_updates
step_sh 04_dir_after_ten_updates "ls -la $ACID && du -sh $ACID && du -sb $ACID"
hive_sql 05_measure_before_compaction

hive_sql 06_compact
step_sh 06_dir_after_compaction "ls -la $ACID && du -sh $ACID && du -sb $ACID"
hive_sql 07_measure_after_compaction

echo "Частина 4 готова: evidence/part4/$RUN_ID"
