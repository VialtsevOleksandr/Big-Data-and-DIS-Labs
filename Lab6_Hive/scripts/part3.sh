#!/usr/bin/env bash
# Частина 3: порядок рядків і статистика ORC. U = 1823.
set -euo pipefail
source /lab/scripts/common.sh
init_run part3

wait_hive
hive_sql 01_tx_sorted
step_sh 01_dirs "ls -la $WH/tx_orc $WH/tx_sorted && du -sh $WH/tx_orc $WH/tx_sorted && du -sb $WH/tx_orc $WH/tx_sorted"

hive_sql 02_a_orc_user
hive_sql 03_b_sorted_user
hive_sql 04_c_sorted_category
hive_sql 05_d_sorted_missing_user
hive_sql 06_e_sorted_user_no_index

# Статистика всередині файлів, стовпець 1 = user_id.
step_sh 07_orcfiledump_tx_orc "/opt/hive/bin/hive --orcfiledump --rowindex 1 $WH/tx_orc/000000_0"
step_sh 08_orcfiledump_tx_sorted "/opt/hive/bin/hive --orcfiledump --rowindex 1 $WH/tx_sorted/000000_0"

echo "Частина 3 готова: evidence/part3/$RUN_ID"
