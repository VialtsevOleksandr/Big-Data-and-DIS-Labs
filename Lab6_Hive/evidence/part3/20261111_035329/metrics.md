# Метрики: evidence/part3/20261111_035329

## 01_tx_sorted.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `CREATE TABLE tx_sorted STORED AS ORC AS SELECT * FROM tx_orc ORDER BY user_id` | 8000000 | 8 000 000 | 18,912 |

## 02_a_orc_user.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_orc WHERE user_id = 1823` | 1 | 8 000 000 | 1,852 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_orc WHERE user_id = 1823` | 1 | 8 000 000 | 1,306 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_orc WHERE user_id = 1823` | 1 | 8 000 000 | 1,222 |

Повтори `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_orc WHERE user_id = 1823`: 1,852; 1,306; 1,222 → медіана **1,306** с

## 03_b_sorted_user.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 1823` | 1 | 10 000 | 0,702 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 1823` | 1 | 10 000 | 0,515 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 1823` | 1 | 10 000 | 0,491 |

Повтори `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 1823`: 0,702; 0,515; 0,491 → медіана **0,515** с

## 04_c_sorted_category.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE category = ...` | 1 | 8 000 000 | 1,071 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE category = ...` | 1 | 8 000 000 | 0,902 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE category = ...` | 1 | 8 000 000 | 0,924 |

Повтори `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE category = ...`: 1,071; 0,902; 0,924 → медіана **0,924** с

## 05_d_sorted_missing_user.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 9...` | 1 | — | 0,484 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 9...` | 1 | — | 0,307 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 9...` | 1 | — | 0,300 |

Повтори `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 9...`: 0,484; 0,307; 0,300 → медіана **0,307** с

## 06_e_sorted_user_no_index.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 1823` | 1 | 8 000 000 | 0,904 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 1823` | 1 | 8 000 000 | 0,739 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 1823` | 1 | 8 000 000 | 0,741 |

Повтори `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 1823`: 0,904; 0,739; 0,741 → медіана **0,741** с

