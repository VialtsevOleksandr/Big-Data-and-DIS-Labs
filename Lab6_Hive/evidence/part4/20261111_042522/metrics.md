# Метрики: evidence/part4/20261111_042522

## 01_create_fill.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `DROP TABLE IF EXISTS tx_acid` | 0 | — | 0,322 |
| `CREATE TABLE tx_acid ( user_id INT, ts TIMESTAMP, category STRING, amount DECIMAL(10,2)...` | 0 | — | 0,094 |
| `INSERT INTO tx_acid SELECT * FROM tx_orc` | 8000000 | 8 000 000 | 22,254 |
| `SELECT ROW__ID, INPUT__FILE__NAME, user_id, ts, category, amount FROM tx_acid WHERE use...` | 2 | 8 000 000 | 4,629 |

## 02_update.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `UPDATE tx_acid SET amount = 0 WHERE amount IS NULL` | 266667 | 8 000 000 | 4,051 |

## 03_delete.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `DELETE FROM tx_acid WHERE ts IS NULL` | 133333 | 7 733 333 | 2,435 |
| `SELECT count(*) AS all_rows FROM tx_acid` | 1 | 7 866 667 | 1,673 |
| `SELECT ROW__ID, INPUT__FILE__NAME, user_id, ts, category, amount FROM tx_acid WHERE use...` | 2 | 266 667 | 1,134 |

## 04_ten_updates.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 1000` | 922 | 5 460 646 | 2,795 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 1500` | 835 | 7 865 745 | 2,169 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 2500` | 840 | 7 864 910 | 2,170 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 3500` | 867 | 7 864 070 | 2,294 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 4500` | 874 | 7 863 203 | 2,237 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 5500` | 886 | 7 862 329 | 2,251 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 6500` | 855 | 7 861 443 | 2,251 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 7500` | 915 | 7 860 588 | 2,232 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 8500` | 863 | 7 859 673 | 2,177 |
| `UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 9999` | 880 | 5 311 380 | 2,002 |

## 05_measure_before_compaction.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'b...` | 1 | 7 866 654 | 1,918 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'b...` | 1 | 7 866 654 | 1,680 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'b...` | 1 | 7 866 654 | 1,700 |

Повтори `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'b...`: 1,918; 1,680; 1,700 → медіана **1,700** с

## 06_compact.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `ALTER TABLE tx_acid COMPACT 'major' AND WAIT` | 0 | — | 30,059 |
| `SHOW COMPACTIONS` | 1 | — | 0,024 |

## 07_measure_after_compaction.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'b...` | 1 | 7 866 654 | 1,711 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'b...` | 1 | 7 866 654 | 1,370 |
| `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'b...` | 1 | 7 866 654 | 1,351 |

Повтори `SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'b...`: 1,711; 1,370; 1,351 → медіана **1,370** с

