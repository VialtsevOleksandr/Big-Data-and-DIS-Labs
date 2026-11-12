# Метрики: evidence/part2/20261111_033714

## 01_naive.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `DROP TABLE IF EXISTS tx_day_naive` | 0 | — | 0,033 |
| `CREATE TABLE tx_day_naive ( user_id INT, ts TIMESTAMP, category STRING, amount DECIMAL(...` | 0 | — | 0,073 |
| `EXPLAIN INSERT OVERWRITE TABLE tx_day_naive PARTITION (dt) SELECT user_id, ts, category...` | 34 | — | 0,246 |
| `INSERT OVERWRITE TABLE tx_day_naive PARTITION (dt) SELECT user_id, ts, category, amount...` | 8000000 | 8 000 000 | 20,556 |

## 02_distribute.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `DROP TABLE IF EXISTS tx_day` | 0 | — | 0,022 |
| `CREATE TABLE tx_day ( user_id INT, ts TIMESTAMP, category STRING, amount DECIMAL(10,2) ...` | 0 | — | 0,056 |
| `EXPLAIN INSERT OVERWRITE TABLE tx_day PARTITION (dt) SELECT user_id, ts, category, amou...` | 34 | — | 0,223 |
| `INSERT OVERWRITE TABLE tx_day PARTITION (dt) SELECT user_id, ts, category, amount, to_d...` | 8000000 | 8 000 000 | 28,766 |

## 03_files_default_partition.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(DISTINCT INPUT__FILE__NAME) AS files FROM tx_day_naive` | 1 | 8 000 000 | 6,399 |
| `SELECT count(DISTINCT INPUT__FILE__NAME) AS files FROM tx_day` | 1 | 8 000 000 | 5,668 |
| `SHOW PARTITIONS tx_day` | 91 | — | 0,069 |
| `SELECT dt, count(*) AS rows_cnt, count(ts) AS ts_not_null, sum(amount) AS amount_sum FR...` | 1 | 133 333 | 0,955 |

## 04_filter_dt.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `EXPLAIN SELECT category, sum(amount) AS revenue FROM tx_day WHERE dt = '2026-02-15' GRO...` | 28 | — | 0,230 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE dt = '2026-02-15' GROUP BY ca...` | 10 | 87 540 | 0,871 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE dt = '2026-02-15' GROUP BY ca...` | 10 | 87 540 | 0,716 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE dt = '2026-02-15' GROUP BY ca...` | 10 | 87 540 | 0,825 |

Повтори `SELECT category, sum(amount) AS revenue FROM tx_day WHERE dt = '2026-02-15' GROUP BY ca...`: 0,871; 0,716; 0,825 → медіана **0,825** с

## 05_filter_to_date.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `EXPLAIN SELECT category, sum(amount) AS revenue FROM tx_day WHERE to_date(ts) = '2026-0...` | 30 | — | 0,283 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE to_date(ts) = '2026-02-15' GR...` | 10 | 8 000 000 | 3,349 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE to_date(ts) = '2026-02-15' GR...` | 10 | 8 000 000 | 2,679 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE to_date(ts) = '2026-02-15' GR...` | 10 | 8 000 000 | 2,698 |

Повтори `SELECT category, sum(amount) AS revenue FROM tx_day WHERE to_date(ts) = '2026-02-15' GR...`: 3,349; 2,679; 2,698 → медіана **2,698** с

## 06_filter_month_day.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `EXPLAIN SELECT category, sum(amount) AS revenue FROM tx_day WHERE month(dt) = 2 AND day...` | 28 | — | 0,164 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE month(dt) = 2 AND day(dt) = 1...` | 10 | 87 540 | 0,802 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE month(dt) = 2 AND day(dt) = 1...` | 10 | 87 540 | 0,679 |
| `SELECT category, sum(amount) AS revenue FROM tx_day WHERE month(dt) = 2 AND day(dt) = 1...` | 10 | 87 540 | 0,636 |

Повтори `SELECT category, sum(amount) AS revenue FROM tx_day WHERE month(dt) = 2 AND day(dt) = 1...`: 0,802; 0,679; 0,636 → медіана **0,679** с

## 07_full_tx_day.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT category, sum(amount) AS revenue FROM tx_day GROUP BY category ORDER BY category` | 10 | 8 000 000 | 3,098 |
| `SELECT category, sum(amount) AS revenue FROM tx_day GROUP BY category ORDER BY category` | 10 | 8 000 000 | 3,013 |
| `SELECT category, sum(amount) AS revenue FROM tx_day GROUP BY category ORDER BY category` | 10 | 8 000 000 | 2,852 |

Повтори `SELECT category, sum(amount) AS revenue FROM tx_day GROUP BY category ORDER BY category`: 3,098; 3,013; 2,852 → медіана **3,013** с

## 08_full_tx_day_naive.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT category, sum(amount) AS revenue FROM tx_day_naive GROUP BY category ORDER BY ca...` | 10 | 8 000 000 | 3,423 |
| `SELECT category, sum(amount) AS revenue FROM tx_day_naive GROUP BY category ORDER BY ca...` | 10 | 8 000 000 | 3,154 |
| `SELECT category, sum(amount) AS revenue FROM tx_day_naive GROUP BY category ORDER BY ca...` | 10 | 8 000 000 | 3,169 |

Повтори `SELECT category, sum(amount) AS revenue FROM tx_day_naive GROUP BY category ORDER BY ca...`: 3,423; 3,154; 3,169 → медіана **3,169** с

