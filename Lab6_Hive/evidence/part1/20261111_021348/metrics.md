# Метрики: evidence/part1/20261111_021348

## 00_env.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT version()` | 1 | — | 0,168 |

## 01_reset.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `DROP TABLE IF EXISTS tx_str` | 0 | — | 0,278 |
| `DROP TABLE IF EXISTS tx_raw` | 0 | — | 0,022 |
| `DROP TABLE IF EXISTS tx_orc` | 0 | — | 0,018 |

## 02_tx_raw.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `CREATE EXTERNAL TABLE tx_raw ( user_id INT, ts STRING, category STRING, amount DECIMAL(...` | 0 | — | 0,262 |
| `LOAD DATA LOCAL INPATH '/data/transactions.csv' INTO TABLE tx_raw` | 0 | — | 10,248 |
| `SELECT count(*) AS all_rows, count(amount) AS amount_not_null, sum(CASE WHEN cast(repla...` | 1 | 8 000 000 | 20,662 |
| `SELECT r.*, cast(replace(r.ts, 'T', ' ') AS TIMESTAMP) AS ts_cast FROM tx_raw r WHERE (...` | 3 | 8 000 000 | 7,848 |

## 03_tx_str.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `CREATE EXTERNAL TABLE tx_str ( user_id STRING, ts STRING, category STRING, amount STRIN...` | 0 | — | 0,059 |
| `SELECT count(*) AS all_rows, sum(CASE WHEN amount = 'N/A' THEN 1 ELSE 0 END) AS na_amou...` | 1 | 8 000 000 | 27,334 |
| `SELECT s.* FROM tx_str s WHERE (s.user_id = '1823' AND s.ts = '2026-02-08T14:40:21') OR...` | 3 | 8 000 000 | 6,713 |
| `DROP TABLE tx_str` | 0 | — | 0,331 |

## 04_analyze.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `ANALYZE TABLE tx_raw COMPUTE STATISTICS` | 0 | 8 000 000 | 4,508 |
| `SHOW TBLPROPERTIES tx_raw('numRows')` | 1 | — | 0,057 |
| `SHOW TBLPROPERTIES tx_raw` | 9 | — | 0,035 |

## 05_late_day.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `SELECT count(*) AS all_rows FROM tx_raw` | 1 | 8 009 288 | 4,076 |
| `SELECT INPUT__FILE__NAME AS file, count(*) AS rows_in_file FROM tx_raw GROUP BY INPUT__...` | 2 | 8 010 000 | 5,938 |
| `SHOW TBLPROPERTIES tx_raw('numRows')` | 1 | — | 0,036 |
| `EXPLAIN SELECT category, count(*) FROM tx_raw GROUP BY category` | 23 | — | 0,100 |

## 06_tx_orc.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `CREATE TABLE tx_orc STORED AS ORC AS SELECT user_id, cast(replace(ts, 'T', ' ') AS TIME...` | 8000000 | 8 000 000 | 24,448 |
| `DESCRIBE FORMATTED tx_orc` | 36 | — | 0,091 |
| `SELECT count(*) AS all_rows, count(ts) AS ts_not_null, count(amount) AS amount_not_null...` | 1 | 8 000 000 | 1,506 |

## 07_leader_tez.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `EXPLAIN WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FRO...` | 40 | — | 0,224 |
| `WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FROM tx_orc...` | 91 | 8 000 000 | 4,361 |
| `WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FROM tx_orc...` | 91 | 8 000 000 | 3,951 |
| `WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FROM tx_orc...` | 91 | 8 000 000 | 4,068 |

Повтори `WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FROM tx_orc...`: 4,361; 3,951; 4,068 → медіана **4,068** с

## 08_leader_mr.log

| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |
| --- | --- | --- | --- |
| `EXPLAIN WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FRO...` | 126 | — | 0,210 |
| `WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FROM tx_orc...` | 91 | — | 5,011 |
| `WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FROM tx_orc...` | 91 | — | 4,775 |
| `WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FROM tx_orc...` | 91 | — | 4,722 |

Повтори `WITH daily AS ( SELECT to_date(ts) AS day, category, sum(amount) AS revenue FROM tx_orc...`: 5,011; 4,775; 4,722 → медіана **4,775** с

