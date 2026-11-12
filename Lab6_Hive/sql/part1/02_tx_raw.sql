SET hive.query.results.cache.enabled=false;
-- count(*) рахуємо читанням файлів, а не зі статистики metastore.
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 1. External-таблиця над каталогом у сховищі.
CREATE EXTERNAL TABLE tx_raw (
  user_id  INT,
  ts       STRING,
  category STRING,
  amount   DECIMAL(10,2)
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/opt/hive/data/warehouse/tx_raw';

LOAD DATA LOCAL INPATH '/data/transactions.csv' INTO TABLE tx_raw;

-- Крок 2. Усі рядки, непорожні суми, правильні дати, сума.
SELECT
  count(*)      AS all_rows,
  count(amount) AS amount_not_null,
  sum(CASE WHEN cast(replace(ts, 'T', ' ') AS TIMESTAMP) IS NOT NULL THEN 1 ELSE 0 END) AS valid_ts,
  sum(amount)   AS amount_sum
FROM tx_raw;

-- Як Hive показує по одному рядку кожного дефекту (рядки 160, 57, 147 файлу).
SELECT r.*, cast(replace(r.ts, 'T', ' ') AS TIMESTAMP) AS ts_cast
FROM tx_raw r
WHERE (r.user_id = 1823 AND r.ts = '2026-02-08T14:40:21')
   OR (r.user_id = 6140 AND r.ts = '2026-02-28T11:42:22')
   OR (r.user_id = 4490 AND r.ts = '2026/03/02 01:39:29');
