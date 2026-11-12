SET hive.query.results.cache.enabled=false;
SET hive.tez.exec.print.summary=true;

-- Крок 3. Друга external-таблиця над тим самим каталогом, усі стовпці STRING.
CREATE EXTERNAL TABLE tx_str (
  user_id  STRING,
  ts       STRING,
  category STRING,
  amount   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/opt/hive/data/warehouse/tx_raw';

SELECT
  count(*) AS all_rows,
  sum(CASE WHEN amount = 'N/A' THEN 1 ELSE 0 END)                  AS na_amount,
  sum(CASE WHEN amount IS NULL THEN 1 ELSE 0 END)                  AS missing_amount,
  sum(CASE WHEN ts LIKE '____/__/__ __:__:__' THEN 1 ELSE 0 END)   AS slash_date,
  sum(CASE WHEN cast(replace(ts, 'T', ' ') AS TIMESTAMP) IS NULL
           THEN 1 ELSE 0 END)                                      AS invalid_ts,
  sum(CASE WHEN cast(replace(ts, 'T', ' ') AS TIMESTAMP) IS NULL
           THEN cast(amount AS DECIMAL(10,2)) END)                 AS invalid_ts_amount
FROM tx_str;

-- Ті самі три дефектні рядки у вигляді рядків.
SELECT s.*
FROM tx_str s
WHERE (s.user_id = '1823' AND s.ts = '2026-02-08T14:40:21')
   OR (s.user_id = '6140' AND s.ts = '2026-02-28T11:42:22')
   OR (s.user_id = '4490' AND s.ts = '2026/03/02 01:39:29');

DROP TABLE tx_str;
