SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 6. Перезапис у ORC зі збереженням усіх рядків.
CREATE TABLE tx_orc STORED AS ORC AS
SELECT user_id, cast(replace(ts, 'T', ' ') AS TIMESTAMP) AS ts, category, amount
FROM tx_raw;

DESCRIBE FORMATTED tx_orc;

SELECT
  count(*)      AS all_rows,
  count(ts)     AS ts_not_null,
  count(amount) AS amount_not_null,
  sum(amount)   AS amount_sum
FROM tx_orc;
