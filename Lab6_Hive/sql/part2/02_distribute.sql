SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

-- Крок 2. Те саме, але рядки одного дня йдуть в одне reduce-завдання.
DROP TABLE IF EXISTS tx_day;
CREATE TABLE tx_day
(
  user_id  INT,
  ts       TIMESTAMP,
  category STRING,
  amount   DECIMAL(10,2)
)
PARTITIONED BY (dt DATE)
STORED AS ORC;

EXPLAIN
INSERT OVERWRITE TABLE tx_day PARTITION (dt)
SELECT user_id, ts, category, amount, to_date(ts) AS dt
FROM tx_orc
DISTRIBUTE BY dt;

INSERT OVERWRITE TABLE tx_day PARTITION (dt)
SELECT user_id, ts, category, amount, to_date(ts) AS dt
FROM tx_orc
DISTRIBUTE BY dt;
