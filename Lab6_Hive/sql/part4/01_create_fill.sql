SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 1. Транзакційна таблиця, заповнена з tx_orc.
DROP TABLE IF EXISTS tx_acid;
CREATE TABLE tx_acid (
  user_id  INT,
  ts       TIMESTAMP,
  category STRING,
  amount   DECIMAL(10,2)
)
STORED AS ORC
TBLPROPERTIES ('transactional' = 'true');

INSERT INTO tx_acid SELECT * FROM tx_orc;

-- Два рядки U з amount IS NULL і правильною датою.
SELECT ROW__ID, INPUT__FILE__NAME, user_id, ts, category, amount
FROM tx_acid
WHERE user_id = 1823 AND amount IS NULL AND ts IS NOT NULL
ORDER BY ts
LIMIT 2;
