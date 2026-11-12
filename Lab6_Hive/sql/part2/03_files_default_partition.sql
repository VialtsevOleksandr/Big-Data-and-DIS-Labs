SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 3. Кількість файлів, з яких читаються рядки.
SELECT count(DISTINCT INPUT__FILE__NAME) AS files FROM tx_day_naive;
SELECT count(DISTINCT INPUT__FILE__NAME) AS files FROM tx_day;

-- Крок 4. Куди потрапили рядки без дати.
SHOW PARTITIONS tx_day;
SELECT dt, count(*) AS rows_cnt, count(ts) AS ts_not_null, sum(amount) AS amount_sum
FROM tx_day
WHERE dt IS NULL
GROUP BY dt;
