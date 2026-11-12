SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 5. late_day.csv уже скопійовано в каталог командою cp.
SELECT count(*) AS all_rows FROM tx_raw;

-- Скільки рядків прийшло з кожного файлу каталогу.
SELECT INPUT__FILE__NAME AS file, count(*) AS rows_in_file
FROM tx_raw
GROUP BY INPUT__FILE__NAME;

SHOW TBLPROPERTIES tx_raw('numRows');

EXPLAIN SELECT category, count(*) FROM tx_raw GROUP BY category;
