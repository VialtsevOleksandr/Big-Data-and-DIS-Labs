SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 2. Видалення рядків без дати.
DELETE FROM tx_acid WHERE ts IS NULL;

-- Крок 3. Кількість рядків і ROW__ID тих самих двох рядків (тепер amount = 0).
SELECT count(*) AS all_rows FROM tx_acid;
SELECT ROW__ID, INPUT__FILE__NAME, user_id, ts, category, amount
FROM tx_acid
WHERE user_id = 1823 AND amount = 0
ORDER BY ts
LIMIT 2;
