SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 5. Виручка за категоріями за день D = 2026-02-15: фільтр за функціями від стовпця партиції.
EXPLAIN
SELECT category, sum(amount) AS revenue
FROM tx_day
WHERE month(dt) = 2 AND day(dt) = 15
GROUP BY category
ORDER BY category;

-- Замір 1
SELECT category, sum(amount) AS revenue
FROM tx_day
WHERE month(dt) = 2 AND day(dt) = 15
GROUP BY category
ORDER BY category;

-- Замір 2
SELECT category, sum(amount) AS revenue
FROM tx_day
WHERE month(dt) = 2 AND day(dt) = 15
GROUP BY category
ORDER BY category;

-- Замір 3
SELECT category, sum(amount) AS revenue
FROM tx_day
WHERE month(dt) = 2 AND day(dt) = 15
GROUP BY category
ORDER BY category;
