SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 6. Повне сканування tx_day_naive без фільтра.

-- Замір 1
SELECT category, sum(amount) AS revenue
FROM tx_day_naive
GROUP BY category
ORDER BY category;

-- Замір 2
SELECT category, sum(amount) AS revenue
FROM tx_day_naive
GROUP BY category
ORDER BY category;

-- Замір 3
SELECT category, sum(amount) AS revenue
FROM tx_day_naive
GROUP BY category
ORDER BY category;
