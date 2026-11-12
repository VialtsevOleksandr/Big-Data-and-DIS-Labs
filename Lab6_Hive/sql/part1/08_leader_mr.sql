SET hive.query.results.cache.enabled=false;

-- Крок 7. Той самий запит на MapReduce.
SET hive.execution.engine=mr;

EXPLAIN
WITH daily AS (
  SELECT to_date(ts) AS day, category, sum(amount) AS revenue
  FROM tx_orc
  GROUP BY to_date(ts), category
),
ranked AS (
  SELECT day, category, revenue,
         row_number() OVER (PARTITION BY day ORDER BY revenue DESC, category) AS rn
  FROM daily
)
SELECT day, category, revenue
FROM ranked
WHERE rn = 1
ORDER BY day;

-- Замір 1
WITH daily AS (
  SELECT to_date(ts) AS day, category, sum(amount) AS revenue
  FROM tx_orc
  GROUP BY to_date(ts), category
),
ranked AS (
  SELECT day, category, revenue,
         row_number() OVER (PARTITION BY day ORDER BY revenue DESC, category) AS rn
  FROM daily
)
SELECT day, category, revenue
FROM ranked
WHERE rn = 1
ORDER BY day;

-- Замір 2
WITH daily AS (
  SELECT to_date(ts) AS day, category, sum(amount) AS revenue
  FROM tx_orc
  GROUP BY to_date(ts), category
),
ranked AS (
  SELECT day, category, revenue,
         row_number() OVER (PARTITION BY day ORDER BY revenue DESC, category) AS rn
  FROM daily
)
SELECT day, category, revenue
FROM ranked
WHERE rn = 1
ORDER BY day;

-- Замір 3
WITH daily AS (
  SELECT to_date(ts) AS day, category, sum(amount) AS revenue
  FROM tx_orc
  GROUP BY to_date(ts), category
),
ranked AS (
  SELECT day, category, revenue,
         row_number() OVER (PARTITION BY day ORDER BY revenue DESC, category) AS rn
  FROM daily
)
SELECT day, category, revenue
FROM ranked
WHERE rn = 1
ORDER BY day;

-- Повертаємо Tez.
SET hive.execution.engine=tez;
