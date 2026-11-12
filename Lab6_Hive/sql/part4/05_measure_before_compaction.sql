SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Запит-агрегат за однією категорією.

-- Замір 1
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'books';

-- Замір 2
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'books';

-- Замір 3
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_acid WHERE category = 'books';
