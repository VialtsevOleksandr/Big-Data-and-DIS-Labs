SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- в) tx_sorted, одна категорія

-- Замір 1
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE category = 'books';

-- Замір 2
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE category = 'books';

-- Замір 3
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE category = 'books';
