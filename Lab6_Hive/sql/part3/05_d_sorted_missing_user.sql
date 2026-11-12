SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- г) tx_sorted, неіснуючий user_id

-- Замір 1
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 999999;

-- Замір 2
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 999999;

-- Замір 3
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_sorted WHERE user_id = 999999;
