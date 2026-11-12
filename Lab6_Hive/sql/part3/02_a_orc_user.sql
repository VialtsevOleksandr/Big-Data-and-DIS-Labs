SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- а) tx_orc, user_id = U

-- Замір 1
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_orc WHERE user_id = 1823;

-- Замір 2
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_orc WHERE user_id = 1823;

-- Замір 3
SELECT count(*) AS rows_cnt, sum(amount) AS amount_sum FROM tx_orc WHERE user_id = 1823;
