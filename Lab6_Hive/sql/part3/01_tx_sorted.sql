SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

CREATE TABLE tx_sorted STORED AS ORC AS SELECT * FROM tx_orc ORDER BY user_id;
