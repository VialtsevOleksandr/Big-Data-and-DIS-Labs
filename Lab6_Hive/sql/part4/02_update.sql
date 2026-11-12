SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 2. Виправлення сум.
UPDATE tx_acid SET amount = 0 WHERE amount IS NULL;
