SET hive.query.results.cache.enabled=false;
SET hive.compute.query.using.stats=false;
SET hive.tez.exec.print.summary=true;

-- Крок 4. 10 окремих UPDATE для 10 різних користувачів з усього діапазону 1000–9999.
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 1000;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 1500;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 2500;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 3500;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 4500;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 5500;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 6500;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 7500;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 8500;
UPDATE tx_acid SET amount = amount + 1 WHERE user_id = 9999;
