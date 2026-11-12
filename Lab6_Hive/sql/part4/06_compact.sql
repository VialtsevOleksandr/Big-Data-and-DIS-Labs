SET hive.compactor.initiator.on;

-- Крок 5. Major compaction з очікуванням завершення.
ALTER TABLE tx_acid COMPACT 'major' AND WAIT;
SHOW COMPACTIONS;
