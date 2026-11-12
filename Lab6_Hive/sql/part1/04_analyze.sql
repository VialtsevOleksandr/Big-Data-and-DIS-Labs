-- Крок 4. Базова статистика таблиці в metastore.
ANALYZE TABLE tx_raw COMPUTE STATISTICS;
SHOW TBLPROPERTIES tx_raw('numRows');
SHOW TBLPROPERTIES tx_raw;
