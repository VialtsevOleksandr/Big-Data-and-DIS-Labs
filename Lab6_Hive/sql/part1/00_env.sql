-- Версія Hive і налаштування, на які спираються питання звіту.
SELECT version();
SET hive.execution.engine;
SET hive.query.results.cache.enabled;
SET hive.compute.query.using.stats;
SET hive.stats.autogather;
SET hive.create.as.acid;
SET hive.txn.manager;
SET hive.support.concurrency;
SET hive.compactor.initiator.on;
SET hive.compactor.worker.threads;
SET hive.exec.dynamic.partition.mode;
SET hive.exec.max.dynamic.partitions;
SET hive.exec.max.dynamic.partitions.pernode;
SET hive.optimize.index.filter;
SET hive.metastore.warehouse.dir;
SET hive.metastore.warehouse.external.dir;
