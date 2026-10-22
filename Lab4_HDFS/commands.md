# Послідовність команд експериментів HDFS

Дата запуску: 22.10.2026. Час у логах — UTC.
Робоча папка: `C:\Antigravity_Projects\BigData_DIS\Lab4_HDFS`.
Команди та наявні результати зібрані в [evidence/experiments.md](evidence/experiments.md).

Це послідовність команд із нашої роботи.
Кластер підготовано раніше: реплікація 3, блок 134217728 байтів,
heartbeat 3 с, recheck 30000 мс, розрахунковий таймаут 90 с.

## 1. Перехід у папку та перевірка кластера

```powershell
cd C:\Antigravity_Projects\BigData_DIS\Lab4_HDFS
docker compose ps
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs dfsadmin -safemode get
```

Додаткові перевірки налаштувань.

```powershell
docker compose exec -T namenode hdfs getconf -confKey dfs.replication
docker compose exec -T namenode hdfs getconf -confKey dfs.blocksize
docker compose exec -T namenode hdfs getconf -confKey dfs.heartbeat.interval
docker compose exec -T namenode hdfs getconf -confKey dfs.namenode.heartbeat.recheck-interval
```

## 2. Завантаження оригіналу в HDFS

```powershell
[DateTimeOffset]::UtcNow.ToString('o')
docker compose exec -T namenode hdfs dfs -mkdir -p /lab/hdfs
docker compose exec -T namenode hdfs dfs -put /data/transactions.csv /lab/hdfs/transactions.csv
```

`/data/transactions.csv` — оригінал, підключений до контейнерів лише для читання.
`/lab/hdfs/transactions.csv` — копія в HDFS.

## 3. Початкова карта блоків

```powershell
[DateTimeOffset]::UtcNow.ToString('o')
docker compose exec -T namenode hdfs dfs -ls /lab/hdfs
docker compose exec -T namenode hdfs dfs -du -s -h /lab/hdfs/transactions.csv
docker compose exec -T namenode hdfs fsck /lab/hdfs/transactions.csv -files -blocks -locations
docker compose exec -T namenode hdfs dfsadmin -report
```

## 4. Локальні блоки та .meta на dn2

```powershell
docker compose exec -T dn2 find /hadoop/dfs/data -type f -name 'blk_*' -printf '%p %s bytes\n'
```

## 5. Запис журналу NameNode в другому терміналі

```powershell
cd C:\Antigravity_Projects\BigData_DIS\Lab4_HDFS
docker compose logs --since 1m --follow --timestamps --no-color namenode 2>&1 | Tee-Object .\evidence\02_node_failure\namenode.log
```

Ця команда працювала паралельно з основним терміналом.
Журнал також містить повернення вузла та події третього експерименту.
Для завершення спостереження використовується Ctrl+C; контейнер це не зупиняє.

## 6. Зупинка dn2 та фіксація часу

```powershell
[DateTimeOffset]::UtcNow.ToString('o')
docker stop lab4-hdfs-dn2-1
docker inspect lab4-hdfs-dn2-1 --format '{{.State.FinishedAt}}'
```

`docker inspect` був виконаний повторно при уточненні потрібного виводу.

## 7. Спостереження за втратою вузла

```powershell
docker compose exec -T namenode hdfs dfsadmin -report
```

Перевірку стану повторювали під час очікування. У надісланому ранньому звіті
NameNode ще вважав dn2 живим. Наступний fsck уже показав відновлення:

```powershell
docker compose exec -T namenode hdfs fsck /lab/hdfs/transactions.csv -files -blocks -locations
```

## 8. Вибір потрібних рядків журналу

```powershell
docker compose logs --no-color --since "2026-10-22T19:29:00Z" --until "2026-10-22T19:32:00Z" namenode 2>&1 | Select-String -Pattern 'heartbeat|dead|1073741825|1073741826|1073741827'
```

## 9. Повернення dn2

```powershell
docker start lab4-hdfs-dn2-1
```

Після очікування приблизно однієї хвилини:

```powershell
docker compose exec -T namenode hdfs fsck /lab/hdfs/transactions.csv -files -blocks -locations
docker compose exec -T dn2 find /hadoop/dfs/data -type f -name 'blk_*' -printf '%p %s bytes\n'
```

## 10. Контрольна сума оригіналу

```powershell
docker compose exec -T dn2 md5sum /data/transactions.csv
```

## 11. Пошкодження однієї репліки на dn2

```powershell
$block = '/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741825'
docker compose exec -T dn2 sh -c "printf 'CORRUPTED!' | dd of=$block bs=1 seek=12345 conv=notrunc"
```

Зміщення 12345, заміна 10 байтів, файл .meta не змінювався.
Вивід dd не надсилався; подальший вивід клієнта підтвердив пошкодження цього блоку.

## 12. Читання з того самого dn2 та перевірка копії

```powershell
docker compose exec -T dn2 hdfs dfs -get /lab/hdfs/transactions.csv /tmp/transactions-readback.csv 2>&1 | Tee-Object .\evidence\03_corruption\client.log
docker compose exec -T dn2 md5sum /tmp/transactions-readback.csv
```

## 13. Реакція NameNode та остаточний fsck

```powershell
docker compose logs --no-color --since "2026-10-22T19:40:00Z" namenode 2>&1 | Select-String -Pattern '1073741825|corrupt|bad block'
docker compose exec -T namenode hdfs fsck /lab/hdfs/transactions.csv -files -blocks -locations
```

## 14. Топологія кластера

```powershell
docker compose exec -T namenode hdfs dfsadmin -printTopology
```
