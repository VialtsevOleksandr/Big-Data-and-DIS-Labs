# HDFS — команди та результати експериментів

**Дата:** 22.10.2026. **Час подій:** UTC.

Команди виділено блоками `powershell`, вивід — блоками `text`.
З виводу прибрано повтори, статистику erasure coding, кешу, місткості дисків
і зайві рядки HTTP-запитів. Рядки, перенесені терміналом усередині слова,
з'єднано. ID блоків, адреси, час і значущі лічильники збережено.
Початкові дані, які є лише у вигляді прочитаної карти, позначені окремо;

## Параметри запуску

Конфігурація: [../hadoop.env](../hadoop.env), [../docker-compose.yml](../docker-compose.yml).
Повний перелік команд: [../commands.md](../commands.md).

| Параметр | Значення |
|---|---|
| Hadoop | 3.5.0 |
| Склад кластера | 1 NameNode, 4 DataNode |
| Оригінал у контейнерах | `/data/transactions.csv`, лише для читання |
| Файл HDFS | `/lab/hdfs/transactions.csv` |
| Розмір файлу | 331348600 байтів |
| Розмір повного блоку | 134217728 байтів (128 МіБ) |
| Реплікація | 3 |
| `dfs.heartbeat.interval` | 3 с |
| `dfs.namenode.heartbeat.recheck-interval` | 30000 мс |
| Розрахунковий таймаут | 2 × 30 + 10 × 3 = 90 с |
| Ділянка даних на одну контрольну суму | 512 байтів |
| Тип контрольної суми | CRC32C |

Відповідність адрес та імен із поточного запуску:

| Вузол | Адреса передачі блоків |
|---|---|
| NameNode | `172.21.0.2` (адреса вузла) |
| dn1 | `172.21.0.5:9866` |
| dn2 | `172.21.0.3:9866` |
| dn3 | `172.21.0.6:9866` |
| dn4 | `172.21.0.4:9866` |

Пул блоків цього запуску: `BP-2004503507-172.21.0.2-1792621365166`.

## 1. Завантаження та початкова карта блоків

### 1.1. Перевірка кластера і завантаження

**Команди, надані для ручного виконання:**

```powershell
cd C:\Antigravity_Projects\BigData_DIS\Lab4_HDFS
docker compose ps
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs dfsadmin -safemode get
[DateTimeOffset]::UtcNow.ToString('o')
docker compose exec -T namenode hdfs dfs -mkdir -p /lab/hdfs
docker compose exec -T namenode hdfs dfs -put /data/transactions.csv /lab/hdfs/transactions.csv
```

### 1.2. Початкове розміщення

**Команди:**

```powershell
[DateTimeOffset]::UtcNow.ToString('o')
docker compose exec -T namenode hdfs dfs -ls /lab/hdfs
docker compose exec -T namenode hdfs dfs -du -s -h /lab/hdfs/transactions.csv
docker compose exec -T namenode hdfs fsck /lab/hdfs/transactions.csv -files -blocks -locations
docker compose exec -T namenode hdfs dfsadmin -report
```

Їх додаткову перевірку після основних експериментів збережено в
[журналі бонусу, розділ 3](experiments_bonus.md#3-додаткова-фіксація-вихідного-файлу-основної-роботи).

| Блок | Розмір, байтів | dn1 | dn2 | dn3 | dn4 |
|---|---:|:---:|:---:|:---:|:---:|
| `blk_1073741825_1001` | 134217728 | — | ✓ | ✓ | ✓ |
| `blk_1073741826_1002` | 134217728 | ✓ | ✓ | ✓ | — |
| `blk_1073741827_1003` | 62913144 | ✓ | ✓ | — | ✓ |

У прочитаному початковому `fsck`: `HEALTHY`, 4 DataNode, 3 блоки,
середня реплікація 3.0, відсутніх і пошкоджених блоків немає.

Розрахунок розміру останнього блоку:

```text
331348600 - 2 × 134217728 = 62913144 байти
```

Обсяг даних трьох реплік без локальних `.meta` та інших службових витрат:

```text
331348600 × 3 = 994045800 байтів
```

### 1.3. Локальні файли на dn2

**Команда:**

```powershell
docker compose exec -T dn2 find /hadoop/dfs/data -type f -name 'blk_*' -printf '%p %s bytes\n'
```

**Вивід користувача:**

```text
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741825 134217728 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741825_1001.meta 1048583 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741827_1003.meta 491519 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741827 62913144 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741826_1002.meta 1048583 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741826 134217728 bytes
```

Підсумок вимірів:

| Блок | Файл даних, байтів | `.meta`, байтів |
|---|---:|---:|
| `1825` | 134217728 | 1048583 |
| `1826` | 134217728 | 1048583 |
| `1827` | 62913144 | 491519 |

Розрахунок `.meta`: 7 байтів заголовка та по 4 байти CRC32C на кожну
ділянку даних до 512 байтів, включно з неповною останньою.

```text
7 + ceil(134217728 / 512) × 4 = 7 + 262144 × 4 = 1048583
7 + ceil(62913144 / 512) × 4 = 7 + 122878 × 4 = 491519
```

## 2. Втрата та повернення dn2

### 2.1. Запис журналу в окремому терміналі

**Команди:**

```powershell
cd C:\Antigravity_Projects\BigData_DIS\Lab4_HDFS
docker compose logs --since 1m --follow --timestamps --no-color namenode 2>&1 | Tee-Object .\evidence\02_node_failure\namenode.log
```

### 2.2. Зупинка вузла та точний час

Обрано `dn2`: на ньому були репліки всіх трьох блоків.

**Команди:**

```powershell
[DateTimeOffset]::UtcNow.ToString('o')
docker stop lab4-hdfs-dn2-1
docker inspect lab4-hdfs-dn2-1 --format '{{.State.FinishedAt}}'
```

**Надісланий вивід `docker inspect`:**

```text
2026-10-22T19:29:19.992387669Z
```

### 2.3. Рання перевірка: NameNode ще бачить 4 живі вузли

**Команда:**

```powershell
docker compose exec -T namenode hdfs dfsadmin -report
```

**Вивід користувача, скорочено:**

```text
Replicated Blocks:
        Under replicated blocks: 0
        Blocks with corrupt replicas: 0
        Missing blocks: 0
        Pending deletion blocks: 0

Live datanodes (4):

Name: 172.21.0.3:9866 (172.21.0.3)
Hostname: dn2
Decommission Status : Normal
Last contact: Thu Oct 22 19:29:18 UTC 2026
Last Block Report: Thu Oct 22 19:02:49 UTC 2026
Num of Blocks: 3

Name: 172.21.0.4:9866 (lab4-hdfs-dn4-1.lab4-hdfs_default)
Hostname: dn4
Decommission Status : Normal
Last contact: Thu Oct 22 19:29:40 UTC 2026
Num of Blocks: 2

Name: 172.21.0.5:9866 (lab4-hdfs-dn1-1.lab4-hdfs_default)
Hostname: dn1
Decommission Status : Normal
Last contact: Thu Oct 22 19:29:40 UTC 2026
Num of Blocks: 2

Name: 172.21.0.6:9866 (lab4-hdfs-dn3-1.lab4-hdfs_default)
Hostname: dn3
Decommission Status : Normal
Last contact: Thu Oct 22 19:29:40 UTC 2026
Num of Blocks: 2
```

`dn2` уже зупинений, але поріг визнання вузла мертвим ще не сплив.

### 2.4. Fsck після автоматичного відновлення

**Команда:**

```powershell
docker compose exec -T namenode hdfs fsck /lab/hdfs/transactions.csv -files -blocks -locations
```

**Вивід користувача, скорочено:**

```text
FSCK started by hadoop (auth:SIMPLE) from /172.21.0.2 for path /lab/hdfs/transactions.csv at Thu Oct 22 19:31:39 UTC 2026

/lab/hdfs/transactions.csv 331348600 bytes, replicated: replication=3, 3 block(s):  OK
0. BP-2004503507-172.21.0.2-1792621365166:blk_1073741825_1001 len=134217728 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.4:9866,DS-f0556269-ef1c-4ae2-af97-f171bcff4d31,DISK], DatanodeInfoWithStorage[172.21.0.6:9866,DS-6a53c24f-bc9c-4787-b892-4b5f239ec3cc,DISK], DatanodeInfoWithStorage[172.21.0.5:9866,DS-a9d7a13e-f502-4c33-abd0-5fe77fc5f1f0,DISK]]
1. BP-2004503507-172.21.0.2-1792621365166:blk_1073741826_1002 len=134217728 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.6:9866,DS-6a53c24f-bc9c-4787-b892-4b5f239ec3cc,DISK], DatanodeInfoWithStorage[172.21.0.5:9866,DS-a9d7a13e-f502-4c33-abd0-5fe77fc5f1f0,DISK], DatanodeInfoWithStorage[172.21.0.4:9866,DS-f0556269-ef1c-4ae2-af97-f171bcff4d31,DISK]]
2. BP-2004503507-172.21.0.2-1792621365166:blk_1073741827_1003 len=62913144 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.5:9866,DS-a9d7a13e-f502-4c33-abd0-5fe77fc5f1f0,DISK], DatanodeInfoWithStorage[172.21.0.4:9866,DS-f0556269-ef1c-4ae2-af97-f171bcff4d31,DISK], DatanodeInfoWithStorage[172.21.0.6:9866,DS-6a53c24f-bc9c-4787-b892-4b5f239ec3cc,DISK]]

Status: HEALTHY
 Number of data-nodes:  3
 Number of racks:               1

Replicated Blocks:
 Total size:    331348600 B
 Total files:   1
 Total blocks (validated):      3 (avg. block size 110449533 B)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Average block replication:     3.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
 Blocks queued for replication: 0

FSCK ended at Thu Oct 22 19:31:39 UTC 2026 in 3 milliseconds
The filesystem under path '/lab/hdfs/transactions.csv' is HEALTHY
```

### 2.5. Виявлення втрати та реєстрація нових реплік у логах

**Команда:**

```powershell
docker compose logs --no-color --since "2026-10-22T19:29:00Z" --until "2026-10-22T19:32:00Z" namenode 2>&1 | Select-String -Pattern 'heartbeat|dead|1073741825|1073741826|1073741827'
```

**Вивід користувача:**

```text
namenode-1  | 2026-10-22 19:31:15 INFO  StateChange:927 - BLOCK* removeDeadDatanode: lost heartbeat from 172.21.0.3:9866, removeBlocksFromBlockMap true
namenode-1  | 2026-10-22 19:31:17 INFO  BlockStateChange:3806 - BLOCK* addStoredBlock: 172.21.0.6:9866 is added to blk_1073741827_1003 (size=62913144)
namenode-1  | 2026-10-22 19:31:18 INFO  BlockStateChange:3806 - BLOCK* addStoredBlock: 172.21.0.5:9866 is added to blk_1073741825_1001 (size=134217728)
namenode-1  | 2026-10-22 19:31:19 INFO  BlockStateChange:3806 - BLOCK* addStoredBlock: 172.21.0.4:9866 is added to blk_1073741826_1002 (size=134217728)
```

| Вимір | Результат |
|---|---|
| Зупинка контейнера | 19:29:19.992387669 UTC |
| Визнання вузла мертвим | 19:31:15 UTC за внутрішнім часом логу |
| Від зупинки до визнання мертвим | приблизно 115 с |
| Нова репліка `1827` на dn3 | приблизно через 2 с після визнання мертвим |
| Нова репліка `1825` на dn1 | приблизно через 3 с |
| Нова репліка `1826` на dn4 | приблизно через 4 с |

Після виключення dn2 кожен із трьох блоків втратив одну здорову доступну репліку.
Короткий стан із двома репліками не потрапив у надісланий `fsck`: на момент
19:31:39 відновлення вже завершилося. Втрата та відновлення підтверджені логами
і порівнянням карт. Часи 2–4 с — час до реєстрації нових реплік, а не лише
тривалість передавання байтів.

### 2.6. Повернення dn2

**Команда:**

```powershell
docker start lab4-hdfs-dn2-1
```

Збережений під час експерименту журнал додатково підтвердив повернення вузла.

```text
namenode-1  | 2026-10-22T19:35:58.001080396Z 2026-10-22 19:35:58 INFO  StateChange:1214 - BLOCK* registerDatanode: from DatanodeRegistration(172.21.0.3:9866, datanodeUuid=0e059cf2-2164-460b-b506-e77f06718959, infoPort=9864, infoSecurePort=0, ipcPort=9867, storageInfo=lv=-57;cid=CID-a9e5074e-1395-4229-9100-89abf2b106cd;nsid=1374691582;c=1792621365168) storage 0e059cf2-2164-460b-b506-e77f06718959
```

Наступний `fsck` зроблено о **19:36:38 UTC**, тобто приблизно через **40 с
після реєстрації** dn2 у журналі. У наданих доказах немає перевірки рівно
через 60 с; цей інтервал не підміняється очікуванням з інструкції.

**Команда:**

```powershell
docker compose exec -T namenode hdfs fsck /lab/hdfs/transactions.csv -files -blocks -locations
```

**Вивід користувача, скорочено:**

```text
FSCK started by hadoop (auth:SIMPLE) from /172.21.0.2 for path /lab/hdfs/transactions.csv at Thu Oct 22 19:36:38 UTC 2026

/lab/hdfs/transactions.csv 331348600 bytes, replicated: replication=3, 3 block(s):  OK
0. BP-2004503507-172.21.0.2-1792621365166:blk_1073741825_1001 len=134217728 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.3:9866,DS-8e58078b-75af-4627-9432-45bf21677d86,DISK], DatanodeInfoWithStorage[172.21.0.6:9866,DS-6a53c24f-bc9c-4787-b892-4b5f239ec3cc,DISK], DatanodeInfoWithStorage[172.21.0.5:9866,DS-a9d7a13e-f502-4c33-abd0-5fe77fc5f1f0,DISK]]
1. BP-2004503507-172.21.0.2-1792621365166:blk_1073741826_1002 len=134217728 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.3:9866,DS-8e58078b-75af-4627-9432-45bf21677d86,DISK], DatanodeInfoWithStorage[172.21.0.5:9866,DS-a9d7a13e-f502-4c33-abd0-5fe77fc5f1f0,DISK], DatanodeInfoWithStorage[172.21.0.4:9866,DS-f0556269-ef1c-4ae2-af97-f171bcff4d31,DISK]]
2. BP-2004503507-172.21.0.2-1792621365166:blk_1073741827_1003 len=62913144 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.3:9866,DS-8e58078b-75af-4627-9432-45bf21677d86,DISK], DatanodeInfoWithStorage[172.21.0.4:9866,DS-f0556269-ef1c-4ae2-af97-f171bcff4d31,DISK], DatanodeInfoWithStorage[172.21.0.6:9866,DS-6a53c24f-bc9c-4787-b892-4b5f239ec3cc,DISK]]

Status: HEALTHY
 Number of data-nodes:  4
 Number of racks:               1

Replicated Blocks:
 Total size:    331348600 B
 Total files:   1
 Total blocks (validated):      3 (avg. block size 110449533 B)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Average block replication:     3.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
 Blocks queued for replication: 0

FSCK ended at Thu Oct 22 19:36:38 UTC 2026 in 7 milliseconds
The filesystem under path '/lab/hdfs/transactions.csv' is HEALTHY
```

**Команда перевірки локального диска dn2:**

```powershell
docker compose exec -T dn2 find /hadoop/dfs/data -type f -name 'blk_*' -printf '%p %s bytes\n'
```

**Вивід користувача:**

```text
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741825 134217728 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741825_1001.meta 1048583 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741827_1003.meta 491519 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741827 62913144 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741826_1002.meta 1048583 bytes
/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741826 134217728 bytes
```

На dn2 збереглися всі три блоки й усі три `.meta`. У карті залишилося по три
репліки: зайві копії зникли з карти dn4 для `1825`, dn3 для `1826` та dn1
для `1827`. Фізичний диск цих трьох інших вузлів цією командою не перевірявся.

## 3. Тихе пошкодження репліки

### 3.1. MD5 оригіналу

**Команда:**

```powershell
docker compose exec -T dn2 md5sum /data/transactions.csv
```

**Вивід користувача:**

```text
e52630c6d29645cf6f357c1df6712d42  /data/transactions.csv
```

### 3.2. Зміна байтів репліки

**Команди, надані для виконання на dn2:**

```powershell
$block = '/hadoop/dfs/data/current/BP-2004503507-172.21.0.2-1792621365166/current/finalized/subdir0/subdir0/blk_1073741825'
docker compose exec -T dn2 sh -c "printf 'CORRUPTED!' | dd of=$block bs=1 seek=12345 conv=notrunc"
```

Замінено 10 байтів зі зміщення 12345; `.meta` не змінювався.
Вивід `dd` не надсилався. Наступне повідомлення клієнта підтверджує
пошкодження вибраного блоку саме на dn2.

### 3.3. Читання з того самого контейнера та MD5 копії

**Команда:**

```powershell
docker compose exec -T dn2 hdfs dfs -get /lab/hdfs/transactions.csv /tmp/transactions-readback.csv 2>&1 | Tee-Object .\evidence\03_corruption\client.log
```

**Повний надісланий вивід клієнта:**

```text
2026-10-22 19:41:06 WARN  DFSClient:800 - Found Checksum error for BP-2004503507-172.21.0.2-1792621365166:blk_1073741825_1001 from DatanodeInfoWithStorage[172.21.0.3:9866,DS-8e58078b-75af-4627-9432-45bf21677d86,DISK] at 12288
```

Повний значущий вивід із колишнього `03_corruption/client.log` наведено вище;
окрему копію файла після зведення видалено.

**Команда:**

```powershell
docker compose exec -T dn2 md5sum /tmp/transactions-readback.csv
```

**Вивід користувача:**

```text
e52630c6d29645cf6f357c1df6712d42  /tmp/transactions-readback.csv
```

MD5 оригіналу й отриманої копії збіглися. Після помилки читання пошкодженої
репліки клієнт отримав правильні дані з іншої репліки; конкретне джерело
повторного читання в надісланому попередженні не вказане.

Позиція 12288 у попередженні — початок 512-байтової ділянки:

```text
12345 = 24 × 512 + 57
floor(12345 / 512) × 512 = 12288
```

### 3.4. Реакція NameNode

**Команда:**

```powershell
docker compose logs --no-color --since "2026-10-22T19:40:00Z" namenode 2>&1 | Select-String -Pattern '1073741825|corrupt|bad block'
```

**Вивід користувача:**

```text
namenode-1  | 2026-10-22 19:41:06 INFO  StateChange:6023 - *DIR* reportBadBlocks for block: BP-2004503507-172.21.0.2-1792621365166:blk_1073741825_1001 on datanode: 172.21.0.3:9866
namenode-1  | 2026-10-22 19:41:11 INFO  BlockStateChange:3806 - BLOCK* addStoredBlock: 172.21.0.4:9866 is added to blk_1073741825_1001 (size=134217728)
```

Від повідомлення про пошкоджену репліку на dn2 до реєстрації нової репліки
на dn4 минуло приблизно 5 с за внутрішніми часовими мітками журналу.

### 3.5. Остаточна перевірка

**Команда:**

```powershell
docker compose exec -T namenode hdfs fsck /lab/hdfs/transactions.csv -files -blocks -locations
```

**Вивід користувача, скорочено:**

```text
FSCK started by hadoop (auth:SIMPLE) from /172.21.0.2 for path /lab/hdfs/transactions.csv at Thu Oct 22 19:42:39 UTC 2026

/lab/hdfs/transactions.csv 331348600 bytes, replicated: replication=3, 3 block(s):  OK
0. BP-2004503507-172.21.0.2-1792621365166:blk_1073741825_1001 len=134217728 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.4:9866,DS-f0556269-ef1c-4ae2-af97-f171bcff4d31,DISK], DatanodeInfoWithStorage[172.21.0.6:9866,DS-6a53c24f-bc9c-4787-b892-4b5f239ec3cc,DISK], DatanodeInfoWithStorage[172.21.0.5:9866,DS-a9d7a13e-f502-4c33-abd0-5fe77fc5f1f0,DISK]]
1. BP-2004503507-172.21.0.2-1792621365166:blk_1073741826_1002 len=134217728 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.3:9866,DS-8e58078b-75af-4627-9432-45bf21677d86,DISK], DatanodeInfoWithStorage[172.21.0.5:9866,DS-a9d7a13e-f502-4c33-abd0-5fe77fc5f1f0,DISK], DatanodeInfoWithStorage[172.21.0.4:9866,DS-f0556269-ef1c-4ae2-af97-f171bcff4d31,DISK]]
2. BP-2004503507-172.21.0.2-1792621365166:blk_1073741827_1003 len=62913144 Live_repl=3  [DatanodeInfoWithStorage[172.21.0.3:9866,DS-8e58078b-75af-4627-9432-45bf21677d86,DISK], DatanodeInfoWithStorage[172.21.0.4:9866,DS-f0556269-ef1c-4ae2-af97-f171bcff4d31,DISK], DatanodeInfoWithStorage[172.21.0.6:9866,DS-6a53c24f-bc9c-4787-b892-4b5f239ec3cc,DISK]]

Status: HEALTHY
 Number of data-nodes:  4
 Number of racks:               1

Replicated Blocks:
 Total size:    331348600 B
 Total files:   1
 Total blocks (validated):      3 (avg. block size 110449533 B)
 Over-replicated blocks:        0 (0.0 %)
 Under-replicated blocks:       0 (0.0 %)
 Average block replication:     3.0
 Missing blocks:                0
 Corrupt blocks:                0
 Missing replicas:              0 (0.0 %)
 Blocks queued for replication: 0

FSCK ended at Thu Oct 22 19:42:39 UTC 2026 in 2 milliseconds
The filesystem under path '/lab/hdfs/transactions.csv' is HEALTHY
```

Здорова репліка `1825` на dn2 більше не обліковується; замість неї є репліка
на dn4. Фізичне видалення пошкодженого файла з диска dn2 після цього
не перевірялося.

## 4. Топологія

**Команда:**

```powershell
docker compose exec -T namenode hdfs dfsadmin -printTopology
```

**Вивід користувача:**

```text
Rack: /default-rack
   172.21.0.6:9866 (lab4-hdfs-dn3-1.lab4-hdfs_default) In Service
   172.21.0.3:9866 (lab4-hdfs-dn2-1.lab4-hdfs_default) In Service
   172.21.0.4:9866 (lab4-hdfs-dn4-1.lab4-hdfs_default) In Service
   172.21.0.5:9866 (lab4-hdfs-dn1-1.lab4-hdfs_default) In Service
```

Усі чотири DataNode належать до `/default-rack`.

## 5. Зміни карти блоків за всіма етапами

| Етап | `blk_1073741825` | `blk_1073741826` | `blk_1073741827` |
|---|---|---|---|
| Початковий стан | dn2, dn3, dn4 | dn1, dn2, dn3 | dn1, dn2, dn4 |
| Після втрати dn2 та відновлення | dn1, dn3, dn4 | dn1, dn3, dn4 | dn1, dn3, dn4 |
| Після повернення dn2 | dn1, dn2, dn3 | dn1, dn2, dn4 | dn2, dn3, dn4 |
| Після виявлення корупції та відновлення | dn1, dn3, dn4 | dn1, dn2, dn4 | dn2, dn3, dn4 |

Цей файл містить докази та короткі розрахунки за трьома основними експериментами.
Бонус оформлено окремо в [experiments_bonus.md](experiments_bonus.md).
