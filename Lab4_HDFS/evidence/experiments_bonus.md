# Бонус — вартість 5000 дрібних файлів

**Дата запуску:** 22.10.2026. **Час у доказах:** UTC.
Команди та повний допоміжний код: [../commands_bonus.md](../commands_bonus.md).

## 1. Умови порівняння

- Той самий CSV: **331348600 байтів**, **8000001 рядків**
  разом з одним заголовком; транзакцій — 8000000.
- Один варіант — цілий файл, інший — рівно **5000** частин із цілими рядками.
  Заголовок залишився тільки в першій частині; додаткові байти не додавалися.
- Усі 4 DataNode працювали. Реплікація — 3, блок — 134217728 байтів.
- Обидва виміряні варіанти завантажувалися послідовним `hdfs dfs -put`
  із локального Linux-сховища контейнера NameNode.
- `grep` працював у LocalJobRunner, з входами в HDFS, без YARN:
  максимум 4 паралельні map-задачі, heap 2048 МіБ, буфер сортування 16 МіБ.
  Параметри для обох варіантів однакові.
- Часи — одиничні виміри цього запуску, з урахуванням запуску процесу Hadoop.
  Кеш ОС не очищався; підготовка частин, контроль MD5 та checkpoint у час `put` не входять.

Спочатку великий файл було завантажено безпосередньо з Windows bind mount
`/data` за 50.492 с. Щоб не порівнювати різні
типи локального джерела, після основного прогону окремо виміряно завантаження
ідентичної Linux-копії за **3.318 с**. Саме цей другий час використано
в підсумковому порівнянні з частинами. Початковий час з `/data` не приховано,
але він не є основною базою порівняння. Порядок вимірів не рандомізувався.

## 2. Поділ без втрати байтів

**Дія:** бінарне читання оригіналу, рівномірний поділ повних рядків,
обчислення MD5 оригіналу та послідовного з'єднання частин.

**Фактичний результат допоміжного коду:**

```json
{
  "path": "/data/transactions.csv",
  "bytes": 331348600,
  "lines_including_header": 8000001,
  "md5": "e52630c6d29645cf6f357c1df6712d42",
  "parts_md5_concatenated": "e52630c6d29645cf6f357c1df6712d42",
  "parts": 5000,
  "min_part_bytes": 65956,
  "max_part_bytes": 66600,
  "min_part_lines": 1600,
  "max_part_lines": 1601
}
```

Вийшло по 1600–1601 рядків у частині,
розміри — 65956–66600 байтів.
Сума розмірів — 331348600 байтів; MD5 збігається з оригіналом основних експериментів.

## 3. Додаткова фіксація вихідного файлу основної роботи

Ці команди виконані вже під час бонусу; це не відновлений вивід раннього запуску.

**Команда:**

```bash
hdfs dfs -ls /lab/hdfs
```

**Вивід:**

```text
Found 1 items
-rw-r--r--   3 hadoop supergroup  331348600 2026-10-22 19:06 /lab/hdfs/transactions.csv
```

**Команда:**

```bash
hdfs dfs -du -s -h /lab/hdfs/transactions.csv
```

**Вивід:**

```text
316.0 M  948.0 M  /lab/hdfs/transactions.csv
```

## 4. Час завантаження

Час виміряно `time.perf_counter()` навколо одного процесу `hdfs dfs -put`.
Для дрібних файлів один процес отримав упорядкований список усіх 5000 шляхів;
наведена маска — короткий запис цього списку.

**Команди:**

```bash
hdfs dfs -put /tmp/hdfs-smallfiles-bonus/single-staged.csv /bonus/upload-control.csv
hdfs dfs -put /tmp/hdfs-smallfiles-bonus/parts/part-*.csv /bonus/input/
```

**Виміри із завершенням процесів без помилок:**

```json
{
  "single": {
    "argv": [
      "hdfs",
      "dfs",
      "-put",
      "/tmp/hdfs-smallfiles-bonus/single-staged.csv",
      "/bonus/upload-control.csv"
    ],
    "started_utc": "2026-10-22T20:08:51.586907+00:00",
    "finished_utc": "2026-10-22T20:08:54.904828+00:00",
    "single_upload_seconds": 3.3178608950001944,
    "returncode": 0,
    "staged_bytes": 331348600,
    "staged_md5": "e52630c6d29645cf6f357c1df6712d42"
  },
  "small": {
    "label": "upload-small",
    "argv": [
      "hdfs",
      "dfs",
      "-put",
      "/tmp/hdfs-smallfiles-bonus/parts/part-*.csv",
      "/bonus/input/"
    ],
    "elapsed_seconds": 190.8284359979989,
    "returncode": 0,
    "started_utc": "2026-10-22T20:03:20.755533+00:00",
    "finished_utc": "2026-10-22T20:06:31.566037+00:00"
  }
}
```

| Варіант | Обсяг, байтів | Файлів | Завантаження, с |
|---|---:|---:|---:|
| Один файл, Linux-джерело | 331348600 | 1 | 3.318 |
| 5000 частин, Linux-джерело | 331348600 | 5000 | 190.828 |

Дрібні файли завантажувалися у **57.52 раза** довше.
Додаткове контрольне завантаження великого файла не входить у metadata-знімки нижче.

## 5. Об'єкти NameNode та fsimage

Виміри зроблено **до запуску MapReduce**, щоб його результати й тимчасові файли
не впливали на порівняння. В обох сценаріях використовувався той самий каталог
`/bonus/input`. Між ними видалено тільки бонусну копію великого файла.
Початковий файл `/lab/hdfs/transactions.csv` залишався у кластері.

На кожному етапі виконано:

```bash
hdfs dfsadmin -safemode enter
hdfs dfsadmin -saveNamespace
hdfs dfsadmin -safemode leave
hdfs dfs -count /bonus/input
```

**Підтвердження checkpoint і лічильників:**

**Етап `baseline`:**

```text
Save namespace successful
1            0                  0 /bonus/input
```
```json
{
  "utc": "2026-10-22T20:02:19.239471+00:00",
  "files_total_including_directories": 6,
  "blocks_total": 3,
  "fsimage_path": "/hadoop/dfs/name/current/fsimage_0000000000000000019",
  "fsimage_bytes": 793,
  "heap_used_after_gc_bytes": 41596128
}
```
**Етап `single`:**

```text
Save namespace successful
1            1          331348600 /bonus/input
```
```json
{
  "utc": "2026-10-22T20:03:17.309795+00:00",
  "files_total_including_directories": 7,
  "blocks_total": 6,
  "fsimage_path": "/hadoop/dfs/name/current/fsimage_0000000000000000033",
  "fsimage_bytes": 912,
  "heap_used_after_gc_bytes": 41816768
}
```
**Етап `small`:**

```text
Save namespace successful
1         5000          331348600 /bonus/input
```
```json
{
  "utc": "2026-10-22T20:06:37.174560+00:00",
  "files_total_including_directories": 5006,
  "blocks_total": 5003,
  "fsimage_path": "/hadoop/dfs/name/current/fsimage_0000000000000030036",
  "fsimage_bytes": 385814,
  "heap_used_after_gc_bytes": 44373360
}
```

`FilesTotal` і `BlocksTotal` отримані з JMX NameNode — тих самих службових
лічильників, які використовує вебінтерфейс. `FilesTotal` включає каталоги.
Розмір узято для найновішого `fsimage_<txid>`, окремий `.md5` не враховано.

| Показник | База, порожній bonus/input | Один файл | 5000 файлів |
|---|---:|---:|---:|
| Файлів даних у bonus/input | 0 | 1 | 5000 |
| Блоків бонусного входу | 0 | 3 | 5000 |
| Байти даних у bonus/input | 0 | 331348600 | 331348600 |
| FilesTotal у всьому кластері, з каталогами | 6 | 7 | 5006 |
| BlocksTotal у всьому кластері | 3 | 6 | 5003 |
| FilesTotal + BlocksTotal у всьому кластері | 9 | 13 | 10009 |
| Нові об'єкти «файл + блок» відносно бази | 0 | 4 | 10000 |
| Свіжий fsimage, байтів | 793 | 912 | 385814 |

Порівняння 5000 файлів з одним:

- файлів — **5000 разів** більше;
- блоків входу — **1666.67 раза** більше;
- нових об'єктів «файл + блок» — **2500 разів** більше;
- сума FilesTotal + BlocksTotal всього кластера —
  **769.92 раза** більша;
- весь fsimage — **423.04 раза** більший;
- приріст fsimage понад порожню базу — **3235.47 раза** більший.

Сума FilesTotal + BlocksTotal — зручний лічильник об'єктів namespace і блоків,
а не кількість усіх Java-об'єктів JVM. Три фізичні репліки блока не означають
три незалежні записи логічних блоків у цьому лічильнику.

## 6. Однаковий MapReduce grep

Обидві команди працювали з одним регулярним виразом та однаковими параметрами.
Версія JAR — 3.5.0, відповідно до встановленого Hadoop.

**Команди:**

```bash
HADOOP_HEAPSIZE_MAX=2048 hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar grep -Dmapreduce.framework.name=local -Dmapreduce.local.map.tasks.maximum=4 -Dmapreduce.task.io.sort.mb=16 /bonus/big-input/transactions.csv /bonus/results/single 'groceries|books'
```

**Вибрані рядки фактичного логу `single`:**

```text
2026-10-22 20:07:31 INFO  FileInputFormat:303 - Total input files to process : 1
2026-10-22 20:07:31 INFO  JobSubmitter:203 - number of splits:3
2026-10-22 20:07:37 INFO  Job:1765 - Job job_local310769743_0001 completed successfully
		Map input records=8000001
		Map output records=2881957
		Reduce output records=2
2026-10-22 20:07:37 INFO  FileInputFormat:303 - Total input files to process : 1
2026-10-22 20:07:37 INFO  JobSubmitter:203 - number of splits:1
2026-10-22 20:07:38 INFO  Job:1765 - Job job_local2142865783_0002 completed successfully
		Map input records=2
		Map output records=2
		Reduce output records=2
```
```bash
HADOOP_HEAPSIZE_MAX=2048 hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar grep -Dmapreduce.framework.name=local -Dmapreduce.local.map.tasks.maximum=4 -Dmapreduce.task.io.sort.mb=16 /bonus/small-input /bonus/results/small 'groceries|books'
```

**Вибрані рядки фактичного логу `small`:**

```text
2026-10-22 20:07:42 INFO  FileInputFormat:303 - Total input files to process : 5000
2026-10-22 20:07:42 INFO  JobSubmitter:203 - number of splits:5000
2026-10-22 20:08:21 INFO  Job:1765 - Job job_local707851490_0001 completed successfully
		Map input records=8000001
		Map output records=2881957
		Reduce output records=2
2026-10-22 20:08:22 INFO  FileInputFormat:303 - Total input files to process : 1
2026-10-22 20:08:22 INFO  JobSubmitter:203 - number of splits:1
2026-10-22 20:08:23 INFO  Job:1765 - Job job_local1523583975_0002 completed successfully
		Map input records=2
		Map output records=2
		Reduce output records=2
```

`grep` виконує два job: пошук і сортування. Нижче порівнюються split-и першого
job, який читає CSV; другий job має 1 split у кожному варіанті.
Час — повна команда `grep`, включно з обома job.

| Показник | Один файл | 5000 файлів | Відношення |
|---|---:|---:|---:|
| Вхідних CSV | 1 | 5000 | 5000× |
| Input split-ів першого job | 3 | 5000 | 1666.67× |
| Split-ів другого job | 1 | 1 | 1× |
| Час grep, с | 9.139 | 43.332 | 4.74× |

**Результат `hdfs dfs -cat /bonus/results/single/part-*`:**

```text
2401206	groceries
480751	books
```

**Результат `hdfs dfs -cat /bonus/results/small/part-*`:**

```text
2401206	groceries
480751	books
```

Результати збігаються. MD5 текстового виводу обох варіантів:
`09f808cee8e9061dfb0546d396a5e9f3`.

5000 дрібних файлів дали 5000 окремих split-ів: звичайний вхід цього прикладу
не об'єднує їх в один split. Це 5000 окремих map-задач першого job,
хоча сумарний обсяг байтів не змінився.

## 7. Оцінка пам'яті NameNode для 100 млн дрібних файлів

Розмір fsimage на диску не дорівнює пам'яті NameNode. Тому додатково виміряно
`java.lang:type=Memory / HeapMemoryUsage.used` після явного GC процесу NameNode.
На етап зроблено три спроби; вибрано найменший used heap, щоб зменшити вплив
короткоживучих виділень. Це heap NameNode, а не heap MapReduce-клієнта.

| Етап | Три виміри used heap після GC, байтів | Мінімум, байтів |
|---|---|---:|
| База | [41596128, 42741352, 42759048] | 41596128 |
| Один файл | [42852880, 41816768, 42870120] | 41816768 |
| 5000 файлів | [45374640, 45393184, 44373360] | 44373360 |

Оцінка за різницею між двома варіантами:

```text
Додатковий heap = 44373360 - 41816768 = 2556592 байтів
Додаткових файлів = 5000 - 1 = 4999
Орієнтовно на дрібний файл з його блоком = 2556592 / 4999 = 511.42 байта
На 100000000 таких файлів ≈ 51142068414 байтів = 51.14 GB = 47.63 GiB
```

Це груба лінійна екстраполяція одного малого запуску: вимір охоплює також
внутрішні структури та кеші JVM. Для кожного нового дрібного файла додається
приблизно один блок; різниця у два додаткові блоки великого файла при відніманні
неістотна на масштабі 100 млн. Оцінка не включає запас для робочих алокацій, GC, інших даних,
реплікаційних черг та відмінностей конфігурації великого кластера.

Ті самі байти в дрібних файлах потребують значно більше імен, inode-записів,
блоків, записів розміщення, операцій створення/закриття та окремих map-задач.
Останній короткий блок не резервує всі 128 МіБ, але все одно потребує обліку
як окремий блок. Саме кількість об'єктів та операцій пояснює виміряну різницю.

## 8. Фінальний стан

**Команди:**

```bash
hdfs fsck /bonus
hdfs dfsadmin -report
```

**Скорочений підсумок fsck:**

```text
Status: HEALTHY
 Number of data-nodes:	4
 Number of racks:		1
 Total dirs:			6
 Total symlinks:		0

Replicated Blocks:
 Total size:	662697262 B
 Total files:	5005
 Total blocks (validated):	5005 (avg. block size 132407 B)
 Minimally replicated blocks:	5005 (100.0 %)
 Over-replicated blocks:	0 (0.0 %)
 Under-replicated blocks:	0 (0.0 %)
 Mis-replicated blocks:		0 (0.0 %)
 Default replication factor:	3
 Average block replication:	3.0
 Missing blocks:		0
 Corrupt blocks:		0
 Missing replicas:		0 (0.0 %)
 Blocks queued for replication:	0


FSCK ended at Thu Oct 22 20:08:58 UTC 2026 in 85 milliseconds
The filesystem under path '/bonus' is HEALTHY
```

**Стан кластера:**

```text
	Under replicated blocks: 0
	Blocks with corrupt replicas: 0
	Missing blocks: 0
Live datanodes (4):
```
