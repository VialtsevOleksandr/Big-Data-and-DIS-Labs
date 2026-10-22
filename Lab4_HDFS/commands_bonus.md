# Бонус — команди експерименту з 5000 дрібними файлами

Результати: [evidence/experiments_bonus.md](evidence/experiments_bonus.md).

## Умови

- Вхід: `/data/transactions.csv`, той самий оригінал 331348600 байтів.
- Фрагменти: рівно 5000 CSV, тільки цілі рядки; заголовок не дублюється.
- Завантаження: стандартний `hdfs dfs -put`, без паралельного `-t`.
- HDFS: реплікація 3, блок 128 МіБ, 4 DataNode.
- MapReduce: стандартний приклад `grep` Hadoop 3.5.0, вираз `groceries|books`.
- Обидва запуски: `mapreduce.framework.name=local`, максимум 4 паралельні map-задачі,
  буфер сортування 16 МіБ, heap клієнтського процесу 2048 МіБ.
- YARN не використовувався. Дані читалися з HDFS; задачі виконував LocalJobRunner.

## 1. Початкова перевірка

```powershell
cd C:\Antigravity_Projects\BigData_DIS\Lab4_HDFS
docker compose ps -a
docker compose exec -T namenode hdfs dfsadmin -report
docker compose exec -T namenode hdfs getconf -confKey mapreduce.framework.name
docker compose exec -T namenode hdfs dfs -test -e /bonus
```

Перед запуском `/bonus` був відсутній; для останньої команди код 1 у цьому
випадку означав відсутність шляху. Існуючий `/lab/hdfs/transactions.csv` не змінювали.

## 2. Підготовка та автоматичний запуск

Допоміжний код, наведений унизу, був збережений у тимчасовій папці `.cache/lab4_bonus`.
Вона ігнорується Git. Код повністю включено в цей файл для відтворюваності.
Фактичний запуск із кореня проєкту:

```powershell
cd C:\Antigravity_Projects\BigData_DIS
docker cp .cache/lab4_bonus/run_bonus.py lab4-hdfs-namenode-1:/tmp/run_hdfs_bonus.py
docker exec lab4-hdfs-namenode-1 python3 /tmp/run_hdfs_bonus.py
```

Скрипт створює `/tmp/hdfs-smallfiles-bonus` лише якщо такого каталогу ще немає.
Він не призначений для повторного запуску поверх уже отриманих результатів.

## 3. Фактична послідовність усередині контейнера

1. Два проходи по оригіналу: кількість рядків і MD5; поділ рядків на 5000 частин.
2. Перевірка сумарного розміру й MD5 послідовного з'єднання частин.
3. Створення порожнього `/bonus/input`, базовий checkpoint і метрики.
4. Виміряне завантаження одного файлу, checkpoint і метрики.
5. Видалення лише бонусної копії одного файлу, завантаження 5000 частин,
   checkpoint і метрики. Початковий файл основного експерименту зберігається.
6. Перейменування каталогу дрібних файлів і повторне створення бонусного великого
   входу для MapReduce. Це повторне завантаження не включене в основне порівняння часу.
7. Два запуски `grep`, порівняння результатів і фінальна перевірка HDFS.

Команди завантаження, які викликав скрипт:

```bash
hdfs dfs -mkdir -p /bonus/input
hdfs dfs -put /data/transactions.csv /bonus/input/transactions.csv
hdfs dfs -rm -skipTrash /bonus/input/transactions.csv
hdfs dfs -put /tmp/hdfs-smallfiles-bonus/parts/part-*.csv /bonus/input/
```

Для дрібних файлів скрипт передає розгорнутий упорядкований список 5000 шляхів
одному процесу `hdfs dfs -put`; маска вище є коротким записом того самого списку.
Час кожного `put` вимірюється `time.perf_counter()` від запуску процесу до його завершення.
Поділ і перевірка MD5 у час завантаження не входять.

На кожному з трьох етапів (`baseline`, `single`, `small`) виконується:

```bash
hdfs dfsadmin -safemode enter
hdfs dfsadmin -saveNamespace
hdfs dfsadmin -safemode leave
jps -l
jcmd <PID_NameNode> GC.run
hdfs dfs -count /bonus/input
```

`leave` виконується в `finally`. Розмір вимірюється для найновішого
`/hadoop/dfs/name/current/fsimage_<txid>`, без `.md5`.
Через `http://localhost:9870/jmx` читаються лічильники `FilesTotal`, `BlocksTotal`
та `HeapMemoryUsage.used`. Для heap робляться три виміри після явного GC;
для порівняння береться мінімальний. `FilesTotal` включає також каталоги.

Підготовка входів до MapReduce:

```bash
hdfs dfs -mv /bonus/input /bonus/small-input
hdfs dfs -mkdir -p /bonus/big-input /bonus/results
hdfs dfs -put /data/transactions.csv /bonus/big-input/transactions.csv
```

Однакові параметри `grep` для обох варіантів:

```bash
HADOOP_HEAPSIZE_MAX=2048 hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar grep -Dmapreduce.framework.name=local -Dmapreduce.local.map.tasks.maximum=4 -Dmapreduce.task.io.sort.mb=16 /bonus/big-input/transactions.csv /bonus/results/single 'groceries|books'
HADOOP_HEAPSIZE_MAX=2048 hadoop jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar grep -Dmapreduce.framework.name=local -Dmapreduce.local.map.tasks.maximum=4 -Dmapreduce.task.io.sort.mb=16 /bonus/small-input /bonus/results/small 'groceries|books'
hdfs dfs -cat '/bonus/results/single/part-*'
hdfs dfs -cat '/bonus/results/small/part-*'
hdfs fsck /bonus -files -blocks
hdfs dfsadmin -report
```

`grep` запускає два послідовні job: пошук і сортування результату.
Для порівняння input split-ів береться перший job, який читає початкові CSV.
Час виконання охоплює всю команду `grep`, тобто обидва job.

## 4. Допоміжний код фактичного запуску

<details>
<summary>Показати run_bonus.py</summary>

```python
import glob
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from urllib.request import urlopen

ROOT = Path('/tmp/hdfs-smallfiles-bonus')
PARTS = ROOT / 'parts'
SOURCE = Path('/data/transactions.csv')
JAR = '/opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.5.0.jar'
STATE = {'started_utc': datetime.now(timezone.utc).isoformat(), 'commands': [], 'snapshots': {}}


def stamp():
    return datetime.now(timezone.utc).isoformat()


def save():
    (ROOT / 'results.json').write_text(json.dumps(STATE, indent=2, ensure_ascii=False) + '\n')


def run(args, label, env=None):
    command_display = args
    if len(args) > 100:
        command_display = args[:3] + [str(PARTS / 'part-*.csv'), args[-1]]
    print(f'{stamp()} START {label}', flush=True)
    log = ROOT / (label + '.log')
    started_utc = stamp()
    start = time.perf_counter()
    with log.open('w') as handle:
        proc = subprocess.Popen(args, stdout=handle, stderr=subprocess.STDOUT, env=env)
        done = threading.Event()
        def progress():
            while not done.wait(30):
                print(f'{stamp()} RUNNING {label}: {time.perf_counter()-start:.1f} s', flush=True)
        threading.Thread(target=progress, daemon=True).start()
        proc.wait()
        done.set()
    elapsed = time.perf_counter() - start
    result = {'label': label, 'argv': command_display, 'elapsed_seconds': elapsed,
              'returncode': proc.returncode, 'started_utc': started_utc, 'finished_utc': stamp()}
    STATE['commands'].append(result)
    save()
    output = log.read_text(errors='replace')
    if proc.returncode:
        print(output[-12000:], flush=True)
        raise RuntimeError(f'{label} exited {proc.returncode}')
    print(f'{stamp()} DONE {label}: {elapsed:.3f} s', flush=True)
    return output, elapsed


def jmx():
    with urlopen('http://localhost:9870/jmx', timeout=15) as response:
        return {b['name']: b for b in json.load(response)['beans']}


def snapshot(label):
    try:
        run(['hdfs', 'dfsadmin', '-safemode', 'enter'], label + '-safemode-enter')
        run(['hdfs', 'dfsadmin', '-saveNamespace'], label + '-saveNamespace')
    finally:
        run(['hdfs', 'dfsadmin', '-safemode', 'leave'], label + '-safemode-leave')
    images = [p for p in Path('/hadoop/dfs/name/current').glob('fsimage_*')
              if re.fullmatch(r'fsimage_\d+', p.name)]
    latest = max(images, key=lambda p: p.name)
    pid_output, _ = run(['jps', '-l'], label + '-jps')
    pid = next(line.split()[0] for line in pid_output.splitlines()
               if line.endswith('org.apache.hadoop.hdfs.server.namenode.NameNode'))
    samples = []
    for index in range(3):
        run(['jcmd', pid, 'GC.run'], label + f'-gc-{index}')
        beans = jmx()
        samples.append(beans['java.lang:type=Memory']['HeapMemoryUsage']['used'])
    beans = jmx()
    fs = beans['Hadoop:service=NameNode,name=FSNamesystem']
    snap = {'utc': stamp(), 'files_total_including_directories': fs['FilesTotal'],
            'blocks_total': fs['BlocksTotal'], 'fsimage_path': str(latest),
            'fsimage_bytes': latest.stat().st_size, 'heap_used_samples': samples,
            'heap_used_after_gc_bytes': min(samples), 'namenode_pid': pid,
            'input_count': run(['hdfs', 'dfs', '-count', '/bonus/input'], label + '-input-count')[0].strip()}
    STATE['snapshots'][label] = snap
    save()
    print(json.dumps({'snapshot': label, **snap}), flush=True)


def split_source():
    print(f'{stamp()} Splitting source into 5000 whole-line files', flush=True)
    count = 0
    digest = hashlib.md5()
    with SOURCE.open('rb') as handle:
        for line in handle:
            count += 1
            digest.update(line)
    per_part, extra = divmod(count, 5000)
    if per_part == 0:
        raise RuntimeError('Not enough rows')
    joined = hashlib.md5()
    sizes = []
    with SOURCE.open('rb') as handle:
        for index in range(5000):
            rows = per_part + (index < extra)
            data = b''.join(itertools.islice(handle, rows))
            if not data.endswith(b'\n'):
                raise RuntimeError('A split does not end on a complete row')
            target = PARTS / f'part-{index:05d}.csv'
            target.write_bytes(data)
            joined.update(data)
            sizes.append(len(data))
        if handle.read(1):
            raise RuntimeError('Unconsumed data')
    if joined.digest() != digest.digest() or sum(sizes) != SOURCE.stat().st_size:
        raise RuntimeError('Split bytes differ from original')
    STATE['source'] = {'path': str(SOURCE), 'bytes': SOURCE.stat().st_size,
                       'lines_including_header': count, 'md5': digest.hexdigest(),
                       'parts_md5_concatenated': joined.hexdigest(), 'parts': 5000,
                       'min_part_bytes': min(sizes), 'max_part_bytes': max(sizes),
                       'min_part_lines': per_part, 'max_part_lines': per_part + bool(extra)}
    save()
    print(json.dumps(STATE['source']), flush=True)


def grep_job(kind, path):
    args = ['hadoop', 'jar', JAR, 'grep', '-Dmapreduce.framework.name=local',
            '-Dmapreduce.local.map.tasks.maximum=4', '-Dmapreduce.task.io.sort.mb=16',
            path, '/bonus/results/' + kind, 'groceries|books']
    env = dict(os.environ, HADOOP_HEAPSIZE_MAX='2048')
    output, elapsed = run(args, 'grep-' + kind, env=env)
    splits = [int(n) for n in re.findall(r'number of splits:\s*(\d+)', output, flags=re.I)]
    processed = [int(n) for n in re.findall(r'Total input files to process\s*:\s*(\d+)', output)]
    result, _ = run(['hdfs', 'dfs', '-cat', '/bonus/results/' + kind + '/part-*'], 'grep-' + kind + '-result')
    selected = [line for line in output.splitlines()
                if re.search(r'number of splits:|Total input files to process|Job job_.*completed successfully|Map input records=|Map output records=|Reduce output records=|Total committed heap usage', line, re.I)]
    STATE.setdefault('mapreduce', {})[kind] = {'elapsed_seconds': elapsed,
        'splits_per_job': splits, 'input_files_per_job': processed,
        'selected_log_lines': selected, 'result': result.strip(),
        'result_md5': hashlib.md5(result.encode()).hexdigest()}
    save()
    print(json.dumps({'mapreduce': kind, **STATE['mapreduce'][kind]}), flush=True)


def main():
    ROOT.mkdir(exist_ok=False)
    PARTS.mkdir()
    save()
    split_source()
    run(['hdfs', 'dfs', '-ls', '/lab/hdfs'], 'main-file-ls')
    run(['hdfs', 'dfs', '-du', '-s', '-h', '/lab/hdfs/transactions.csv'], 'main-file-du')
    run(['hdfs', 'dfs', '-mkdir', '-p', '/bonus/input'], 'create-input')
    snapshot('baseline')
    _, STATE['single_upload_seconds'] = run(['hdfs', 'dfs', '-put', str(SOURCE), '/bonus/input/transactions.csv'], 'upload-single')
    save()
    snapshot('single')
    run(['hdfs', 'dfs', '-rm', '-skipTrash', '/bonus/input/transactions.csv'], 'remove-single-comparison-copy')
    _, STATE['small_upload_seconds'] = run(['hdfs', 'dfs', '-put', *map(str, sorted(PARTS.glob('part-*.csv'))), '/bonus/input/'], 'upload-small')
    save()
    snapshot('small')
    run(['hdfs', 'dfs', '-mv', '/bonus/input', '/bonus/small-input'], 'keep-small-input')
    run(['hdfs', 'dfs', '-mkdir', '-p', '/bonus/big-input', '/bonus/results'], 'create-job-paths')
    run(['hdfs', 'dfs', '-put', str(SOURCE), '/bonus/big-input/transactions.csv'], 'restore-single-for-mapreduce')
    grep_job('single', '/bonus/big-input/transactions.csv')
    grep_job('small', '/bonus/small-input')
    if STATE['mapreduce']['single']['result'] != STATE['mapreduce']['small']['result']:
        raise RuntimeError('Grep results differ')
    if STATE['mapreduce']['single']['splits_per_job'][0] != 3:
        raise RuntimeError('Unexpected big-file split count')
    if STATE['mapreduce']['small']['splits_per_job'][0] != 5000:
        raise RuntimeError('Unexpected small-file split count')
    run(['hdfs', 'fsck', '/bonus', '-files', '-blocks'], 'final-bonus-fsck')
    run(['hdfs', 'dfsadmin', '-report'], 'final-report')
    STATE['finished_utc'] = stamp()
    STATE['status'] = 'complete'
    save()
    print(f'{stamp()} ALL BONUS EXPERIMENTS COMPLETE', flush=True)


if __name__ == '__main__':
    try:
        main()
    except BaseException as error:
        if ROOT.exists():
            STATE['status'] = 'failed'
            STATE['error'] = str(error)
            save()
        raise

```

</details>

## 5. Контроль однакового локального джерела

Перший великий файл завантажувався з Windows bind mount `/data`.
Тому після основного прогону виконано окремий контроль: 5000 частин послідовно
з'єднано в Linux-копію `/tmp/hdfs-smallfiles-bonus/single-staged.csv`, перевірено
MD5 і розмір, потім виміряно один `hdfs dfs -put` з того самого типу сховища,
де лежали дрібні частини. Саме цей час використано в основній таблиці бонусу.

Фактичні команди із кореня проєкту:

```powershell
docker cp .cache/lab4_bonus/control_upload.py lab4-hdfs-namenode-1:/tmp/control_hdfs_bonus.py
docker exec lab4-hdfs-namenode-1 python3 /tmp/control_hdfs_bonus.py
```

Усередині контрольного коду після підготовки Linux-копії:

```bash
hdfs dfs -test -e /bonus/upload-control.csv
hdfs dfs -put /tmp/hdfs-smallfiles-bonus/single-staged.csv /bonus/upload-control.csv
hdfs dfs -rm -skipTrash /bonus/upload-control.csv
hdfs fsck /bonus
hdfs dfsadmin -report
```

Поділ, з'єднання частин і MD5 не входять у час `put`. Контрольну HDFS-копію
видалено після вимірювання. Початковий час з `/data` також наведено в результатах.
Метадані одного файла й 5000 файлів виміряно раніше, на однаковому `/bonus/input`,
до MapReduce і до цього контрольного завантаження.

<details>
<summary>Показати control_upload.py</summary>

```python
from pathlib import Path
import hashlib
import json
import subprocess
import time
from datetime import datetime, timezone

root = Path('/tmp/hdfs-smallfiles-bonus')
state = json.loads((root / 'results.json').read_text())
if state.get('status') != 'complete':
    raise RuntimeError('The primary run must finish first')
target = root / 'single-staged.csv'
digest = hashlib.md5()
parts = sorted((root / 'parts').glob('part-*.csv'))
assert len(parts) == 5000
with target.open('wb') as out:
    for part in parts:
        content = part.read_bytes()
        out.write(content)
        digest.update(content)
assert target.stat().st_size == state['source']['bytes']
assert digest.hexdigest() == state['source']['md5']
test = subprocess.run(['hdfs', 'dfs', '-test', '-e', '/bonus/upload-control.csv'])
assert test.returncode == 1
args = ['hdfs', 'dfs', '-put', str(target), '/bonus/upload-control.csv']
started = datetime.now(timezone.utc).isoformat()
begin = time.perf_counter()
with (root / 'upload-single-linux.log').open('w') as handle:
    result = subprocess.run(args, stdout=handle, stderr=subprocess.STDOUT)
elapsed = time.perf_counter() - begin
assert result.returncode == 0
control = {'argv': args, 'started_utc': started,
           'finished_utc': datetime.now(timezone.utc).isoformat(),
           'single_upload_seconds': elapsed, 'returncode': result.returncode,
           'staged_bytes': target.stat().st_size, 'staged_md5': digest.hexdigest()}
(root / 'control.json').write_text(json.dumps(control, indent=2) + '\n')
print(json.dumps(control), flush=True)
for args, name in [
    (['hdfs', 'dfs', '-rm', '-skipTrash', '/bonus/upload-control.csv'], 'remove-upload-control'),
    (['hdfs', 'fsck', '/bonus'], 'final-bonus-fsck'),
    (['hdfs', 'dfsadmin', '-report'], 'final-report'),
]:
    with (root / (name + '.log')).open('w') as handle:
        subprocess.run(args, stdout=handle, stderr=subprocess.STDOUT, check=True)
print('LINUX SOURCE CONTROL COMPLETE', flush=True)

```

</details>
