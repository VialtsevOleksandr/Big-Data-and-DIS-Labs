# Аналіз транзакцій через MapReduce

Два послідовні Hadoop Streaming job на Python:

1. Сума транзакцій за кожною парою дата/категорія.
2. Категорія з найбільшою сумою за весь період. Вхід — результат першого job.

## Структура

| Файл | Призначення |
|---|---|
| `generate_transactions.py` | Генерація CSV транзакцій |
| `mapper1.py` | Транзакція → ключ дата/категорія та сума в копійках |
| `reducer1.py` | Денна сума за категорією; також combiner першого job |
| `mapper2.py` | Денний підсумок → категорія та сума |
| `reducer2.py` | Загальні суми категорій і вибір лідера |
| `amounts.py` | Перетворення грошових сум у копійки й назад |
| `driver.py` | Доставлення файлів у Docker, запуск job і збереження результатів |

## Запуск

Потрібні Python 3, Docker і запущений кластер Hadoop Streaming із Python 3 на робочому вузлі. Driver за замовчуванням використовує контейнери `hadoop-namenode-1` і `hadoop-nodemanager-1`.

Із папки `Lab3_MapReduce`:

```bash
python generate_transactions.py 369 transactions.csv
python driver.py
```

Перший job використовує два reducer-и та combiner, другий — один reducer. Параметри можна змінити:

```bash
python driver.py --reducers1 4
python driver.py --no-combiner
python driver.py --container my-namenode --worker-container my-nodemanager
```

Результати зберігаються в `runs/<run_id>/`: `daily_totals.tsv` містить денні суми в копійках, `winner.tsv` — переможця з десятковою сумою, `job1.log` і `job2.log` — журнали Hadoop, `run.json` — параметри запуску.

CSV, папка `runs` і Word-звіт ігноруються Git.
