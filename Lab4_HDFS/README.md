# HDFS — блоки, збої й відновлення

Три експерименти: розміщення блоків, втрата й повернення DataNode та виявлення
пошкодженої репліки. Бонус — порівняння одного CSV із 5000 дрібними файлами.

| Матеріал | Основна частина | Бонус |
|---|---|---|
| Команди | [commands.md](commands.md) | [commands_bonus.md](commands_bonus.md) |
| Результати та докази | [experiments.md](evidence/experiments.md) | [experiments_bonus.md](evidence/experiments_bonus.md) |

Конфігурація кластера: [docker-compose.yml](docker-compose.yml) і [hadoop.env](hadoop.env).
Hadoop 3.5.0, один NameNode, чотири DataNode, реплікація 3, блок 128 МіБ.

Для запуску потрібен `transactions.csv` у папці `Lab3_MapReduce` — великий CSV
не зберігається в Git. Його можна створити [генератором попередньої роботи](../Lab3_MapReduce/generate_transactions.py).
Команди експериментів виконуються в PowerShell із папки цієї лабораторної.
