# HR-система в трьох моделях даних

Облік резюме здобувачів, реалізований одночасно в реляційній (PostgreSQL), документній (MongoDB) і графовій (Neo4j) базах даних. Одні й ті самі дані та п'ять однакових запитів у кожній базі.

📄 **Звіт:** [report.pdf](report.pdf)
📋 **Результати запуску:** [results.txt](results.txt)

## Структура

| Файл | Призначення |
|---|---|
| `config.py` | Параметри підключення до баз і параметри запитів |
| `generate_data.py` | Генерація 50 резюме з перетинами по містах, хобі та закладах → `data.json` |
| `postgres_runner.py` | Схема (7 таблиць), наповнення, 5 запитів SQL |
| `mongo_runner.py` | Колекція `users` з вкладеним резюме, наповнення, 5 запитів |
| `neo4j_runner.py` | Граф, обмеження унікальності, наповнення, 5 запитів Cypher |
| `run_all.py` | Наповнює всі бази й виконує запити, зберігає `results.txt` |
| `utils.py` | Форматування результатів |

## Запити

1. Резюме конкретного користувача
2. Усі унікальні хобі
3. Усі унікальні міста
4. Хобі здобувачів, що мешкають у заданому місті
5. Здобувачі, що працювали в одному й тому самому закладі

## Запуск

Потрібні PostgreSQL (локально), MongoDB і Neo4j (Docker):

```bash
docker run -d --name mongo -p 27017:27017 mongo:latest
docker run -d --name neo4j -p 7474:7474 -p 7687:7687 -e NEO4J_AUTH=neo4j/password123 neo4j:latest
```

Паролі задаються в `config.py` або змінними середовища `PG_PASSWORD`, `NEO4J_PASSWORD`.

```bash
pip install -r requirements.txt
python run_all.py           # наповнити бази і виконати запити
python run_all.py seed      # лише наповнити бази
python run_all.py queries   # лише виконати запити → results.txt
```

Запити окремої бази: `python postgres_runner.py`, `python mongo_runner.py`, `python neo4j_runner.py`.