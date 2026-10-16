"""Завантаження одного набору salesgen у PostgreSQL і DuckDB без перезапису таблиць.

Повний цикл: python load_data.py --generate
Імпорт готових CSV: python load_data.py КАТАЛОГ_З_MANIFEST
"""

import argparse
import json
import os
from pathlib import Path
import sys
import time

import psycopg
from psycopg import sql

from connections import BASE_DIR, connect_duckdb, connect_postgres
from salesgen.schema import LOAD_ORDER, TABLES, schema_for_manifest
from salesgen.validate import validate


KEYS = {
    "units": "unit_id", "categories": "category_id", "product_types": "product_type_id",
    "stores": "store_id", "products": "product_id", "receipts": "receipt_id",
    "receipt_items": "receipt_id, line_no",
}
DIMENSIONS = {"units", "categories", "product_types", "stores", "products"}
UNIQUE = {"units": "code", "categories": "name", "product_types": "category_id, name"}
REFERENCES = {
    "product_types": [("category_id", "categories", "category_id")],
    "products": [("product_type_id", "product_types", "product_type_id"), ("unit_id", "units", "unit_id")],
    "receipts": [("store_id", "stores", "store_id")],
    "receipt_items": [("receipt_id", "receipts", "receipt_id"), ("product_id", "products", "product_id")],
}
CHECKS = {
    "stores": [
        "CHECK ((latitude IS NULL) = (longitude IS NULL))",
        "CHECK (latitude BETWEEN -90 AND 90)", "CHECK (longitude BETWEEN -180 AND 180)",
    ],
    "units": ["CHECK (quantity_scale IN (0, 3))"],
    "products": ["CHECK (current_price BETWEEN 0 AND 99999999.99)"],
    "receipts": ["CHECK (total_amount BETWEEN 0 AND 999999999999.99)"],
    "receipt_items": [
        "CHECK (line_no > 0)", "CHECK (quantity > 0 AND quantity <= 9999999.999)",
        "CHECK (unit_price BETWEEN 0 AND 99999999.99)",
        "CHECK (line_amount BETWEEN 0 AND 999999999999.99)",
        "CHECK (line_amount = ROUND(quantity * unit_price, 2))",
    ],
}


def create_database():
    """Окрема база з .env; жодних DROP або змін наявних баз."""
    name = os.getenv("PG_DB", "storage_retrieval_lab")
    with psycopg.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"), port=int(os.getenv("PG_PORT", "5432")),
        user=os.getenv("PG_USER", "postgres"), password=os.getenv("PG_PASSWORD", ""),
        dbname="postgres", connect_timeout=5, autocommit=True,
    ) as connection:
        if not connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            connection.execute(sql.SQL("CREATE DATABASE {} ENCODING 'UTF8' TEMPLATE template0").format(sql.Identifier(name)))
            print(f"Створено PostgreSQL-базу {name}")


def create_tables(connection, backend):
    for table in LOAD_ORDER:
        definitions = [f'"{column}" {kind}' + ("" if nullable else " NOT NULL")
                       for column, kind, nullable in TABLES[table]]
        definitions += CHECKS.get(table, [])
        if backend == "duckdb" and table in DIMENSIONS:
            definitions.append(f"PRIMARY KEY ({KEYS[table]})")
            if table in UNIQUE:
                definitions.append(f"UNIQUE ({UNIQUE[table]})")
            for column, parent, key in REFERENCES.get(table, []):
                definitions.append(f"FOREIGN KEY ({column}) REFERENCES {parent} ({key})")
        # У PostgreSQL ключі та індекси будуємо після пакетного імпорту.
        prefix = "public." if backend == "postgresql" else ""
        connection.execute(f'CREATE TABLE {prefix}"{table}" (' + ", ".join(definitions) + ")")


def postgres_constraints(connection):
    for table in LOAD_ORDER:
        include = " INCLUDE (product_id)" if table == "receipt_items" else ""
        connection.execute(f'ALTER TABLE public."{table}" ADD PRIMARY KEY ({KEYS[table]}){include}')
        if table in UNIQUE:
            connection.execute(f'ALTER TABLE public."{table}" ADD UNIQUE ({UNIQUE[table]})')
    for table, references in REFERENCES.items():
        for column, parent, key in references:
            connection.execute(f'ALTER TABLE public."{table}" ADD FOREIGN KEY ({column}) REFERENCES public."{parent}" ({key})')
    connection.execute("CREATE INDEX idx_receipts_time_cover ON public.receipts (sold_at) INCLUDE (receipt_id, store_id, total_amount)")
    connection.execute("CREATE INDEX idx_receipt_items_product_receipt_cover ON public.receipt_items (product_id, receipt_id) INCLUDE (quantity)")


def compare(pg, duck, manifest):
    """Перевіряємо кожне значення в однаковому порядку; буфер — 2000 рядків."""
    counts = {}
    for table in LOAD_ORDER:
        query = f'SELECT * FROM "{table}" ORDER BY {KEYS[table]}'
        count = 0
        with pg.cursor(name=f"verify_{table}") as cursor:
            cursor.execute(query)
            result = duck.execute(query)
            while True:
                left, right = cursor.fetchmany(2000), result.fetchmany(2000)
                if left != right:
                    raise ValueError(f"Дані двох баз відрізняються: {table}, після рядка {count}")
                if not left:
                    break
                count += len(left)
        if count != manifest["row_counts"][table]:
            raise ValueError(f"Неправильна кількість рядків у {table}")
        counts[table] = count
        print(f"  {table}: {count:,} — усі значення збігаються")
    return counts


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", nargs="?", type=Path, help="Каталог з manifest.json; типово DATA_DIR з .env")
    parser.add_argument("--generate", action="store_true", help="Спочатку згенерувати повний набір: 3 млн чеків за 2025 рік")
    args = parser.parse_args()
    source = args.source if args.source is not None else Path(os.getenv("DATA_DIR", "data/source"))
    if args.source is None and not source.is_absolute():
        source = BASE_DIR / source
    source = source.resolve()
    if args.generate:
        # Перевіряємо цілі до тривалої генерації; наявні дані не стираємо.
        create_database()
        with connect_postgres() as pg:
            tables = {row[0] for row in pg.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")}
            if tables & set(TABLES):
                raise RuntimeError("PostgreSQL уже містить цільові таблиці. Генерацію не розпочато.")
        duck_path = Path(os.getenv("DUCKDB_PATH", "data/sales.duckdb"))
        if not duck_path.is_absolute():
            duck_path = BASE_DIR / duck_path
        if duck_path.exists():
            with connect_duckdb() as duck:
                if {row[0] for row in duck.execute("SHOW TABLES")} & set(TABLES):
                    raise RuntimeError("DuckDB уже містить цільові таблиці. Генерацію не розпочато.")
        from salesgen.config import Config
        from salesgen.writer import run
        print(f"Генерація повного набору в {source}", flush=True)
        run(Config.from_json(BASE_DIR / "configs" / "full.json"), source)
    print(f"Перевірка CSV у {source}", flush=True)
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))["data"]
    if manifest["schema"] != schema_for_manifest():
        raise ValueError("Схема набору не відповідає погодженій схемі")
    paths = {}
    for table in LOAD_ORDER:
        paths[table] = [(source / entry["path"]).resolve() for entry in manifest["files"] if entry["table"] == table]
        if not paths[table] or any(not path.is_relative_to(source) for path in paths[table]):
            raise ValueError(f"Неправильні шляхи CSV: {table}")
    errors, _ = validate(source)
    if not errors.ok:
        raise ValueError(f"Набір не пройшов перевірку: {dict(errors.counts)}")

    create_database()
    with connect_postgres() as pg, connect_duckdb(read_only=False) as duck:
        pg.execute("SET search_path TO public")
        pg_tables = {row[0] for row in pg.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()}
        duck_tables = {row[0] for row in duck.execute("SHOW TABLES").fetchall()}
        if (pg_tables | duck_tables) & set(TABLES):
            raise RuntimeError("Цільові таблиці вже існують. Автоматичного очищення або повторного завантаження немає.")
        duck.execute("SET preserve_insertion_order = true")
        duck.execute("BEGIN TRANSACTION")
        try:
            with pg.transaction():
                pg_start = time.perf_counter()
                create_tables(pg, "postgresql")
                for table in LOAD_ORDER:
                    print(f"PostgreSQL: завантаження {table}", flush=True)
                    for path in paths[table]:
                        statement = sql.SQL("COPY public.{} FROM STDIN WITH (FORMAT CSV, HEADER TRUE, ENCODING 'UTF8')").format(sql.Identifier(table))
                        with pg.cursor().copy(statement) as copy, path.open("rb") as file:
                            for block in iter(lambda: file.read(1 << 20), b""):
                                copy.write(block)
                print("PostgreSQL: побудова ключів та індексів", flush=True)
                postgres_constraints(pg)
                print(f"PostgreSQL: імпорт і побудова обмежень/індексів — {time.perf_counter() - pg_start:.2f} с")

                duck_start = time.perf_counter()
                create_tables(duck, "duckdb")
                for table in LOAD_ORDER:
                    print(f"DuckDB: завантаження {table}", flush=True)
                    columns = {column: kind for column, kind, _ in TABLES[table]}
                    order = "sold_at, receipt_id" if table == "receipts" else KEYS[table]
                    duck.execute(
                        f'INSERT INTO "{table}" SELECT * FROM read_csv(?, header=true, columns=?, auto_detect=false, nullstr=\'\') ORDER BY {order}',
                        [[str(path) for path in paths[table]], columns],
                    )
                print(f"DuckDB: імпорт впорядкованих таблиць — {time.perf_counter() - duck_start:.2f} с")
                print("Порівняння всіх записів у двох базах", flush=True)
                compare(pg, duck, manifest)
                # Сума чеків повинна дорівнювати сумі округлених позицій.
                total = pg.execute("SELECT (SELECT SUM(total_amount) FROM receipts), (SELECT SUM(line_amount) FROM receipt_items)").fetchone()
                if total[0] != total[1]:
                    raise ValueError("Сума чеків не відповідає сумі позицій")
                print(f"Загальна виручка в обох базах: {total[0]} UAH")
            duck.execute("COMMIT")
        except BaseException:
            duck.execute("ROLLBACK")
            raise
        pg.execute("VACUUM (ANALYZE)")
        duck.execute("CHECKPOINT")
        print("Готово: дані збережені, підключення закриваються; можна відкривати бази у клієнтах.")


if __name__ == "__main__":
    main()
