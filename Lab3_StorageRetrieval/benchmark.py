"""Один запуск замірів: python benchmark.py (SQL у queries.sql)."""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import re
import statistics
import sys
import threading
import tempfile
import time
import uuid

import duckdb
import psycopg

from connections import BASE_DIR, connect_duckdb, connect_postgres


def read_queries(path):
    source = path.read_text(encoding="utf-8")
    markers = list(re.finditer(r"^-- query: (\d+) \| (.+)$", source, re.MULTILINE))
    queries = []
    for i, match in enumerate(markers):
        end = markers[i + 1].start() if i + 1 < len(markers) else len(source)
        queries.append({"query": int(match[1]), "name": match[2].strip(),
                        "sql": source[match.end():end].strip()})
    if [q["query"] for q in queries] != list(range(1, 10)):
        raise ValueError("queries.sql має містити рівно запити 1–9")
    return queries


def execute(connection, backend, statement, timeout):
    """Від відправлення SQL до отримання всіх рядків; без відкриття з'єднання."""
    expired = threading.Event()
    timer = None
    if backend == "DuckDB":
        def interrupt():
            expired.set()
            connection.interrupt()
        timer = threading.Timer(timeout, interrupt)
        timer.daemon = True
        timer.start()
    start = time.perf_counter()
    try:
        rows = connection.execute(statement).fetchall()
        elapsed = time.perf_counter() - start
        if expired.is_set():
            raise TimeoutError("Запит перевищив ліміт часу")
        return elapsed, rows
    except (psycopg.errors.QueryCanceled, duckdb.InterruptException) as error:
        raise TimeoutError(str(error)) from error
    finally:
        if timer:
            timer.cancel()
            timer.join()


def write_result(path, contents, encoding):
    """Повторюємо заміну при блокуванні; завершений знімок завжди має окремий файл."""
    with tempfile.NamedTemporaryFile(mode="w", encoding=encoding, newline="", dir=path.parent,
                                     prefix=f".{path.name}-", suffix=".tmp", delete=False) as file:
        temporary = Path(file.name)
        file.write(contents)
    for attempt in range(6):
        try:
            temporary.replace(path)
            return path
        except PermissionError:
            if attempt < 5:
                time.sleep(min(0.1 * 2 ** attempt, 0.5))
    # Відкритий CSV/JSON може забороняти видалення старого файла на Windows.
    # Пишемо під новою назвою й використовуємо її до кінця цього прогону.
    fallback = path.with_name(f"{path.stem}-recovery-{uuid.uuid4().hex[:8]}{path.suffix}")
    try:
        temporary.rename(fallback)
    except PermissionError:
        fallback = temporary  # Повний знімок уже записаний, навіть якщо заблокували його переміщення.
    print(f"Файл {path.name} заблокований. Результати зберігаються тут: {fallback}", flush=True)
    return fallback


def save(report, directory, paths=None):
    directory.mkdir(parents=True, exist_ok=True)
    if paths is None:
        paths = {}
    json_path = paths.setdefault("json", directory / "benchmark.json")
    csv_path = paths.setdefault("csv", directory / "benchmark.csv")
    paths["json"] = write_result(json_path, json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", "utf-8")
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(["query", "database", "status", "runs", "median_ms", "min_ms", "max_ms", "results_equal"])
    for record in report["measurements"]:
        complete = record["status"] == "ok"
        samples = record["seconds"]
        writer.writerow([record["query"], record["database"], record["status"], len(samples),
                         round(statistics.median(samples) * 1000, 3) if complete else "",
                         round(min(samples) * 1000, 3) if samples else "",
                         round(max(samples) * 1000, 3) if samples else "",
                         record.get("results_equal", "")])
    paths["csv"] = write_result(csv_path, buffer.getvalue(), "utf-8-sig")
    return paths


def metadata(connection, backend):
    if backend == "PostgreSQL":
        settings = connection.execute("SELECT name, setting, unit FROM pg_settings WHERE name IN "
                                      "('work_mem','shared_buffers','max_parallel_workers_per_gather','jit',"
                                      "'effective_cache_size','data_directory') ORDER BY name").fetchall()
        return {"version": connection.execute("SELECT version()").fetchone()[0], "settings": settings,
                "database_size_bytes": connection.execute("SELECT pg_database_size(current_database())").fetchone()[0],
                "indexes": connection.execute("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname").fetchall()}
    path = Path(os.getenv("DUCKDB_PATH", "data/sales.duckdb"))
    if not path.is_absolute():
        path = BASE_DIR / path
    return {"version": connection.execute("SELECT version()").fetchone()[0],
            "settings": connection.execute("SELECT name,value FROM duckdb_settings() WHERE name IN ('threads','memory_limit','temp_directory') ORDER BY name").fetchall(),
            "database_file_bytes": path.stat().st_size}


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queries", nargs="+", type=int, choices=range(1, 10), default=list(range(1, 10)))
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=600, help="Ліміт одного виконання в секундах, максимум 600")
    parser.add_argument("--output", type=Path, default=BASE_DIR / "results")
    args = parser.parse_args()
    if args.runs < 5 or not 0 < args.timeout <= 600:
        parser.error("Потрібно щонайменше 5 запусків і ліміт у межах (0, 600] секунд")
    sql_path = BASE_DIR / "queries.sql"
    queries = [q for q in read_queries(sql_path) if q["query"] in args.queries]
    report = {
        "started_at_utc": datetime.now(timezone.utc).isoformat(), "finished": False,
        "method": {"warmups_per_query_per_database": 1, "measured_runs": args.runs,
                   "timeout_seconds": args.timeout, "cache": "warm; OS and database caches are not cleared",
                   "timing": "perf_counter around execute + fetchall; connection and EXPLAIN excluded",
                   "order": "PostgreSQL queries first, then DuckDB; no concurrent queries",
                   "period_3_to_6": "[2025-07-01,2025-08-01) UTC", "period_7_to_9": "[2025-07-15,2025-07-16) UTC",
                   "product_A": 568, "store_B": 90, "combinations": "distinct product_type_id per receipt"},
        "python": platform.python_version(), "platform": platform.platform(),
        "sql_sha256": hashlib.sha256(sql_path.read_bytes()).hexdigest(),
        "queries": queries, "databases": {}, "measurements": [],
    }
    hardware = BASE_DIR / "results" / "hardware.json"
    if hardware.exists():
        report["hardware"] = json.loads(hardware.read_text(encoding="utf-8-sig"))
    source = Path(os.getenv("DATA_DIR", "data/source"))
    if not source.is_absolute():
        source = BASE_DIR / source
    manifest = source / "manifest.json"
    if manifest.exists():
        data = json.loads(manifest.read_text(encoding="utf-8"))["data"]
        report["dataset"] = {key: data[key] for key in ("seed", "row_counts", "data_fingerprint_sha256")}
    expected = {}
    paths = {}
    try:
        # Відкриття обох перед стартом виявляє блокування файлу DuckDB завчасно.
        with connect_postgres(), connect_duckdb():
            pass
        for backend, connect in (("PostgreSQL", connect_postgres), ("DuckDB", connect_duckdb)):
            with connect() as connection:
                if backend == "PostgreSQL":
                    connection.execute("SET default_transaction_read_only = on")
                    connection.execute("SELECT set_config('statement_timeout', %s, false)", (str(int(args.timeout * 1000)),))
                else:
                    connection.execute("SET enable_progress_bar = false")
                report["databases"][backend] = metadata(connection, backend)
                for query in queries:
                    record = {"query": query["query"], "name": query["name"], "database": backend,
                              "status": "running", "seconds": []}
                    report["measurements"].append(record)
                    save(report, args.output, paths)
                    try:
                        print(f"{backend} / Q{query['query']}: прогрів…", flush=True)
                        elapsed, baseline = execute(connection, backend, query["sql"], args.timeout)
                        record["warmup_seconds"] = elapsed
                        record["result"] = baseline
                        key = query["query"]
                        if key in expected:
                            record["results_equal"] = baseline == expected[key]
                            if not record["results_equal"]:
                                raise ValueError("Результати PostgreSQL і DuckDB відрізняються")
                            for prior in report["measurements"]:
                                if prior["query"] == key:
                                    prior["results_equal"] = True
                        else:
                            expected[key] = baseline
                        for repeat in range(1, args.runs + 1):
                            elapsed, rows = execute(connection, backend, query["sql"], args.timeout)
                            if rows != baseline:
                                raise ValueError("Результат змінився між повторами")
                            record["seconds"].append(elapsed)
                            save(report, args.output, paths)
                            print(f"  {repeat}/{args.runs}: {elapsed * 1000:.2f} мс", flush=True)
                        record["status"] = "ok"
                        print(f"  Медіана: {statistics.median(record['seconds']) * 1000:.2f} мс", flush=True)
                        try:
                            record["plan"] = connection.execute("EXPLAIN " + query["sql"]).fetchall()
                        except Exception as error:
                            record["plan_error"] = str(error)
                    except TimeoutError as error:
                        record["status"] = "timeout"
                        record["error"] = str(error)
                        print(f"  Ліміт {args.timeout:g} с: зупинено, решту повторів пропущено", flush=True)
                    except Exception as error:
                        record["status"] = "error"
                        record["error"] = str(error)
                        print(f"  Помилка: {error}", flush=True)
                    finally:
                        save(report, args.output, paths)
        report["finished"] = True
    except KeyboardInterrupt:
        if report["measurements"] and report["measurements"][-1]["status"] == "running":
            report["measurements"][-1]["status"] = "interrupted"
        print("Зупинено користувачем; отримані виміри збережено.")
    finally:
        report["ended_at_utc"] = datetime.now(timezone.utc).isoformat()
        save(report, args.output, paths)
    print(f"Результати: {paths['csv']}")
    print(f"Подробиці, SQL, відповіді та плани: {paths['json']}")
    if not report["finished"] or any(r["status"] == "error" for r in report["measurements"]):
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
