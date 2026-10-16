"""Пункт 4 звіту: python index_experiment.py — запити 3–5 з індексом і без нього."""

import argparse
import csv
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import statistics
import sys

from benchmark import execute, metadata, read_queries, write_result
from connections import BASE_DIR, connect_postgres


INDEX = "public.idx_receipt_items_product_receipt_cover"


def save(report, directory, paths):
    directory.mkdir(parents=True, exist_ok=True)
    paths["json"] = write_result(paths.get("json", directory / "index-experiment.json"),
                                  json.dumps(report, ensure_ascii=False, indent=2, default=str) + "\n", "utf-8")
    buffer = io.StringIO(newline="")
    writer = csv.writer(buffer)
    writer.writerow(["query", "phase", "status", "runs", "median_ms", "min_ms", "max_ms", "results_equal"])
    for record in report["measurements"]:
        samples = record["seconds"]
        writer.writerow([record["query"], record["phase"], record["status"], len(samples),
                         round(statistics.median(samples) * 1000, 3) if record["status"] == "ok" else "",
                         round(min(samples) * 1000, 3) if samples else "",
                         round(max(samples) * 1000, 3) if samples else "",
                         record.get("results_equal", "")])
    paths["csv"] = write_result(paths.get("csv", directory / "index-experiment.csv"), buffer.getvalue(), "utf-8-sig")


def measure(connection, phase, queries, args, report, paths, expected):
    for query in queries:
        key = query["query"]
        record = {"query": key, "phase": phase, "status": "running", "seconds": []}
        report["measurements"].append(record)
        save(report, args.output, paths)
        try:
            # У фазі без індексу це SAVEPOINT: таймаут не скасовує весь експеримент.
            with connection.transaction():
                print(f"{phase} / Q{key}: прогрів…", flush=True)
                elapsed, baseline = execute(connection, "PostgreSQL", query["sql"], args.timeout)
                record.update(warmup_seconds=elapsed, result=baseline)
                if key in expected:
                    record["results_equal"] = baseline == expected[key]
                    if not record["results_equal"]:
                        raise ValueError("Результат змінився після видалення індексу")
                    for prior in report["measurements"]:
                        if prior["query"] == key:
                            prior["results_equal"] = True
                else:
                    expected[key] = baseline
                for repeat in range(1, args.runs + 1):
                    elapsed, rows = execute(connection, "PostgreSQL", query["sql"], args.timeout)
                    if rows != baseline:
                        raise ValueError("Результат змінився між повторами")
                    record["seconds"].append(elapsed)
                    save(report, args.output, paths)
                    print(f"  {repeat}/{args.runs}: {elapsed * 1000:.2f} мс", flush=True)
                record["plan"] = connection.execute("EXPLAIN " + query["sql"]).fetchall()
                record["status"] = "ok"
                print(f"  Медіана: {statistics.median(record['seconds']) * 1000:.2f} мс", flush=True)
        except TimeoutError as error:
            record.update(status="timeout", error=str(error))
            print(f"  Зупинено за лімітом {args.timeout:g} с", flush=True)
        except Exception as error:
            record.update(status="error", error=str(error))
            raise
        finally:
            save(report, args.output, paths)


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--output", type=Path, default=BASE_DIR / "results")
    args = parser.parse_args()
    if args.runs < 5 or not 0 < args.timeout <= 600:
        parser.error("Потрібно щонайменше 5 повторів і ліміт у межах (0, 600] с")
    sql_path = BASE_DIR / "queries.sql"
    queries = [q for q in read_queries(sql_path) if q["query"] in (3, 4, 5)]
    report = {"started_at_utc": datetime.now(timezone.utc).isoformat(), "finished": False,
              "method": {"index": INDEX, "warmups": 1, "runs": args.runs, "timeout_seconds": args.timeout,
                         "order": ["with_index", "without_index"], "cache": "warm; caches not cleared",
                         "timing": "execute + fetchall; DDL, EXPLAIN and result saves excluded",
                         "restore": "DROP INDEX in a transaction, always followed by ROLLBACK",
                         "control_query": 3},
              "queries": queries, "sql_sha256": hashlib.sha256(sql_path.read_bytes()).hexdigest(),
              "measurements": []}
    paths = {}
    expected = {}
    try:
        with connect_postgres() as connection:
            connection.execute("SELECT set_config('application_name', 'storage_retrieval_index_experiment', false)")
            connection.execute("SELECT set_config('statement_timeout', %s, false)", (str(int(args.timeout * 1000)),))
            initial = connection.execute("SELECT to_regclass(%s)::oid", (INDEX,)).fetchone()[0]
            if initial is None:
                raise RuntimeError(f"Потрібний індекс відсутній: {INDEX}")
            report["database"] = metadata(connection, "PostgreSQL")
            measure(connection, "with_index", queries, args, report, paths, expected)
            connection.execute("BEGIN")
            try:
                connection.execute("SET LOCAL lock_timeout = '5s'")
                connection.execute(f"DROP INDEX {INDEX}")
                if connection.execute("SELECT to_regclass(%s)", (INDEX,)).fetchone()[0] is not None:
                    raise RuntimeError("Індекс не видалено")
                measure(connection, "without_index", queries, args, report, paths, expected)
            finally:
                connection.execute("ROLLBACK")
                restored = connection.execute("SELECT to_regclass(%s)::oid", (INDEX,)).fetchone()[0]
                report["index_restored"] = restored == initial
                if not report["index_restored"]:
                    raise RuntimeError("Не підтверджено відновлення початкового індексу")
            report["finished"] = True
    except KeyboardInterrupt:
        if report["measurements"] and report["measurements"][-1]["status"] == "running":
            report["measurements"][-1]["status"] = "interrupted"
        print("Зупинено користувачем; транзакцію скасовано.", flush=True)
    finally:
        report["ended_at_utc"] = datetime.now(timezone.utc).isoformat()
        save(report, args.output, paths)
    print(f"Результати: {paths['csv']}")
    print(f"Плани та всі повтори: {paths['json']}")
    return 0 if report["finished"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
