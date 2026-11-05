"""Виручка міст: a — defaults, b — shuffle/skew, c — фільтр гостей перед join."""

import argparse
import csv
import hashlib
import json
from pathlib import Path
from statistics import median
from time import perf_counter

from pyspark.sql import SparkSession, functions as F, types as T

from part2 import explain_text, sql_execution_ids, write_json
from part3_metrics import describe_join_stage, describe_stages


USERS_SCHEMA = T.StructType([
    T.StructField("user_id", T.LongType(), True),
    T.StructField("city", T.StringType(), True),
    T.StructField("signup_date", T.DateType(), True),
])


def configure(spark, variant):
    if variant in ("b", "c"):
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
        spark.conf.set("spark.sql.adaptive.enabled", False)
        spark.conf.set("spark.sql.shuffle.partitions", 8)


def read_users(spark, path):
    return (spark.read.schema(USERS_SCHEMA).option("header", True)
            .option("mode", "FAILFAST").csv(str(path)))


def build_query(transactions, users, variant):
    tx = transactions.select("user_id", "amount")
    if variant == "c":
        # user_id=0 відсутній у users: для inner join можна прибрати гостей раніше.
        tx = tx.filter(F.col("user_id") != 0)
    joined = tx.join(users.select("user_id", "city"), on="user_id", how="inner")
    # Сортуємо вже маленький список міст на driver, щоб не додавати shuffle.
    return joined.groupBy("city").agg(F.sum("amount").alias("revenue"))


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def inspect_inputs(transactions, users):
    files = sorted(transactions.rglob("*.parquet"))
    if not files:
        raise ValueError(f"Немає Parquet-файлів: {transactions}. Спочатку виконайте частину 2")
    manifest = [{"file": str(path.relative_to(transactions)), "bytes": path.stat().st_size,
                 "sha256": sha256(path)} for path in files]
    ids, cities = set(), set()
    with users.open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["user_id", "city", "signup_date"]:
            raise ValueError("Некоректний заголовок users.csv")
        for row in reader:
            user_id = int(row["user_id"])
            if user_id <= 0 or user_id in ids or not row["city"]:
                raise ValueError("users.csv має містити унікальних додатних user_id та непорожні міста")
            ids.add(user_id)
            cities.add(row["city"])
    if not ids:
        raise ValueError("Порожній довідник users.csv")
    return {
        "parquet_files": manifest, "parquet_bytes": sum(item["bytes"] for item in manifest),
        "parquet_fingerprint": hashlib.sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
        "users_sha256": sha256(users), "users_bytes": users.stat().st_size,
        "registered_users": len(ids), "cities": len(cities), "guest_user_in_directory": False,
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", choices=("a", "b", "c"), required=True)
    parser.add_argument("--transactions", default="/work2/data/tx.parquet")
    parser.add_argument("--users", default="/work2/data/users.csv")
    parser.add_argument("--metadata", help="Метадані генератора; типово transactions.metadata.json поруч із Parquet")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("Потрібно щонайменше три вимірювані повтори")
    return args


def main():
    args = parse_args()
    transactions_path, users_path = Path(args.transactions).resolve(), Path(args.users).resolve()
    inputs = inspect_inputs(transactions_path, users_path)
    metadata_path = Path(args.metadata) if args.metadata else transactions_path.parent / "transactions.metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else None
    if metadata and metadata["users"]["sha256"] != inputs["users_sha256"]:
        raise ValueError("users.csv не відповідає метаданим генератора")
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    plans_dir = output_dir / "plans"
    plans_dir.mkdir(exist_ok=True)
    spark = SparkSession.builder.appName(f"Lab5_Spark_2_Part3_{args.variant.upper()}").getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    summary = {
        "variant": args.variant, "application": sc.appName, "application_id": sc.applicationId,
        "spark_version": spark.version, "master": sc.master, "inputs": inputs,
        "transactions": str(transactions_path), "users": str(users_path), "generator_metadata": metadata,
        "warmups": 1, "repeats": args.repeats, "runs": [],
        "timing_scope": "query.collect() only; excludes input fingerprinting, DataFrame creation, explain, metrics and driver sorting",
        "cache_policy": "no Spark cache/persist; OS cache is not cleared",
    }
    try:
        if sc.master != "local[4]":
            raise ValueError(f"За умовою потрібен local[4], отримано {sc.master}")
        configure(spark, args.variant)
        settings = (
            "spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions",
            "spark.sql.join.preferSortMergeJoin", "spark.sql.adaptive.skewJoin.enabled",
            "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
            "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
        )
        summary["settings"] = {key: spark.conf.get(key) for key in settings}
        summary["broadcast_threshold_bytes"] = int(spark._jsparkSession.sessionState().conf().autoBroadcastJoinThreshold())
        print(f"Application: {sc.appName}; ID: {sc.applicationId}; master={sc.master}", flush=True)
        print(f"Transactions: {transactions_path}; data_bytes={inputs['parquet_bytes']}", flush=True)
        print(f"Users: {users_path}; bytes={inputs['users_bytes']}; users={inputs['registered_users']}; cities={inputs['cities']}", flush=True)
        print("Settings: " + json.dumps(summary["settings"]), flush=True)
        if metadata:
            print(f"Generator: rows={metadata['transaction_rows']}; guest_percent={metadata['guest_percent']}; "
                  f"guest_rows={metadata['guest_rows']}; seed={metadata['seed']}", flush=True)
        expected = None
        for run in range(args.repeats + 1):
            label = "warmup" if run == 0 else f"repeat_{run}"
            group = f"part3_{args.variant}_{label}"
            sc.setJobGroup(f"{group}_schema", f"SCHEMA | {label} | tx={transactions_path}")
            tx = spark.read.parquet(str(transactions_path))
            users = read_users(spark, users_path)
            # Група охоплює саме запит, без допоміжного читання схеми Parquet.
            sc.setJobGroup(group, f"{args.variant} | {label} | tx={transactions_path} | users={users_path}")
            query = build_query(tx, users, args.variant)
            if run == 0:
                summary["users_estimated_plan_bytes"] = int(str(users.select("user_id", "city")._jdf.queryExecution().optimizedPlan().stats().sizeInBytes()))
            (plans_dir / f"{label}_before.txt").write_text(explain_text(query), encoding="utf-8")
            previous_sql_ids = sql_execution_ids(spark)
            started = perf_counter()
            rows = query.collect()
            elapsed = perf_counter() - started
            result = sorted([{"city": row["city"], "revenue": str(row["revenue"])} for row in rows], key=lambda row: row["city"])
            if not result:
                raise ValueError("Inner join не повернув жодного міста")
            if expected is None:
                expected = result
            elif result != expected:
                raise RuntimeError("Результати повторів відрізняються")
            sql_ids = sorted(sql_execution_ids(spark) - previous_sql_ids)
            if len(sql_ids) != 1:
                raise RuntimeError(f"Очікувався один SQL execution, отримано {sql_ids}")
            job = describe_stages(sc, group)
            after = explain_text(query)
            strategy = next((name for name in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin") if name in after), "CHECK PLAN")
            record = {"label": label, "seconds": elapsed, "sql_execution_id": sql_ids[0],
                      "join_strategy": strategy, "result": result, **job}
            if args.variant in ("b", "c"):
                record["join_stage"] = describe_join_stage(sc, job["stages"])
            summary["runs"].append(record)
            (plans_dir / f"{label}_after.txt").write_text(after, encoding="utf-8")
            print(f"\n{args.variant} | {label}; seconds={elapsed:.6f}; SQL execution={sql_ids[0]}; "
                  f"join={strategy}; jobs={job['job_ids']}; total Shuffle Write bytes={job['total_shuffle_write_bytes']}", flush=True)
            for stage in job["stages"]:
                print(f"  stage={stage['stage_id']}; tasks={stage['tasks']}; Shuffle Read bytes={stage['shuffle_read_bytes']}; "
                      f"Shuffle Read records={stage['shuffle_read_records']}; Shuffle Write bytes={stage['shuffle_write_bytes']}", flush=True)
            if "join_stage" in record:
                join = record["join_stage"]
                print(f"  JOIN STAGE: {join['stage_id']}; task index | task ID | Shuffle Read Records | Shuffle Read bytes", flush=True)
                for task in join["task_metrics"]:
                    print(f"  {task['index']} | {task['task_id']} | {task['shuffle_read_records']} | {task['shuffle_read_bytes']}", flush=True)
                print(f"  max/median records={join['max_over_median']:.6f}; source Shuffle Write bytes={join['source_shuffle_write_bytes']}", flush=True)
            for row in result:
                print(f"  {row['city']} | {row['revenue']}", flush=True)
            print("  Repeats equality: OK", flush=True)
            if run == 1:
                (output_dir / "explain.txt").write_text(after, encoding="utf-8")
                (output_dir / "explain_formatted.txt").write_text(explain_text(query, "formatted"), encoding="utf-8")
                print("\nPlan repeat_1:\n" + after, flush=True)
            write_json(output_dir / "results.json", summary)

        measured = summary["runs"][1:]
        times = [run["seconds"] for run in measured]
        summary.update({"status": "complete", "result": expected, "repeats_equal": True,
                        "seconds": times, "median_seconds": median(times)})
        if args.variant in ("b", "c"):
            with (output_dir / "join_tasks.csv").open("w", encoding="utf-8-sig", newline="") as stream:
                fields = ["label", "stage_id"] + list(measured[0]["join_stage"]["task_metrics"][0])
                writer = csv.DictWriter(stream, fieldnames=fields)
                writer.writeheader()
                for run in measured:
                    for task in run["join_stage"]["task_metrics"]:
                        writer.writerow({"label": run["label"], "stage_id": run["join_stage"]["stage_id"], **task})
        with (output_dir / "city_revenue.csv").open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=["city", "revenue"])
            writer.writeheader()
            writer.writerows(expected)
        write_json(output_dir / "results.json", summary)
        print("\nMeasured seconds: " + ", ".join(f"{value:.6f}" for value in times), flush=True)
        print(f"Median seconds: {median(times):.6f}", flush=True)
        print(f"Screenshots: {args.variant} repeat_1 | SQL execution={measured[0]['sql_execution_id']}", flush=True)
        if "join_stage" in measured[0]:
            print(f"Screenshots: join stage={measured[0]['join_stage']['stage_id']}; 8 tasks", flush=True)
        print(f"Results: {output_dir}", flush=True)
        print("After a, b and c: python compare_part3.py", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
