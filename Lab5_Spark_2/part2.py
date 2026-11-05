"""DataFrame: топ-5 днів категорії на CSV, Parquet і Parquet за категоріями."""

import argparse
import csv
import hashlib
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from statistics import median
from time import perf_counter

from pyspark.sql import SparkSession, functions as F, types as T


SCHEMA = T.StructType([
    T.StructField("user_id", T.LongType(), True),
    T.StructField("timestamp", T.TimestampType(), True),
    T.StructField("category", T.StringType(), True),
    T.StructField("amount", T.DecimalType(18, 2), True),
])
SOURCES = ("csv", "parquet", "parquet_by_category")
SCAN_KEYS = ("numFiles", "filesSize", "numOutputRows")


def read_csv(spark, path):
    return (spark.read.schema(SCHEMA).option("header", True)
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
            .option("mode", "FAILFAST").csv(str(path)))


def build_query(df, category):
    # Той самий запит для всіх джерел. Суми Decimal, без похибок float.
    return (df.filter(F.col("category") == category)
            .groupBy(F.to_date("timestamp").alias("date"))
            .agg(F.sum("amount").alias("revenue"))
            .orderBy(F.desc("revenue"), F.asc("date")).limit(5))


def explain_text(query, mode="extended"):
    stream = io.StringIO()
    with redirect_stdout(stream):
        query.explain(mode=mode)
    return stream.getvalue()


def scan_metrics(query):
    """Читаємо сирі SQLMetric з виконаного плану, ті самі лічильники, що в UI.

    AQE ховає Scan усередині QueryStage, тому звичайного children() недостатньо.
    Приватний JVM API перевіряється саме на версії Spark із завдання (4.2.0).
    """
    scans, seen = [], set()

    def visit(node):
        identity = (int(node.id()), node.nodeName())
        if identity in seen:
            return
        seen.add(identity)
        metrics = {}
        entries = node.metrics().iterator()
        while entries.hasNext():
            entry = entries.next()
            metric = entry._2()
            name = metric.name()
            metrics[str(entry._1())] = {
                "name": str(name.get()) if name.isDefined() else str(entry._1()),
                "value": int(metric.value()), "accumulator_id": int(metric.id()),
            }
        if all(key in metrics for key in SCAN_KEYS):
            scans.append({
                "node_id": int(node.id()), "node_name": node.nodeName(),
                "number_of_files_read": metrics["numFiles"]["value"],
                "size_of_files_read_bytes": metrics["filesSize"]["value"],
                "number_of_output_rows": metrics["numOutputRows"]["value"],
                "all_metrics": metrics,
            })
        class_name = node.getClass().getSimpleName()
        if class_name == "AdaptiveSparkPlanExec":
            visit(node.executedPlan())
        elif class_name.endswith("QueryStageExec"):
            visit(node.plan())
        children = node.children().iterator()
        while children.hasNext():
            visit(children.next())

    visit(query._jdf.queryExecution().executedPlan())
    if len(scans) != 1:
        raise RuntimeError(f"Очікувався один файловий Scan, отримано {len(scans)}")
    return scans[0]


def sql_execution_ids(spark):
    executions = spark._jsparkSession.sharedState().statusStore().executionsList().iterator()
    result = set()
    while executions.hasNext():
        result.add(int(executions.next().executionId()))
    return result


def disk_size(path):
    files = [path] if path.is_file() else sorted(item for item in path.rglob("*") if item.is_file())
    data_files = files if path.is_file() else [item for item in files if item.suffix == ".parquet"]
    return {
        "path": str(path), "total_bytes": sum(item.stat().st_size for item in files),
        "data_file_bytes": sum(item.stat().st_size for item in data_files),
        "data_files": len(data_files), "all_files": len(files),
    }


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", default="/work2/data/transactions.csv")
    parser.add_argument("--data-dir", default="/work2/data")
    parser.add_argument("--category", default="books")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output-dir", default="/work2/evidence/part2")
    parser.add_argument("--app-name", default="Lab5_Spark_2_Part2_CSV_Parquet")
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("Потрібно щонайменше три вимірювані повтори")
    return args


def main():
    args = parse_args()
    input_path = Path(args.input).resolve()
    data_dir = Path(args.data_dir).resolve()
    paths = {"csv": input_path, "parquet": data_dir / "tx.parquet",
             "parquet_by_category": data_dir / "tx_by_category.parquet"}
    # overwrite дозволено лише для двох похідних наборів; CSV не перезаписуємо.
    for target in (paths["parquet"], paths["parquet_by_category"]):
        if target == input_path or target in input_path.parents:
            raise ValueError("Каталог Parquet не може містити вхідний CSV")
    output_dir = Path(args.output_dir).resolve()
    for target in (paths["parquet"], paths["parquet_by_category"]):
        if output_dir == target or target in output_dir.parents:
            raise ValueError("Докази не можна зберігати всередині каталогів Parquet")
    output_dir.mkdir(parents=True, exist_ok=True)
    plans_dir = output_dir / "plans"
    plans_dir.mkdir(exist_ok=True)
    digest = hashlib.sha256()
    with input_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    metadata_path = input_path.with_suffix(".metadata.json")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else None
    if metadata and (metadata["transactions"]["sha256"] != digest.hexdigest()
                     or metadata["transactions"]["bytes"] != input_path.stat().st_size):
        raise ValueError("CSV не відповідає метаданим генератора")
    if metadata and args.category not in metadata["categories"]:
        raise ValueError(f"Категорії {args.category!r} немає в метаданих генератора")

    spark = SparkSession.builder.appName(args.app_name).getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    summary = {
        "application": sc.appName, "application_id": sc.applicationId,
        "spark_version": spark.version, "master": sc.master,
        "input": str(input_path), "input_sha256": digest.hexdigest(),
        "category": args.category, "generator_metadata": metadata,
        "warmups_per_source": 1, "repeats_per_source": args.repeats, "runs": [],
        "timing_scope": "query.collect() only; excludes CSV-to-Parquet conversion, DataFrame creation and explain",
        "cache_policy": "no Spark cache/persist; one warmup; OS file cache is not cleared",
        "size_notes": "disk total_bytes includes auxiliary files; Scan filesSize is the full size of selected files, not physical disk IO or selected column bytes",
    }
    try:
        if sc.master != "local[4]":
            raise ValueError(f"За умовою потрібен local[4], отримано {sc.master}")
        spark.conf.set("spark.sql.session.timeZone", "UTC")
        settings = (
            "spark.sql.session.timeZone", "spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled",
            "spark.sql.sources.useV1SourceList", "spark.sql.parquet.compression.codec",
            "spark.sql.parquet.filterPushdown", "spark.sql.parquet.enableVectorizedReader",
            "spark.sql.parquet.recordLevelFilter.enabled", "spark.sql.csv.filterPushdown.enabled",
        )
        summary["settings"] = {key: spark.conf.get(key) for key in settings}
        print(f"Application: {sc.appName}; ID: {sc.applicationId}", flush=True)
        print(f"Spark: {spark.version}; master: {sc.master}; input={input_path}", flush=True)
        print(f"CSV bytes={input_path.stat().st_size}; SHA256={digest.hexdigest()}; category={args.category}", flush=True)
        print("Explicit CSV schema: " + SCHEMA.simpleString(), flush=True)
        print("Settings: " + json.dumps(summary["settings"]), flush=True)
        print("Preparing Parquet datasets (excluded from query timings)...", flush=True)
        sc.setJobGroup("part2_prepare_plain", f"PREPARE plain Parquet | input={input_path}")
        df = read_csv(spark, input_path)
        df.write.mode("overwrite").parquet(str(paths["parquet"]))
        sc.setJobGroup("part2_prepare_partitioned", f"PREPARE partitionBy(category) | input={input_path}")
        df.write.mode("overwrite").partitionBy("category").parquet(str(paths["parquet_by_category"]))
        summary["disk"] = {name: disk_size(path) for name, path in paths.items()}
        for name in SOURCES:
            info = summary["disk"][name]
            print(f"{name}: path={info['path']}; total_bytes={info['total_bytes']}; "
                  f"data_file_bytes={info['data_file_bytes']}; data_files={info['data_files']}", flush=True)
        write_json(output_dir / "results.json", summary)

        expected = None
        for run in range(args.repeats + 1):
            label = "warmup" if run == 0 else f"repeat_{run}"
            # Змінюємо порядок джерел, кожного разу будуємо новий DataFrame/запит.
            offset = run % len(SOURCES)
            for source in SOURCES[offset:] + SOURCES[:offset]:
                group = f"part2_{source}_{label}"
                source_path = paths[source]
                sc.setJobGroup(group, f"{source} | {label} | category={args.category} | input={source_path}")
                frame = read_csv(spark, source_path) if source == "csv" else spark.read.parquet(str(source_path))
                query = build_query(frame, args.category)
                before = explain_text(query)
                (plans_dir / f"{source}_{label}_before.txt").write_text(before, encoding="utf-8")
                previous_sql_ids = sql_execution_ids(spark)
                started = perf_counter()
                rows = query.collect()  # Єдина action у вимірюваному запиті.
                elapsed = perf_counter() - started
                result = [{"date": row["date"].isoformat(), "revenue": str(row["revenue"])} for row in rows]
                if not result:
                    raise ValueError(f"Немає покупок у категорії {args.category!r}")
                if expected is None:
                    expected = result
                elif result != expected:
                    raise RuntimeError("Результати трьох джерел або повторів відрізняються")
                scan = scan_metrics(query)
                execution_ids = sorted(sql_execution_ids(spark) - previous_sql_ids)
                if len(execution_ids) != 1:
                    raise RuntimeError(f"Очікувався один SQL execution для collect(), отримано {execution_ids}")
                after = explain_text(query)
                plan_file = plans_dir / f"{source}_{label}_after.txt"
                plan_file.write_text(after, encoding="utf-8")
                record = {
                    "source": source, "input": str(source_path), "label": label,
                    "seconds": elapsed, "sql_execution_id": execution_ids[0],
                    "job_ids": sorted(int(job) for job in sc.statusTracker().getJobIdsForGroup(group)),
                    "scan": scan, "result": result, "plan_file": str(plan_file),
                }
                summary["runs"].append(record)
                print(f"\n{source} | {label}; seconds={elapsed:.6f}; SQL execution={execution_ids[0]}; "
                      f"jobs={record['job_ids']}; input={source_path}", flush=True)
                print(f"  Scan: files={scan['number_of_files_read']}; size_bytes={scan['size_of_files_read_bytes']}; "
                      f"output_rows={scan['number_of_output_rows']}", flush=True)
                for row in result:
                    print(f"  {row['date']} | {row['revenue']}", flush=True)
                print("  Results equality: OK", flush=True)
                if run == 1:
                    (output_dir / f"{source}_explain.txt").write_text(after, encoding="utf-8")
                    (output_dir / f"{source}_explain_formatted.txt").write_text(
                        explain_text(query, "formatted"), encoding="utf-8")
                    print(f"\n{source} explain (after execution):\n{after}", flush=True)
                write_json(output_dir / "results.json", summary)

        summary["result"] = expected
        summary["results_equal"] = True
        summary["comparison"] = []
        for source in SOURCES:
            measured = [run for run in summary["runs"] if run["source"] == source and run["label"] != "warmup"]
            times = [run["seconds"] for run in measured]
            first = measured[0]
            comparison = {
                "source": source, "disk_bytes": summary["disk"][source]["total_bytes"],
                "data_file_bytes": summary["disk"][source]["data_file_bytes"],
                "seconds": times, "median_seconds": median(times),
                "scan_number_of_files_read": first["scan"]["number_of_files_read"],
                "scan_size_of_files_read_bytes": first["scan"]["size_of_files_read_bytes"],
                "scan_number_of_output_rows": first["scan"]["number_of_output_rows"],
                "scan_reference_label": "repeat_1", "sql_execution_id": first["sql_execution_id"],
            }
            summary["comparison"].append(comparison)
            print(f"\n{source} measured seconds: {', '.join(f'{value:.6f}' for value in times)}; "
                  f"median={median(times):.6f}", flush=True)
            print(f"  Screenshots: {source} repeat_1 | SQL execution={first['sql_execution_id']} | "
                  f"input={paths[source]}", flush=True)
        with (output_dir / "comparison.csv").open("w", encoding="utf-8-sig", newline="") as stream:
            fieldnames = list(summary["comparison"][0])
            writer = csv.DictWriter(stream, fieldnames=fieldnames)
            writer.writeheader()
            for item in summary["comparison"]:
                writer.writerow({**item, "seconds": "; ".join(f"{value:.6f}" for value in item["seconds"])})
        write_json(output_dir / "results.json", summary)
        print("\nAll three sources and all repeats: identical results", flush=True)
        print("Scan size of files read = full selected file sizes; it is NOT physical disk IO.", flush=True)
        print(f"Results: {output_dir}", flush=True)
        print("History Server: http://localhost:18080", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
