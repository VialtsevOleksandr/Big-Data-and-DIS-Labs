"""RDD: groupByKey проти reduceByKey; прогрів, три повтори й метрики Spark UI."""

import argparse
import csv
import hashlib
import inspect
import json
from decimal import Decimal
from pathlib import Path
from statistics import median
from time import perf_counter, sleep
from urllib.parse import quote
from urllib.request import ProxyHandler, build_opener

from pyspark import SparkConf, SparkContext


HEADER = "user_id,timestamp,category,amount"
VARIANTS = {"A": "groupByKey", "B": "reduceByKey"}


def is_transaction(line):
    return bool(line.strip()) and line.lstrip("\ufeff").strip() != HEADER


def parse_line(line):
    """CSV → (user_id, (кількість=1, сума в цілих копійках))."""
    user_id, _timestamp, _category, amount = next(csv.reader([line]))
    cents = Decimal(amount) * 100
    if not cents.is_finite() or cents != cents.to_integral_value():
        raise ValueError(f"Некоректна грошова сума: {amount!r}")
    return int(user_id), (1, int(cents))


def merge_totals(left, right):
    return left[0] + right[0], left[1] + right[1]


def sum_group(values):
    count = cents = 0
    for value in values:
        count += value[0]
        cents += value[1]
    return count, cents


def format_cents(cents):
    sign = "-" if cents < 0 else ""
    whole, fraction = divmod(abs(cents), 100)
    return f"{sign}{whole}.{fraction:02d}"


def build_pipeline(sc, input_path, variant):
    # Нові RDD для КОЖНОГО запуску, без cache/persist і повторного shuffle.
    source = sc.textFile(input_path).setName("01_transactions_csv")
    pairs = source.filter(is_transaction).map(parse_line).setName("02_user_count_cents")
    partitions = source.getNumPartitions()
    if variant == "A":
        grouped = pairs.groupByKey(numPartitions=partitions)  # SHUFFLE A
        totals = grouped.mapValues(sum_group)
    else:
        totals = pairs.reduceByKey(merge_totals, numPartitions=partitions)  # SHUFFLE B
    return source, totals.setName(f"03_{VARIANTS[variant]}_user_totals")


def describe_jobs(sc, group):
    """Метрики читаються ПІСЛЯ зупинки таймера, додаткових Spark actions немає."""
    tracker = sc.statusTracker()
    job_ids = sorted(int(value) for value in tracker.getJobIdsForGroup(group))
    if not job_ids:
        raise RuntimeError(f"Не знайдено job для {group}")
    stage_ids = set()
    for job_id in job_ids:
        job = tracker.getJobInfo(job_id)
        if job is None:
            raise RuntimeError(f"Не знайдено метадані job {job_id}")
        stage_ids.update(int(value) for value in job.stageIds)
    opener = build_opener(ProxyHandler({}))
    stages = []
    for stage_id in sorted(stage_ids):
        info = tracker.getStageInfo(stage_id)
        if info is None:
            raise RuntimeError(f"Не знайдено метадані stage {stage_id}")
        stage = {"stage_id": stage_id, "tasks": int(info.numTasks)}
        url = (f"{sc.uiWebUrl}/api/v1/applications/{quote(sc.applicationId, safe='')}/"
               f"stages/{stage_id}?details=false")
        error = None
        for _attempt in range(5):
            try:
                with opener.open(url, timeout=2) as response:
                    attempts = json.load(response)
                completed = [value for value in attempts if value["status"] == "COMPLETE"]
                if not completed:
                    raise RuntimeError("UI ще не отримав завершення stage")
                data = max(completed, key=lambda value: value["attemptId"])
                stage.update({
                    "attempt_id": data["attemptId"],
                    "shuffle_write_bytes": data["shuffleWriteBytes"],
                    "shuffle_write_records": data["shuffleWriteRecords"],
                    "shuffle_read_bytes": data["shuffleReadBytes"],
                    "shuffle_read_records": data["shuffleReadRecords"],
                    "memory_bytes_spilled": data["memoryBytesSpilled"],
                    "disk_bytes_spilled": data["diskBytesSpilled"],
                })
                error = None
                break
            except (OSError, ValueError, KeyError, RuntimeError) as exc:
                error = str(exc)
                sleep(0.2)
        if error is not None:
            stage["metrics_error"] = error
            print(f"WARNING: stage {stage_id}: {error}; перепишіть Shuffle Write з UI", flush=True)
        stages.append(stage)
    return {"job_ids": job_ids, "job_count": len(job_ids), "stages": stages,
            "stage_count": len(stages), "tasks": sum(stage["tasks"] for stage in stages)}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", default="/work2/data/transactions.csv")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--app-name", default="Lab5_Spark_2_Part1_GroupByKey_ReduceByKey")
    parser.add_argument("--output", default="/work2/evidence/part1/results.json")
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("Потрібно щонайменше три вимірювані повтори")
    return args


def main():
    args = parse_args()
    input_path = Path(args.input)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.resolve() == input_path.resolve():
        raise ValueError("Результати не можна записувати поверх вхідного CSV")
    digest = hashlib.sha256()
    with input_path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    input_sha256 = digest.hexdigest()
    metadata_path = input_path.with_suffix(".metadata.json")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8")) if metadata_path.exists() else None
    if metadata and (metadata["transactions"]["sha256"] != input_sha256
                     or metadata["transactions"]["bytes"] != input_path.stat().st_size):
        raise ValueError("CSV не відповідає метаданим генератора")
    sc = SparkContext(conf=SparkConf().setAppName(args.app_name))
    sc.setLogLevel("WARN")
    summary = {
        "application": sc.appName, "application_id": sc.applicationId,
        "spark_version": sc.version, "master": sc.master, "input": args.input,
        "input_bytes": input_path.stat().st_size, "input_sha256": input_sha256,
        "generator_metadata": metadata, "runs": [],
        "timing_scope": "collect user totals, including CSV read, parse, shuffle and aggregation",
        "warmups_per_variant": 1, "repeats_per_variant": args.repeats,
    }
    try:
        if sc.master != "local[4]":
            raise ValueError(f"За умовою потрібен local[4], отримано {sc.master}")
        summary["local_block_bytes"] = int(sc._jsc.hadoopConfiguration().getLong("fs.local.block.size", 0))
        summary["default_parallelism"] = sc.defaultParallelism
        print(f"Application: {sc.appName}; ID: {sc.applicationId}", flush=True)
        print(f"Spark: {sc.version}; master: {sc.master}; heap={sc.getConf().get('spark.driver.memory')}", flush=True)
        print(f"Input: {args.input}; bytes={summary['input_bytes']}; SHA256={input_sha256}", flush=True)
        print(f"Local block bytes: {summary['local_block_bytes']}; defaultParallelism={sc.defaultParallelism}", flush=True)
        if metadata:
            print(f"Generator: seed={metadata['seed']}; guest_percent={metadata['guest_percent']}; "
                  f"rows={metadata['transaction_rows']}; registered_users={metadata['registered_users']}", flush=True)
        print("Pairs: (user_id, (1, amount_cents)); sums use integer cents", flush=True)
        sample = []
        with input_path.open(encoding="utf-8") as stream:
            for number, line in enumerate(stream, 1):
                if 100 <= number <= 104:
                    sample.append({"file_line": number, "csv": line.rstrip("\n"), "pair": parse_line(line)})
                if number >= 104:
                    break
        summary["sample_lines_100_104_including_header"] = sample
        print("\nSample: file lines 100–104 (header is line 1):", flush=True)
        for item in sample:
            print(f"  line {item['file_line']}: {item['csv']} -> {item['pair']}", flush=True)
        code_lines, first_line = inspect.getsourcelines(build_pipeline)
        summary["shuffle_code_lines"] = [
            {"line": first_line + index, "code": line.strip()}
            for index, line in enumerate(code_lines) if "# SHUFFLE" in line
        ]
        print("\nShuffle boundaries:", flush=True)
        for item in summary["shuffle_code_lines"]:
            print(f"  {__file__}:{item['line']}: {item['code']}", flush=True)

        expected = None
        for run in range(args.repeats + 1):
            label = "warmup" if run == 0 else f"repeat_{run}"
            # Чергуємо порядок, щоб один варіант не був завжди першим.
            order = ("A", "B") if run % 2 == 0 else ("B", "A")
            for variant in order:
                group = f"part1_{variant}_{label}"
                sc.setJobGroup(group, f"{variant} {VARIANTS[variant]} | {label} | input={args.input}")
                source, totals = build_pipeline(sc, args.input, variant)
                partitions = source.getNumPartitions()
                print(f"\n{variant} {VARIANTS[variant]} | {label}; input_partitions={partitions}", flush=True)
                if variant == "B" and run == 0:
                    lineage = totals.toDebugString().decode("utf-8")
                    summary["lineage_B"] = lineage
                    output.with_name("lineage_B.txt").write_text(lineage + "\n", encoding="utf-8")
                    print("Lineage B:\n" + lineage, flush=True)
                started = perf_counter()
                # Лише агрегати ~9001 користувача надходять на driver.
                # Сортування невеликого результату на driver не додає job/shuffle.
                result = dict(totals.collect())  # Єдина action у вимірі.
                elapsed = perf_counter() - started
                if not result:
                    raise ValueError("CSV не містить транзакцій")
                if expected is None:
                    expected = result
                elif result != expected:
                    raise RuntimeError("Повні результати A/B або повторів відрізняються")
                top10 = sorted(result.items(), key=lambda item: (-item[1][1], item[0]))[:10]
                job = describe_jobs(sc, group)
                record = {"variant": variant, "label": label, "seconds": elapsed,
                          "input_partitions": partitions, **job,
                          "top10": [{"user_id": key, "count": value[0], "amount_cents": value[1]}
                                    for key, value in top10]}
                summary["runs"].append(record)
                print(f"seconds={elapsed:.6f}; jobs={job['job_ids']}; stage_count={job['stage_count']}; tasks={job['tasks']}", flush=True)
                for stage in job["stages"]:
                    print(f"  stage={stage['stage_id']}; tasks={stage['tasks']}; "
                          f"Shuffle Write bytes={stage.get('shuffle_write_bytes', 'CHECK UI')}; "
                          f"Shuffle Write records={stage.get('shuffle_write_records', 'CHECK UI')}", flush=True)
                print("  TOP 10: user_id | count | amount", flush=True)
                for key, (count, cents) in top10:
                    print(f"  {key} | {count} | {format_cents(cents)}", flush=True)
                print("  Full results equality: OK", flush=True)
                # Проміжний файл залишається навіть у разі збою наступного запуску.
                output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

        rows = sum(value[0] for value in expected.values())
        guest_rows = expected.get(0, (0, 0))[0]
        registered_users = len(expected) - int(0 in expected)
        summary["actual_data"] = {"rows": rows, "registered_users": registered_users,
                                  "guest_rows": guest_rows, "guest_percent": 100 * guest_rows / rows}
        if metadata and (rows != metadata["transaction_rows"] or guest_rows != metadata["guest_rows"]
                         or registered_users != metadata["registered_users"]):
            raise RuntimeError("Фактичні агрегати не збігаються з параметрами генератора")
        summary["results_equal"] = True
        print(f"\nActual: rows={rows}; registered_users={registered_users}; "
              f"guest_rows={guest_rows}; guest_percent={100 * guest_rows / rows:.2f}", flush=True)
        summary["timings"] = {}
        for variant in VARIANTS:
            measured = [run for run in summary["runs"] if run["variant"] == variant and run["label"] != "warmup"]
            times = [run["seconds"] for run in measured]
            summary["timings"][variant] = {"seconds": times, "median_seconds": median(times)}
            print(f"{variant} measured seconds: {', '.join(f'{value:.6f}' for value in times)}; "
                  f"median={median(times):.6f}", flush=True)
            example = measured[0]
            print(f"  Screenshots: {variant} repeat_1; jobs={example['job_ids']}; "
                  f"first_stage={example['stages'][0]['stage_id']}", flush=True)
        example_A = next(run for run in summary["runs"] if run["variant"] == "A" and run["label"] == "repeat_1")
        example_B = next(run for run in summary["runs"] if run["variant"] == "B" and run["label"] == "repeat_1")
        write_A = example_A["stages"][0].get("shuffle_write_bytes")
        write_B = example_B["stages"][0].get("shuffle_write_bytes")
        if write_A is not None and write_B:
            summary["shuffle_write_ratio_A_over_B_repeat1"] = write_A / write_B
            print(f"Shuffle Write A/B (repeat_1): {write_A / write_B:.3f}x", flush=True)
        summary["B_logical_partial_pairs_upper_bound"] = len(expected) * example_B["input_partitions"]
        print(f"B logical partial pairs <= users_with_guest * partitions = {len(expected)} * "
              f"{example_B['input_partitions']} = {summary['B_logical_partial_pairs_upper_bound']}", flush=True)
        print("This estimates logical pairs, not PySpark serialized Shuffle Write Records.", flush=True)
        output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"Results JSON: {output}", flush=True)
        print("History Server: http://localhost:18080", flush=True)
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
