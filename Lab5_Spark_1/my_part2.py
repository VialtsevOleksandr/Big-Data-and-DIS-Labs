"""Частина 2: ліниві обчислення, помилка розбору та повторні дії з cache()."""

import argparse
import csv
import json
from operator import add
from pathlib import Path
from statistics import median
from time import perf_counter, sleep
import traceback
from urllib.parse import urlsplit
from urllib.request import ProxyHandler, build_opener

from pyspark import SparkConf, SparkContext

from my_part1 import build_pipeline, format_cents, is_transaction, parse_line


def parse_broken_line(line):
    """Навмисна помилка: на число перетворюється category замість amount."""
    _user_id, timestamp, category, _amount = next(csv.reader([line]))
    cents = float(category)  # Навмисна помилка виникне на worker під час дії.
    return (timestamp.split("T", 1)[0], category), cents


def print_times(label, values):
    print(f"{label}: seconds=" + ", ".join(f"{v:.6f}" for v in values), flush=True)
    print(f"{label}: median_seconds={median(values):.6f}", flush=True)


def report_jobs(sc, group):
    """Метрики завершених stage-ів через REST Spark UI, поза заміром часу."""
    jobs = sorted(sc.statusTracker().getJobIdsForGroup(group))
    print(f"{group}: jobs={jobs}", flush=True)
    if not sc.uiWebUrl:
        print("Spark UI вимкнено: Input потрібно перевірити в History Server.", flush=True)
        return
    port = urlsplit(sc.uiWebUrl).port
    base = f"http://127.0.0.1:{port}/api/v1/applications/{sc.applicationId}"
    opener = build_opener(ProxyHandler({}))
    for job_id in jobs:
        info = sc.statusTracker().getJobInfo(job_id)
        if info is None:
            print(f"job={job_id}: метадані недоступні; перевірити UI", flush=True)
            continue
        total_bytes = total_records = 0
        try:
            for stage_id in info.stageIds:
                with opener.open(f"{base}/stages/{stage_id}", timeout=5) as response:
                    attempts = json.load(response)
                stage = max(attempts, key=lambda item: item["attemptId"])
                size, records = stage["inputBytes"], stage["inputRecords"]
                total_bytes += size
                total_records += records
                print(
                    f"  job={job_id}; stage={stage_id}; "
                    f"input_bytes={size}; input_records={records}",
                    flush=True,
                )
            print(
                f"  job={job_id}; total_input_bytes={total_bytes}; "
                f"total_input_records={total_records}",
                flush=True,
            )
        except (OSError, ValueError, KeyError) as exc:
            print(f"job={job_id}: Input не отримано ({exc}); перевірити UI", flush=True)


def run_lazy(sc, args):
    build_times, action_times = [], []
    expected = None
    print("\n=== Побудова ланцюжка та дія ===", flush=True)
    for run in range(args.repeats + 1):
        label = "warmup" if run == 0 else f"repeat_{run}"
        group = f"part2_lazy_{label}"
        sc.setJobGroup(group, f"Побудова та дія | {label} | input={args.input}")

        started = perf_counter()
        source, leader = build_pipeline(sc, args.input)
        build_seconds = perf_counter() - started

        print(
            f"{label}: build_seconds={build_seconds:.6f}; "
            f"input_partitions={source.getNumPartitions()}; "
            f"jobs_before_action={sc.statusTracker().getJobIdsForGroup(group)}",
            flush=True,
        )
        started = perf_counter()
        result = leader.collect()
        action_seconds = perf_counter() - started
        if not result:
            raise ValueError("CSV не містить транзакцій")
        if expected is not None and result[0] != expected:
            raise RuntimeError("Результати повторів відрізняються")
        expected = result[0]
        category, cents, daily_count = expected
        print(
            f"{label}: action_seconds={action_seconds:.6f}; "
            f"leader={category}; amount={format_cents(cents)}; task1_records={daily_count}",
            flush=True,
        )
        report_jobs(sc, group)
        if run > 0:
            build_times.append(build_seconds)
            action_times.append(action_seconds)
    print_times("build", build_times)
    print_times("action", action_times)


def run_error(sc, args):
    print("\n=== Навмисна помилка розбору ===", flush=True)
    group = "part2_expected_parse_error"
    sc.setJobGroup(group, f"Навмисна помилка | input={args.input}")
    _source, leader = build_pipeline(sc, args.input, parser=parse_broken_line)
    print("Ланцюжок із помилковим parser побудовано без винятку.", flush=True)
    print(f"jobs_before_action={sc.statusTracker().getJobIdsForGroup(group)}", flush=True)
    print("Зараз викликається leader.collect():", flush=True)
    try:
        leader.collect()  # На driver виняток буде отримано саме на цій дії.
    except Exception as exc:
        # Інші проблеми середовища або вхідного файлу не видаємо за очікувану помилку.
        if "could not convert string to float" not in str(exc):
            raise
        print("EXPECTED_ERROR: перетворення category на число під час дії.", flush=True)
        traceback.print_exc()
        print(f"Failed jobs: {sc.statusTracker().getJobIdsForGroup(group)}", flush=True)
    else:
        raise RuntimeError("Навмисна помилка не виникла: перевірте вхідні категорії")


def build_pairs(sc, input_path):
    return sc.textFile(input_path).filter(is_transaction).map(parse_line)


def run_action_pair(sc, pairs, label, input_path):
    count_group = f"part2_{label}_count"
    aggregate_group = f"part2_{label}_aggregate"
    sc.setJobGroup(count_group, f"{label}: count | input={input_path}")
    started = perf_counter()
    rows = pairs.count()
    sc.setJobGroup(aggregate_group, f"{label}: reduceByKey + count | input={input_path}")
    # Нова агрегація для кожної пари: старі shuffle-файли не використовуються.
    daily_count = pairs.reduceByKey(add).count()
    elapsed = perf_counter() - started
    print(
        f"{label}: pair_seconds={elapsed:.6f}; rows={rows}; task1_records={daily_count}",
        flush=True,
    )
    report_jobs(sc, count_group)
    report_jobs(sc, aggregate_group)
    return elapsed, (rows, daily_count)


def report_storage(sc, pairs):
    for info in sc._jsc.sc().getRDDStorageInfo():
        if info.id() == pairs.id():
            print(
                f"Storage: rdd_id={info.id()}; name={info.name()}; "
                f"cached_partitions={info.numCachedPartitions()}/{info.numPartitions()}; "
                f"memory_bytes={info.memSize()}; disk_bytes={info.diskSize()}; "
                f"storage_level={info.storageLevel().description()}",
                flush=True,
            )
            return
    print(f"Storage: rdd_id={pairs.id()}; кешованих блоків немає", flush=True)


def run_cache(sc, args):
    print("\n=== Дві дії без кешу ===", flush=True)
    no_cache_times, first_cache_times, next_cache_times = [], [], []
    expected = None
    for run in range(args.repeats + 1):
        suffix = "warmup" if run == 0 else f"repeat_{run}"
        pairs = build_pairs(sc, args.input).setName(f"no_cache_{suffix}")
        elapsed, counts = run_action_pair(sc, pairs, f"no_cache_{suffix}", args.input)
        if expected is not None and counts != expected:
            raise RuntimeError("Кількість записів змінилася між повторами")
        expected = counts
        if run > 0:
            no_cache_times.append(elapsed)
    print_times("no_cache", no_cache_times)

    print("\n=== Перша пара дій після cache() ===", flush=True)
    cached_pairs = None
    try:
        for run in range(args.repeats + 1):
            if cached_pairs is not None:
                cached_pairs.unpersist(blocking=True)
            suffix = "warmup" if run == 0 else f"repeat_{run}"
            cached_pairs = build_pairs(sc, args.input).setName(f"cached_pairs_{suffix}")
            cached_pairs.cache()  # Позначаємо RDD; дані ще не обчислено.
            elapsed, counts = run_action_pair(
                sc, cached_pairs, f"cache_first_{suffix}", args.input
            )
            if counts != expected:
                raise RuntimeError("Результат із кешем відрізняється від результату без кешу")
            report_storage(sc, cached_pairs)
            if run > 0:
                first_cache_times.append(elapsed)
        print_times("cache_first", first_cache_times)

        print("\n=== Наступні пари на заповненому кеші ===", flush=True)
        for run in range(args.repeats + 1):
            suffix = "warmup" if run == 0 else f"repeat_{run}"
            elapsed, counts = run_action_pair(
                sc, cached_pairs, f"cache_next_{suffix}", args.input
            )
            if counts != expected:
                raise RuntimeError("Результат повторного читання кешу змінився")
            if run > 0:
                next_cache_times.append(elapsed)
        print_times("cache_next", next_cache_times)
        report_storage(sc, cached_pairs)
        if args.pause:
            continue_file = Path(args.continue_file)
            continue_file.parent.mkdir(parents=True, exist_ok=True)
            continue_file.unlink(missing_ok=True)
            print(
                f"\nSpark UI через Docker: http://localhost:4040; "
                f"Storage RDD ID: {cached_pairs.id()}",
                flush=True,
            )
            print(f"PAUSE_FOR_SCREENSHOTS: створи файл {continue_file} для продовження.", flush=True)
            while not continue_file.exists():
                sleep(0.25)
            continue_file.unlink()
    finally:
        if cached_pairs is not None:
            cached_pairs.unpersist(blocking=True)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", default="/data/transactions.csv")
    parser.add_argument("--mode", choices=("all", "lazy", "error", "cache"), default="all")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--app-name", default="Lab5_Part2_RDD")
    parser.add_argument("--pause", action="store_true", help="Пауза для скриншота Storage")
    parser.add_argument("--continue-file", default="evidence/part2.continue", help="Файл-сигнал завершення паузи")
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("Потрібно щонайменше 3 вимірювані повтори")
    return args


def main():
    args = parse_args()
    sc = SparkContext(conf=SparkConf().setAppName(args.app_name))
    sc.setLogLevel("WARN")
    try:
        # Допоміжні функції частини 1 повинні бути доступні й Python worker-ам.
        sc.addPyFile(str(Path(__file__).resolve().with_name("my_part1.py")))
        print(f"Application: {sc.appName}; ID: {sc.applicationId}", flush=True)
        print(f"Spark: {sc.version}; master: {sc.master}; input={args.input}", flush=True)
        print(
            "Input враховує і читання кешованих блоків; "
            "це не лічильник фізичних звернень до диска.",
            flush=True,
        )
        if args.mode in ("all", "lazy"):
            run_lazy(sc, args)
        if args.mode in ("all", "error"):
            run_error(sc, args)
        if args.mode in ("all", "cache"):
            run_cache(sc, args)
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
