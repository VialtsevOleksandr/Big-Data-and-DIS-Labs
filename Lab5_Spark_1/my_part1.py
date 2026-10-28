"""Частина 1: один ланцюжок RDD, одна дія, прогрів і повтори."""

import argparse
import csv
from decimal import Decimal
from operator import add
from statistics import median
from time import perf_counter

from pyspark import SparkConf, SparkContext


HEADER = "user_id,timestamp,category,amount"


def is_transaction(line):
    return bool(line.strip()) and line.lstrip("\ufeff").strip() != HEADER


def parse_line(line):
    """CSV → ((дата, категорія), сума в копійках)."""
    _user_id, timestamp, category, amount = next(csv.reader([line]))
    cents = Decimal(amount) * 100
    if not cents.is_finite() or cents != cents.to_integral_value():
        raise ValueError(f"Некоректна грошова сума: {amount!r}")
    return (timestamp.split("T", 1)[0], category), int(cents)


def merge_category_totals(left, right):
    # (сума, кількість денних підсумків) однієї категорії.
    return left[0] + right[0], left[1] + right[1]


def merge_leaders(left, right):
    # Кандидат: (категорія, її сума, кількість денних підсумків).
    # За рівних сум перемагає перша категорія за алфавітом, як у ДЗ 4.
    winner = min((left, right), key=lambda item: (-item[1], item[0]))
    return winner[0], winner[1], left[2] + right[2]


def format_cents(cents):
    sign = "-" if cents < 0 else ""
    whole, fraction = divmod(abs(cents), 100)
    return f"{sign}{whole}.{fraction:02d}"


def build_pipeline(sc, input_path, parser=parse_line):
    """Тільки трансформації"""
    source = sc.textFile(input_path).setName("01_csv_textFile")
    pairs = source.filter(is_transaction).map(parser).setName("02_date_category_pairs")

    # Завдання 1: ((дата, категорія), денна сума).
    daily_totals = pairs.reduceByKey(add).setName("03_task1_daily_totals")

    # Завдання 2 отримує дані виключно з результату Завдання 1.
    category_pairs = daily_totals.map(
        lambda item: (item[0][1], (item[1], 1))
    ).setName("04_category_pairs_with_daily_count")
    category_totals = category_pairs.reduceByKey(
        merge_category_totals
    ).setName("05_category_totals")

    # Ще одна трансформація обирає глобального лідера всередині Spark.
    # На driver потрапить один запис, а не всі денні підсумки.
    candidates = category_totals.map(
        lambda item: ("leader", (item[0], item[1][0], item[1][1]))
    ).setName("06_leader_candidates")
    leader = candidates.reduceByKey(
        merge_leaders, numPartitions=1
    ).values().setName("07_task2_leader_and_daily_count")
    return source, leader


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", default="/data/transactions.csv")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--app-name", default="Lab5_Part1_RDD")
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("Потрібно щонайменше 3 вимірювані повтори")
    return args


def main():
    args = parse_args()
    sc = SparkContext(conf=SparkConf().setAppName(args.app_name))
    sc.setLogLevel("WARN")
    try:
        print(f"Application: {sc.appName}", flush=True)
        print(f"Application ID: {sc.applicationId}", flush=True)
        print(f"Spark: {sc.version}; master: {sc.master}", flush=True)
        print(f"Input: {args.input}", flush=True)
        block_size = sc._jsc.hadoopConfiguration().get("fs.local.block.size")
        print(f"fs.local.block.size: {block_size} bytes", flush=True)
        timings = []
        expected = None

        for run in range(args.repeats + 1):
            label = "warmup" if run == 0 else f"repeat_{run}"
            group = f"part1_{label}"
            sc.setJobGroup(group, f"{label} | input={args.input}")

            # Кожен повтор починається з нового textFile та нових shuffle RDD.
            source, leader = build_pipeline(sc, args.input)
            print(f"\n{label}: input_partitions={source.getNumPartitions()}", flush=True)
            if run == 0:
                print("\nTask 2 lineage:", flush=True)
                print(leader.toDebugString().decode("utf-8"), flush=True)

            started = perf_counter()
            result = leader.collect()  # Єдина дія в кожному запуску ланцюжка.
            elapsed = perf_counter() - started

            if not result:
                raise ValueError("CSV не містить транзакцій")
            category, cents, daily_count = result[0]
            if expected is None:
                expected = result[0]
            elif result[0] != expected:
                raise RuntimeError("Результати повторів відрізняються")

            jobs = sorted(sc.statusTracker().getJobIdsForGroup(group))
            print(f"{label}: action_seconds={elapsed:.6f}; jobs={jobs}", flush=True)
            print(
                f"Leader: {category}; amount={format_cents(cents)}; "
                f"task1_records={daily_count}",
                flush=True,
            )
            if run > 0:
                timings.append(elapsed)

        print("\nMeasured seconds: " + ", ".join(f"{t:.6f}" for t in timings), flush=True)
        print(f"Median seconds: {median(timings):.6f}", flush=True)
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
