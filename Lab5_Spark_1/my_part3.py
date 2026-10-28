"""Частина 3: та сама дія RDD; master і розмір шматка задає spark-submit."""

import argparse
import json
import os
from pathlib import Path
from statistics import median
from time import perf_counter, sleep

from pyspark import SparkConf, SparkContext

from my_part1 import build_pipeline, format_cents


def describe_job(sc, group):
    """Читаємо метадані після дії: цей код не входить у замір часу."""
    tracker = sc.statusTracker()
    job_ids = sorted(tracker.getJobIdsForGroup(group))
    if len(job_ids) != 1:
        raise RuntimeError(f"Очікувався один job для {group}, отримано {job_ids}")
    job = tracker.getJobInfo(job_ids[0])
    if job is None:
        raise RuntimeError(f"Немає метаданих job {job_ids[0]}")
    stages = []
    for stage_id in sorted(job.stageIds):
        stage = tracker.getStageInfo(stage_id)
        if stage is None:
            raise RuntimeError(f"Немає метаданих stage {stage_id}")
        stages.append({"stage_id": int(stage_id), "tasks": int(stage.numTasks)})
    return {
        "job_id": int(job_ids[0]),
        "stages": stages,
        "tasks": sum(stage["tasks"] for stage in stages),
        "reading_stage_id": stages[0]["stage_id"],
        "reading_tasks": stages[0]["tasks"],
    }


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", nargs="?", default="/data/transactions.csv")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--app-name", default="Lab5_Part3_RDD")
    parser.add_argument("--output", help="Шлях до JSON із результатами")
    parser.add_argument("--pause", action="store_true", help="Пауза для Timeline та Executors")
    args = parser.parse_args()
    if args.repeats < 3:
        parser.error("Потрібно щонайменше 3 вимірювані повтори")
    return args


def main():
    args = parse_args()
    sc = SparkContext(conf=SparkConf().setAppName(args.app_name))
    sc.setLogLevel("WARN")
    try:
        sc.addPyFile(str(Path(__file__).resolve().with_name("my_part1.py")))
        # Фактичні параметри spark-submit доступні після створення SparkContext.
        master = sc.master
        block_bytes = int(sc._jsc.hadoopConfiguration().getLong("fs.local.block.size", 0))
        if master not in ("local[1]", "local[2]", "local[4]"):
            raise ValueError("Для частини 3 потрібен local[1], local[2] або local[4]")
        if block_bytes not in (33554432, 134217728):
            raise ValueError("Для частини 3 потрібен шматок 32 або 128 МіБ")
        cores = int(master[6:-1])
        config_id = f"local{cores}_{block_bytes // 1048576}MiB"
        summary = {
            "application": sc.appName,
            "application_id": sc.applicationId,
            "spark_version": sc.version,
            "master": sc.master,
            "block_bytes": block_bytes,
            "input": args.input,
            "input_bytes": os.path.getsize(args.input),
            "container_cpu_count": os.cpu_count(),
            "jvm_available_processors": sc._jvm.java.lang.Runtime.getRuntime().availableProcessors(),
            "runs": [],
        }
        print(f"Application: {sc.appName}; ID: {sc.applicationId}", flush=True)
        print(f"Spark: {sc.version}; master: {sc.master}; block_bytes={block_bytes}", flush=True)
        print(f"Input: {args.input}; bytes={summary['input_bytes']}", flush=True)
        print(
            f"Container CPUs: {summary['container_cpu_count']}; "
            f"JVM available processors: {summary['jvm_available_processors']}",
            flush=True,
        )
        expected = None
        for run in range(args.repeats + 1):
            label = "warmup" if run == 0 else f"repeat_{run}"
            group = f"part3_{config_id}_{label}"
            sc.setJobGroup(group, f"{config_id} | {label} | input={args.input}")
            # Кожен раз новий textFile і нові shuffle RDD; кешування немає.
            source, leader = build_pipeline(sc, args.input)
            partitions = source.getNumPartitions()
            started = perf_counter()
            result = leader.collect()  # Та сама єдина дія, що й у частині 1.
            elapsed = perf_counter() - started
            if not result:
                raise ValueError("CSV не містить транзакцій")
            if expected is not None and result[0] != expected:
                raise RuntimeError("Результати повторів відрізняються")
            expected = result[0]
            category, cents, daily_count = expected
            job = describe_job(sc, group)
            summary["runs"].append({
                "label": label,
                "seconds": elapsed,
                "input_partitions": partitions,
                **job,
            })
            print(
                f"{label}: partitions={partitions}; job={job['job_id']}; "
                f"tasks={job['tasks']}; action_seconds={elapsed:.6f}",
                flush=True,
            )
            for stage in job["stages"]:
                print(f"  stage={stage['stage_id']}; tasks={stage['tasks']}", flush=True)
            print(
                f"  reading_stage={job['reading_stage_id']}; reading_tasks={job['reading_tasks']}; "
                f"leader={category}; amount={format_cents(cents)}; task1_records={daily_count}",
                flush=True,
            )

        times = [run["seconds"] for run in summary["runs"][1:]]
        summary["median_seconds"] = median(times)
        summary["leader"] = {"category": expected[0], "amount_cents": expected[1]}
        summary["task1_records"] = expected[2]
        print("Measured seconds: " + ", ".join(f"{value:.6f}" for value in times), flush=True)
        print(f"Median seconds: {summary['median_seconds']:.6f}", flush=True)
        output = Path(args.output or f"evidence/part3/{config_id}.json")
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"Results JSON: {output}", flush=True)
        example = summary["runs"][1]
        print(
            f"Timeline: repeat_1, job={example['job_id']}, "
            f"first stage={example['reading_stage_id']}, reading_tasks={example['reading_tasks']}",
            flush=True,
        )
        if args.pause:
            continue_file = output.with_suffix(".continue")
            continue_file.unlink(missing_ok=True)
            print("Spark UI: http://localhost:4040", flush=True)
            print(f"PAUSE_FOR_SCREENSHOTS: {continue_file}", flush=True)
            # spark-submit не передає stdin дочірньому Python-процесу.
            # PowerShell прочитає Enter і створить файл-сигнал у спільній папці.
            while not continue_file.exists():
                sleep(0.25)
            continue_file.unlink()
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
