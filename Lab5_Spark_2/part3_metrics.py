"""Метрики stage-ів і задач частини 3; запити до REST API Spark UI після таймера."""

import json
from statistics import median
from time import sleep
from urllib.parse import quote
from urllib.request import ProxyHandler, build_opener


def read_api(sc, suffix):
    url = f"{sc.uiWebUrl}/api/v1/applications/{quote(sc.applicationId, safe='')}/{suffix}"
    opener = build_opener(ProxyHandler({}))
    for attempt in range(10):
        try:
            with opener.open(url, timeout=3) as response:
                return json.load(response)
        except (OSError, ValueError):
            if attempt == 9:
                raise
            sleep(0.2)


def describe_stages(sc, group):
    tracker = sc.statusTracker()
    job_ids = sorted(int(value) for value in tracker.getJobIdsForGroup(group))
    stage_ids = set()
    for job_id in job_ids:
        info = tracker.getJobInfo(job_id)
        if info is None:
            raise RuntimeError(f"Не знайдено job {job_id}")
        stage_ids.update(int(value) for value in info.stageIds)
    if not stage_ids:
        raise RuntimeError(f"Не знайдено stage-ів для {group}")
    stages, skipped = [], []
    for stage_id in sorted(stage_ids):
        for attempt in range(10):
            data = read_api(sc, f"stages/{stage_id}?details=false")
            completed = [item for item in data if item["status"] == "COMPLETE"]
            if completed:
                break
            if data and all(item["status"] == "SKIPPED" for item in data):
                # AQE може повторно використати вже матеріалізований shuffle:
                # його пропущений stage не виконує нових задач і не пише дані.
                skipped.append(stage_id)
                break
            if attempt == 9:
                raise RuntimeError(f"UI ще не має завершеного stage {stage_id}: {[item['status'] for item in data]}")
            sleep(0.2)
        if not completed:
            continue
        stage = max(completed, key=lambda item: item["attemptId"])
        stages.append({
            "stage_id": stage_id, "attempt_id": stage["attemptId"],
            "tasks": stage["numTasks"], "name": stage["name"],
            "shuffle_read_bytes": stage["shuffleReadBytes"],
            "shuffle_read_records": stage["shuffleReadRecords"],
            "shuffle_write_bytes": stage["shuffleWriteBytes"],
            "shuffle_write_records": stage["shuffleWriteRecords"],
            "input_bytes": stage["inputBytes"], "input_records": stage["inputRecords"],
            "memory_bytes_spilled": stage["memoryBytesSpilled"],
            "disk_bytes_spilled": stage["diskBytesSpilled"],
        })
    return {"job_ids": job_ids, "stages": stages, "skipped_stage_ids": skipped,
            "total_shuffle_write_bytes": sum(stage["shuffle_write_bytes"] for stage in stages)}


def describe_join_stage(sc, stages):
    # За умовою: із двох stage-ів на 8 задач обираємо більший за Shuffle Read.
    candidates = [stage for stage in stages if stage["tasks"] == 8 and stage["shuffle_read_bytes"] > 0]
    if len(candidates) != 2:
        raise RuntimeError(f"Очікувалися два stage-и на 8 задач зі Shuffle Read, отримано {candidates}")
    selected = max(candidates, key=lambda stage: stage["shuffle_read_bytes"])
    details = read_api(sc, f"stages/{selected['stage_id']}/{selected['attempt_id']}?details=true&taskStatus=SUCCESS")
    by_index = {}
    for task in details["tasks"].values():
        if task["status"] != "SUCCESS":
            continue
        index = task["index"]
        if index not in by_index or task["attempt"] > by_index[index]["attempt"]:
            by_index[index] = task
    if sorted(by_index) != list(range(8)):
        raise RuntimeError(f"Немає метрик усіх восьми задач: {sorted(by_index)}")
    tasks = []
    for index, task in sorted(by_index.items()):
        metrics = task["taskMetrics"]
        read = metrics["shuffleReadMetrics"]
        tasks.append({
            "index": index, "task_id": task["taskId"], "attempt": task["attempt"],
            "duration_ms": task["duration"],
            "shuffle_read_records": read["recordsRead"],
            "shuffle_read_bytes": read["localBytesRead"] + read["remoteBytesRead"],
            "shuffle_write_bytes": metrics["shuffleWriteMetrics"]["bytesWritten"],
            "memory_bytes_spilled": metrics["memoryBytesSpilled"],
            "disk_bytes_spilled": metrics["diskBytesSpilled"],
        })
    records = [task["shuffle_read_records"] for task in tasks]
    if sum(records) != selected["shuffle_read_records"]:
        raise RuntimeError("Сума Shuffle Read Records задач не збігається з метрикою stage-у")
    return {
        **selected, "task_metrics": tasks,
        "max_records": max(records), "median_records": median(records),
        "max_over_median": max(records) / median(records) if median(records) else None,
        "largest_task_read_bytes": max(task["shuffle_read_bytes"] for task in tasks),
        "source_shuffle_write_bytes": sum(stage["shuffle_write_bytes"] for stage in stages
                                          if stage["shuffle_read_bytes"] == 0),
    }
