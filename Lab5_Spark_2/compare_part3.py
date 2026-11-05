"""Порівняння завершених запусків a/b/c без повторного запуску Spark."""

import argparse
import csv
import json
from pathlib import Path


def latest_completed(root, variant):
    paths = sorted((root / variant).glob("*/results.json"), key=lambda path: path.stat().st_mtime_ns, reverse=True)
    for path in paths:
        data = json.loads(path.read_text(encoding="utf-8"))
        if data.get("status") == "complete" and data.get("variant") == variant:
            return path, data
    raise ValueError(f"Не знайдено завершений запуск {variant}: {root / variant}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence-dir", type=Path, default=Path(__file__).resolve().parent / "evidence" / "part3")
    args = parser.parse_args()
    runs = {variant: latest_completed(args.evidence_dir, variant) for variant in ("a", "b", "c")}
    baseline = runs["a"][1]
    identity = lambda data: (data["inputs"]["parquet_fingerprint"], data["inputs"]["users_sha256"])
    for variant, (path, data) in runs.items():
        if identity(data) != identity(baseline):
            raise ValueError(f"Запуск {variant} використовує інші дані: {path}")
        if data["result"] != baseline["result"] or not data["repeats_equal"]:
            raise ValueError(f"Результати {variant} відрізняються: {path}")
    for key in ("spark.sql.autoBroadcastJoinThreshold", "spark.sql.adaptive.enabled", "spark.sql.shuffle.partitions"):
        if runs["b"][1]["settings"][key] != runs["c"][1]["settings"][key]:
            raise ValueError(f"Налаштування b/c відрізняються: {key}")
    comparisons, tasks = [], []
    for variant, (path, data) in runs.items():
        first = data["runs"][1]
        join = first.get("join_stage", {})
        comparisons.append({
            "variant": variant, "application_id": data["application_id"],
            "join_strategy": first["join_strategy"],
            "seconds": "; ".join(f"{value:.6f}" for value in data["seconds"]),
            "median_seconds": data["median_seconds"], "sql_execution_id": first["sql_execution_id"],
            "join_stage_id": join.get("stage_id"), "max_records": join.get("max_records"),
            "median_records": join.get("median_records"), "max_over_median": join.get("max_over_median"),
            "join_shuffle_read_bytes": join.get("shuffle_read_bytes"),
            "source_shuffle_write_bytes": join.get("source_shuffle_write_bytes"),
            "total_shuffle_write_bytes": first["total_shuffle_write_bytes"], "results_file": str(path),
        })
        for task in join.get("task_metrics", []):
            tasks.append({"variant": variant, "stage_id": join["stage_id"], **task})
    comparison = {"results_equal": True, "reference": "repeat_1 metrics; medians from all three measured repeats",
                  "comparison": comparisons, "join_tasks": tasks, "city_revenue": baseline["result"]}
    (args.evidence_dir / "comparison.json").write_text(json.dumps(comparison, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    for filename, values in (("comparison.csv", comparisons), ("shuffle_read_records.csv", tasks)):
        with (args.evidence_dir / filename).open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(values[0]))
            writer.writeheader()
            writer.writerows(values)
    print("Results a = b = c: OK (all cities, exact decimal sums, identical input fingerprints)")
    for row in comparisons:
        print(f"{row['variant']}: {row['join_strategy']}; seconds={row['seconds']}; "
              f"median={row['median_seconds']:.6f}; max/median records={row['max_over_median']}")
    print(f"Comparison files: {args.evidence_dir.resolve()}")


if __name__ == "__main__":
    main()
