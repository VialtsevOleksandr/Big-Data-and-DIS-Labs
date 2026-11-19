"""Експеримент 2: час підтвердження запису в асинхронному та синхронному режимах.

  python -m notes.exp2 --label async --writes 10000 --warmup 1000 --runs 3 --dir /results/exp2/async

Режим реплікації перемикає run_exp2.ps1 (synchronous_standby_names на лідері); цей модуль
лише пише й міряє. Спочатку прогрів (не враховується), потім --runs запусків по --writes
записів. Для кожного запуску — p50, p95, p99; у підсумку — медіана цих значень між запусками.
"""
from __future__ import annotations

import argparse
import csv
import random
import statistics
import sys
import time
from pathlib import Path

from . import config, db, textgen
from .journal import Journal
from .service import INSERT_SQL
from .stats import summarize


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.exp2")
    p.add_argument("--label", required=True, help="mode name for the report, e.g. async or sync")
    p.add_argument("--writes", type=int, default=10000)
    p.add_argument("--warmup", type=int, default=1000)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--leader")
    p.add_argument("--dir", required=True)
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args(argv)

    leader = args.leader or config.default_leader()
    out = Path(args.dir)
    out.mkdir(parents=True, exist_ok=True)
    run_id = f"{out.name}-{time.strftime('%H%M%S')}"
    rng = random.Random(args.seed)
    bodies = [textgen.filler(rng, 200) for _ in range(64)]

    conn = db.connect(leader)
    node = db.scalar(conn, "SHOW cluster_name")
    settings = {
        "synchronous_standby_names": db.scalar(conn, "SHOW synchronous_standby_names"),
        "synchronous_commit": db.scalar(conn, "SHOW synchronous_commit"),
        "replicas": db.rows(conn, "SELECT application_name, state, sync_state FROM pg_stat_replication "
                                  "ORDER BY application_name"),
    }
    print(f"exp2 label={args.label} leader={leader} settings={settings}", flush=True)

    for i in range(args.warmup):
        conn.execute(INSERT_SQL, (rng.randint(1, 1000), f"{run_id}/warmup/{i:06d}", bodies[i % 64])).fetchone()
    print(f"  warm-up done: {args.warmup} writes", flush=True)

    journal = Journal(out / "journal.csv")
    runs = []
    latency_file = (out / "latencies.csv").open("w", newline="", encoding="utf-8")
    latency_csv = csv.writer(latency_file)
    latency_csv.writerow(["run", "seq", "latency_ms"])
    seq = 0
    for run_no in range(1, args.runs + 1):
        latencies = []
        started = time.perf_counter()
        for i in range(args.writes):
            seq += 1
            title = f"{run_id}/{seq:08d}"
            sent_at = time.time()
            t0 = time.perf_counter()
            note_id = conn.execute(INSERT_SQL, (rng.randint(1, 1000), title, bodies[i % 64])).fetchone()["id"]
            latency = (time.perf_counter() - t0) * 1000.0
            latencies.append(latency)
            journal.write(seq=seq, status="ok", note_id=note_id, title=title, node=node,
                          sent_at=sent_at, acked_at=sent_at + latency / 1000.0, latency_ms=latency)
            latency_csv.writerow([run_no, seq, f"{latency:.3f}"])
        elapsed = time.perf_counter() - started
        stats = summarize(latencies)
        stats["run"] = run_no
        stats["seconds"] = round(elapsed, 3)
        stats["writes_per_second"] = round(args.writes / elapsed, 1)
        runs.append(stats)
        print(f"  run {run_no}: p50={stats['p50']} ms p95={stats['p95']} ms p99={stats['p99']} ms "
              f"({stats['writes_per_second']} writes/s)", flush=True)

    journal.close()
    latency_file.close()
    conn.close()

    summary = {
        "label": args.label,
        "leader": leader,
        "writes_per_run": args.writes,
        "warmup_writes": args.warmup,
        "settings": settings,
        "runs": runs,
        "median_of_runs_ms": {
            key: round(statistics.median(r[key] for r in runs), 3) for key in ("p50", "p95", "p99", "mean")
        },
    }
    db.dump(summary, out / "summary.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
