"""Експеримент 1: відставання реплікації і читання власних записів.

  python -m notes.exp1 --mode naive --iterations 10000 --dir /results/exp1/1_naive_nodelay
  python -m notes.exp1 --mode raw   --iterations 10000 --dir /results/exp1/4_raw_delay

Кожна ітерація: записати нотатку на лідера і одразу прочитати її за id.
  naive — читання завжди з фоловера; рахуємо, скільки читань не побачили запис;
  raw   — читання через ReadYourWritesSession (гарантія read-after-write).

Паралельно окремий потік міряє відставання засобами самої СУБД:
pg_stat_replication на лідері та pg_last_xact_replay_timestamp() на фоловері.
"""
from __future__ import annotations

import argparse
import csv
import random
import sys
import threading
import time
from pathlib import Path

from . import config, db, textgen
from .journal import Journal
from .service import NotesService, ReadYourWritesSession
from .stats import summarize

LEADER_LAG_SQL = """
SELECT state, sync_state,
       pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)::bigint   AS send_lag_bytes,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint AS replay_lag_bytes,
       extract(epoch FROM write_lag)::float8  AS write_lag_s,
       extract(epoch FROM flush_lag)::float8  AS flush_lag_s,
       extract(epoch FROM replay_lag)::float8 AS replay_lag_s
FROM pg_stat_replication
WHERE application_name = %s
"""

FOLLOWER_LAG_SQL = """
SELECT CASE WHEN pg_last_wal_receive_lsn() = pg_last_wal_replay_lsn() THEN 0
            ELSE extract(epoch FROM clock_timestamp() - pg_last_xact_replay_timestamp())::float8
       END AS replay_delay_s,
       pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn())::bigint AS received_not_replayed_bytes,
       current_setting('recovery_min_apply_delay') AS apply_delay_setting
"""


class LagSampler(threading.Thread):
    """Раз на interval секунд знімає відставання фоловера з лідера і з самого фоловера."""

    def __init__(self, leader: str, follower: str, interval: float, path: Path):
        super().__init__(daemon=True)
        self.leader_conn = db.connect(leader, application_name="lag-sampler")
        self.follower_conn = db.connect(follower, application_name="lag-sampler")
        self.follower_name = config.node_name(follower)
        self.interval = interval
        self.path = path
        self.samples: list[dict] = []
        self._halt = threading.Event()

    def run(self) -> None:
        while not self._halt.is_set():
            sample = {"at": round(time.time(), 3)}
            try:
                sample.update(db.one(self.leader_conn, LEADER_LAG_SQL, (self.follower_name,)) or {})
                sample.update(db.one(self.follower_conn, FOLLOWER_LAG_SQL) or {})
            except Exception as exc:  # вимір не має зривати експеримент
                sample["error"] = str(exc).splitlines()[0]
            self.samples.append(sample)
            self._halt.wait(self.interval)

    def finish(self) -> dict:
        self._halt.set()
        self.join(timeout=5)
        fields = ["at", "state", "sync_state", "send_lag_bytes", "replay_lag_bytes", "write_lag_s",
                  "flush_lag_s", "replay_lag_s", "replay_delay_s", "received_not_replayed_bytes",
                  "apply_delay_setting", "error"]
        with self.path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(self.samples)
        for conn in (self.leader_conn, self.follower_conn):
            try:
                conn.close()
            except Exception:
                pass
        setting = next((s.get("apply_delay_setting") for s in self.samples if s.get("apply_delay_setting")), None)
        return {
            "samples": len(self.samples),
            "recovery_min_apply_delay": setting,
            "leader_replay_lag_bytes": summarize([s.get("replay_lag_bytes") for s in self.samples], 0),
            "leader_write_lag_ms": summarize([_ms(s.get("write_lag_s")) for s in self.samples]),
            "leader_flush_lag_ms": summarize([_ms(s.get("flush_lag_s")) for s in self.samples]),
            "leader_replay_lag_ms": summarize([_ms(s.get("replay_lag_s")) for s in self.samples]),
            "follower_replay_delay_ms": summarize([_ms(s.get("replay_delay_s")) for s in self.samples]),
        }


def _ms(seconds):
    return None if seconds is None else seconds * 1000.0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.exp1")
    p.add_argument("--mode", choices=["naive", "raw"], required=True)
    p.add_argument("--iterations", type=int, default=10000)
    p.add_argument("--leader", help="node that takes writes (default: first of NOTES_NODES)")
    p.add_argument("--follower", help="node that serves reads (default: second of NOTES_NODES)")
    p.add_argument("--dir", required=True)
    p.add_argument("--label", default="")
    p.add_argument("--raw-wait-ms", type=float, default=10.0,
                   help="raw mode: how long a read waits for the follower before going to the leader")
    p.add_argument("--measure-visible", type=int, default=30,
                   help="naive mode: for this many stale reads, measure how long until the note appears")
    p.add_argument("--lag-interval-ms", type=float, default=200.0)
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args(argv)

    leader = args.leader or config.NODES[0]
    follower = args.follower or config.NODES[1]
    out = Path(args.dir)
    out.mkdir(parents=True, exist_ok=True)
    run_id = f"{out.name}-{time.strftime('%H%M%S')}"
    rng = random.Random(args.seed)
    body = textgen.filler(rng, 200)

    leader_conn = db.connect(leader)
    follower_conn = db.connect(follower)
    if db.scalar(leader_conn, "SELECT pg_is_in_recovery()"):
        raise SystemExit(f"{leader} is a follower, cannot write to it")
    if not db.scalar(follower_conn, "SELECT pg_is_in_recovery()"):
        raise SystemExit(f"{follower} is not a follower")

    leader_svc = NotesService(leader_conn)
    follower_svc = NotesService(follower_conn)
    session = ReadYourWritesSession(leader_conn, follower_conn, wait_ms=args.raw_wait_ms)
    journal = Journal(out / "journal.csv")
    sampler = LagSampler(leader, follower, args.lag_interval_ms / 1000.0, out / "lag_samples.csv")
    sampler.start()

    stale = 0
    stale_ids: list[int] = []
    write_ms: list[float] = []
    read_ms: list[float] = []
    visible_after_ms: list[float] = []
    started = time.perf_counter()
    last_report = started
    print(f"exp1 mode={args.mode} leader={leader} follower={follower} iterations={args.iterations}", flush=True)

    for i in range(1, args.iterations + 1):
        title = f"{run_id}/{i:08d}"
        author = rng.randint(1, 1000)
        sent_at = time.time()
        t0 = time.perf_counter()
        if args.mode == "raw":
            note_id = session.create(author, title, body)
        else:
            note_id = leader_svc.create(author, title, body)
        t1 = time.perf_counter()
        acked_at = time.time()

        if args.mode == "raw":
            note, source = session.get(note_id)
        else:
            note, source = follower_svc.get(note_id), "follower"
        t2 = time.perf_counter()

        write_ms.append((t1 - t0) * 1000.0)
        read_ms.append((t2 - t1) * 1000.0)
        is_stale = note is None
        journal.write(seq=i, status="ok", note_id=note_id, title=title, node=config.node_name(leader),
                      sent_at=sent_at, acked_at=acked_at, latency_ms=(t1 - t0) * 1000.0,
                      detail=f"read={source} stale={int(is_stale)}")
        if is_stale:
            stale += 1
            if len(stale_ids) < 20:
                stale_ids.append(note_id)
            # Для кількох застарілих читань міряємо, через скільки запис таки з'явився на фоловері.
            if args.mode == "naive" and len(visible_after_ms) < args.measure_visible:
                deadline = time.perf_counter() + 10.0
                while time.perf_counter() < deadline:
                    if follower_svc.get(note_id) is not None:
                        visible_after_ms.append((time.perf_counter() - t1) * 1000.0)
                        break
                    time.sleep(0.001)

        now = time.perf_counter()
        if now - last_report >= 5.0:
            last_report = now
            print(f"  {i}/{args.iterations} stale={stale}", flush=True)

    elapsed = time.perf_counter() - started
    journal.close()
    lag = sampler.finish()
    leader_conn.close()
    follower_conn.close()

    summary = {
        "label": args.label or out.name,
        "mode": args.mode,
        "leader": leader,
        "follower": follower,
        "iterations": args.iterations,
        "stale_reads": stale,
        "stale_share_percent": round(100.0 * stale / args.iterations, 3),
        "first_stale_note_ids": stale_ids,
        "seconds": round(elapsed, 3),
        "write_ms": summarize(write_ms),
        "read_ms": summarize(read_ms),
        "replication_lag": lag,
    }
    if args.mode == "naive":
        summary["stale_note_visible_after_ms"] = summarize(visible_after_ms)
    else:
        summary["raw_wait_ms_limit"] = args.raw_wait_ms
        summary["read_sources"] = dict(session.sources)
        summary["reads_from_leader_percent"] = round(100.0 * session.sources["leader"] / args.iterations, 3)
        summary["reads_that_waited_percent"] = round(
            100.0 * session.sources["follower_waited"] / args.iterations, 3)
        summary["wait_before_read_ms"] = summarize(session.wait_ms_samples)
    db.dump(summary, out / "summary.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
