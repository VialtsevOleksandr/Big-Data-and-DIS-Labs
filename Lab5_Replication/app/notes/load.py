"""Навантаження зі сталою швидкістю: N записів на секунду на лідера.

  python -m notes.load --dir /results/exp3/run --rate 100

Працює, доки в каталозі --dir не з'явиться файл STOP (або не мине --duration секунд).
Пише journal.csv (кожна спроба запису), load.log (хід роботи) і наприкінці summary.json.

Лідера застосунок шукає сам серед --hosts (див. db.LeaderFinder), тому після failover
він без перезапуску переходить на новий вузол.

Сталий темп: записи заплановано на моменти t0 + k/rate. Коротке відставання (до --catch-up,
за замовчуванням 5 с) застосунок надолужує, пишучи без пауз, тож середній темп лишається заданим:
лідер періодично зупиняється на 1–1,5 с на fsync контрольної точки.
Те, що старше, не надолужується, а рахується як skipped_ticks: після довгої зупинки бази
застосунок не засипає її шквалом відкладених записів.
"""
from __future__ import annotations

import argparse
import random
import sys
import threading
import time
import traceback
from pathlib import Path

import psycopg

from . import config, db, textgen
from .journal import Journal
from .service import INSERT_SQL
from .stats import summarize


class Watchdog(threading.Thread):
    """Скасовує запит, якщо база не підтвердила запис за timeout секунд."""

    def __init__(self, timeout: float):
        super().__init__(daemon=True)
        self.timeout = timeout
        self.conn = None
        self.deadline = None
        self.fired = 0

    def arm(self, conn) -> None:
        self.conn = conn
        self.deadline = time.monotonic() + self.timeout

    def disarm(self) -> None:
        self.deadline = None

    def run(self) -> None:
        while True:
            time.sleep(0.05)
            deadline, conn = self.deadline, self.conn
            if deadline is not None and time.monotonic() > deadline:
                self.deadline = None
                self.fired += 1
                try:
                    conn.cancel()
                except Exception:
                    pass


def run(args) -> dict:
    out = Path(args.dir)
    out.mkdir(parents=True, exist_ok=True)
    stop_file = out / "STOP"
    summary_file = out / "summary.json"
    for stale in (stop_file, summary_file):
        if stale.exists():
            stale.unlink()

    log = (out / "load.log").open("w", encoding="utf-8")

    def say(message: str) -> None:
        line = f"{time.strftime('%H:%M:%S')} {message}"
        print(line, flush=True)
        log.write(line + "\n")
        log.flush()

    hosts = config.parse_hosts(args.hosts)
    run_id = args.run_id or f"{out.name}-{time.strftime('%H%M%S')}"
    rng = random.Random(args.seed)
    bodies = [textgen.filler(rng, args.body_size) for _ in range(64)]
    journal = Journal(out / "journal.csv")
    finder = db.LeaderFinder(hosts)

    watchdog = None
    if args.ack_timeout > 0:
        watchdog = Watchdog(args.ack_timeout)
        watchdog.start()

    notices: list[str] = []
    conn = None
    node = ""
    connections = []
    counts = {"ok": 0, "error": 0, "no_leader": 0}
    ok_by_node: dict[str, int] = {}
    latencies: list[float] = []
    skipped = 0
    last_ack = None
    longest_gap = {"seconds": 0.0, "from": None, "to": None}

    interval = 1.0 / args.rate
    max_backlog = int(args.catch_up * args.rate)
    started_wall = time.time()
    started = time.monotonic()
    next_tick = started
    last_check = started
    last_progress = started
    seq = 0
    say(f"run_id={run_id} rate={args.rate}/s hosts={','.join(hosts)} body={args.body_size}B "
        f"ack_timeout={args.ack_timeout}s catch_up={args.catch_up}s")

    try:
        while True:
            now = time.monotonic()
            if now - last_check >= 0.25:
                last_check = now
                if stop_file.exists():
                    say("STOP file found")
                    break
                if args.duration > 0 and now - started >= args.duration:
                    say("duration elapsed")
                    break
            if now - last_progress >= 5.0:
                last_progress = now
                say(f"ok={counts['ok']} error={counts['error']} no_leader={counts['no_leader']} "
                    f"skipped={skipped} node={node or '-'}")

            if now < next_tick:
                time.sleep(next_tick - now)
            else:
                behind = int((now - next_tick) / interval)
                if behind > max_backlog:
                    skipped += behind - max_backlog
                    next_tick += (behind - max_backlog) * interval
            next_tick += interval
            seq += 1
            title = f"{run_id}/{seq:08d}"

            if conn is None:
                conn, host = finder.try_connect()
                if conn is None:
                    counts["no_leader"] += 1
                    journal.write(seq=seq, status="no_leader", title=title, sent_at=time.time(),
                                  detail="; ".join(f"{h}: {e}" for h, e in finder.last_errors.items()))
                    continue
                conn.add_notice_handler(
                    lambda diag: notices.append(f"{diag.severity}: {diag.message_primary}"))
                node = db.scalar(conn, "SHOW cluster_name")
                connections.append({"at": round(time.time(), 6), "host": host, "node": node, "seq": seq})
                say(f"connected to leader {node} ({host}) at seq {seq}")

            sent_at = time.time()
            t0 = time.perf_counter()
            try:
                if watchdog:
                    watchdog.arm(conn)
                note_id = db.scalar(conn, INSERT_SQL,
                                    (rng.randint(1, args.authors), title, bodies[seq % len(bodies)]))
                if watchdog:
                    watchdog.disarm()
                latency = (time.perf_counter() - t0) * 1000.0
                acked_at = time.time()
                detail = " | ".join(notices)
                notices.clear()
                journal.write(seq=seq, status="ok", note_id=note_id, title=title, node=node,
                              sent_at=sent_at, acked_at=acked_at, latency_ms=latency, detail=detail)
                counts["ok"] += 1
                ok_by_node[node] = ok_by_node.get(node, 0) + 1
                latencies.append(latency)
                if last_ack is not None and acked_at - last_ack > longest_gap["seconds"]:
                    longest_gap = {"seconds": round(acked_at - last_ack, 3),
                                   "from": round(last_ack, 6), "to": round(acked_at, 6)}
                last_ack = acked_at
            except psycopg.Error as exc:
                if watchdog:
                    watchdog.disarm()
                latency = (time.perf_counter() - t0) * 1000.0
                message = str(exc).strip().splitlines()[0] if str(exc).strip() else ""
                journal.write(seq=seq, status="error", title=title, node=node, sent_at=sent_at,
                              latency_ms=latency, detail=f"{type(exc).__name__}: {message}")
                counts["error"] += 1
                notices.clear()
                if conn.closed or conn.broken:
                    say(f"lost connection to {node} at seq {seq}: {type(exc).__name__}: {message}")
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
    except KeyboardInterrupt:
        say("interrupted")
    finally:
        journal.close()
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass

    elapsed = time.monotonic() - started
    summary = {
        "run_id": run_id,
        "hosts": hosts,
        "rate_requested": args.rate,
        "rate_acked": round(counts["ok"] / elapsed, 2) if elapsed > 0 else 0,
        "seconds": round(elapsed, 3),
        "started_at": round(started_wall, 6),
        "ended_at": round(time.time(), 6),
        "attempts": seq,
        "ok": counts["ok"],
        "error": counts["error"],
        "no_leader": counts["no_leader"],
        "skipped_ticks": skipped,
        "ok_by_node": ok_by_node,
        "connections": connections,
        "ack_timeout_fired": watchdog.fired if watchdog else 0,
        "latency_ms": summarize(latencies),
        "longest_ack_gap": longest_gap,
    }
    say(f"finished: ok={counts['ok']} error={counts['error']} no_leader={counts['no_leader']} "
        f"skipped={skipped}")
    log.close()
    db.dump(summary, summary_file)  # summary.json з'являється останнім: це сигнал, що запуск завершено
    return summary


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.load", description="Constant-rate writer.")
    p.add_argument("--dir", required=True, help="output directory (journal.csv, load.log, summary.json)")
    p.add_argument("--rate", type=float, default=100.0, help="writes per second (default 100)")
    p.add_argument("--duration", type=float, default=0.0, help="seconds to run; 0 = until STOP file")
    p.add_argument("--hosts", help="comma-separated nodes to look for the leader (default NOTES_NODES)")
    p.add_argument("--run-id", help="prefix of note titles (default: directory name + time)")
    p.add_argument("--body-size", type=int, default=200, help="note body size in bytes (default 200)")
    p.add_argument("--authors", type=int, default=1000)
    p.add_argument("--ack-timeout", type=float, default=0.0,
                   help="cancel a write that is not acknowledged within this many seconds; 0 = wait forever")
    p.add_argument("--catch-up", type=float, default=5.0,
                   help="seconds of backlog the writer still makes up for; older ticks are skipped (default 5)")
    p.add_argument("--seed", type=int, default=1)
    args = p.parse_args(argv)
    if args.rate <= 0:
        raise SystemExit("--rate must be positive")
    try:
        run(args)
    except Exception:
        # docker exec -d не показує stderr, тому причину падіння лишаємо у файлі.
        Path(args.dir).mkdir(parents=True, exist_ok=True)
        (Path(args.dir) / "load_error.log").write_text(traceback.format_exc(), encoding="utf-8")
        raise
    return 0


if __name__ == "__main__":
    sys.exit(main())
