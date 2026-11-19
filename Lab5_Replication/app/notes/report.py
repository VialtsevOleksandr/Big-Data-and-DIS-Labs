"""Зведення журналу застосунку: скільки записів підтверджено, найдовша пауза, затримки.

  python -m notes.report --dir /results/exp3/partition_300s --t0 <розрив> --t1 <відновлення>

--t0 і --t1 — моменти збою та його завершення (секунди Unix, годинник контейнера).
  window           — що відбувалося між t0 і t1;
  longest_ack_gap  — найдовший проміжок без жодного підтвердженого запису;
  failover         — якщо після t0 записи почав підтверджувати інший вузол: коли саме.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from . import db
from .journal import read_journal
from .stats import summarize


def iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.report")
    p.add_argument("--dir", required=True, help="directory with journal.csv")
    p.add_argument("--t0", help="Unix time of the fault")
    p.add_argument("--t1", help="Unix time when the fault ended")
    p.add_argument("--out", help="result file (default <dir>/report.json)")
    args = p.parse_args(argv)

    journal = read_journal(Path(args.dir) / "journal.csv")
    t0 = float(args.t0) if args.t0 else None
    t1 = float(args.t1) if args.t1 else None
    acked = [r for r in journal if r["status"] == "ok"]

    by_status: dict[str, int] = {}
    for r in journal:
        by_status[r["status"]] = by_status.get(r["status"], 0) + 1
    ok_by_node: dict[str, int] = {}
    for r in acked:
        ok_by_node[r["node"]] = ok_by_node.get(r["node"], 0) + 1

    gap = {"seconds": 0.0}
    for prev, cur in zip(acked, acked[1:]):
        delta = cur["acked_at"] - prev["acked_at"]
        if delta > gap["seconds"]:
            gap = {"seconds": round(delta, 3),
                   "from": iso(prev["acked_at"]), "to": iso(cur["acked_at"]),
                   "from_unix": prev["acked_at"], "to_unix": cur["acked_at"],
                   "node_before": prev["node"], "node_after": cur["node"],
                   "attempts_without_ack": cur["seq"] - prev["seq"] - 1}

    warnings = [r["detail"] for r in acked if r["detail"]]
    result = {
        "dir": args.dir,
        "attempts": len(journal),
        "by_status": by_status,
        "ok_by_node": ok_by_node,
        "first_sent": iso(journal[0]["sent_at"]) if journal else None,
        "last_acked": iso(acked[-1]["acked_at"]) if acked else None,
        "latency_ms_all": summarize([r["latency_ms"] for r in acked]),
        "acks_slower_than_1s": sum(1 for r in acked if r["latency_ms"] > 1000.0),
        "acks_with_server_warning": len(warnings),
        "first_server_warnings": sorted(set(warnings))[:3],
        "longest_ack_gap": gap,
    }

    if t0 is not None:
        end = t1 if t1 is not None else float("inf")
        sent_in = [r for r in journal if r["sent_at"] is not None and t0 <= r["sent_at"] < end]
        acked_in = [r for r in acked if t0 <= r["acked_at"] < end]
        result["window"] = {
            "t0": iso(t0), "t1": iso(t1),
            "seconds": None if t1 is None else round(t1 - t0, 3),
            "attempts_sent": len(sent_in),
            "acked": len(acked_in),
            "not_acked": sum(1 for r in sent_in if r["status"] != "ok"),
            "latency_ms": summarize([r["latency_ms"] for r in acked_in]),
        }
        before = [r for r in acked if r["acked_at"] <= t0]
        if before:
            old_node = before[-1]["node"]
            switched = next((r for r in acked if r["acked_at"] > t0 and r["node"] != old_node), None)
            failed = next((r for r in journal if r["sent_at"] is not None and r["sent_at"] >= t0
                           and r["status"] != "ok"), None)
            result["failover"] = {
                "node_before": old_node,
                "last_ack_before_fault": iso(before[-1]["acked_at"]),
                "first_failed_attempt": iso(failed["sent_at"]) if failed else None,
                "node_after": switched["node"] if switched else None,
                "first_ack_on_new_node": iso(switched["acked_at"]) if switched else None,
                "fault_to_first_ack_on_new_node_s":
                    round(switched["acked_at"] - t0, 3) if switched else None,
                "no_acks_for_s":
                    round(switched["acked_at"] - before[-1]["acked_at"], 3) if switched else None,
            }

    db.dump(result, args.out or Path(args.dir) / "report.json")
    return 0


if __name__ == "__main__":
    sys.exit(main())
