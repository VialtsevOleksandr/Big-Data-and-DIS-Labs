"""Ручний failover для конфігурації A.

  python -m notes.failover roles
  python -m notes.failover promote --candidates repa-pg2,repa-pg3 --out /results/exp4/run/promote.json
  python -m notes.failover probe-write --node repa-pg1
"""
from __future__ import annotations

import argparse
import sys
import time

import psycopg

from . import config, db
from .service import NotesService


def node_role(host: str) -> dict:
    try:
        conn = db.connect(host, admin=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        text = str(exc).strip()
        return {"host": host, "reachable": False, "error": text.splitlines()[-1] if text else "error"}
    with conn:
        info = db.one(conn, """
            SELECT current_setting('cluster_name') AS name, pg_is_in_recovery() AS in_recovery,
                   (SELECT system_identifier FROM pg_control_system()) AS system_identifier""")
        role = {"host": host, "reachable": True, "name": info["name"],
                "role": "follower" if info["in_recovery"] else "leader",
                "accepts_writes": not info["in_recovery"],
                "system_identifier": info["system_identifier"],
                "notes": db.scalar(conn, "SELECT count(*) FROM notes")}
        if info["in_recovery"]:
            receiver = db.one(conn, "SELECT status, sender_host, received_tli FROM pg_stat_wal_receiver")
            role["timeline"] = receiver["received_tli"] if receiver else None
            role["follows"] = receiver["sender_host"] if receiver else None
            role["lsn"] = db.scalar(conn, "SELECT pg_last_wal_replay_lsn()")
        else:
            # Перші 8 символів імені файлу WAL — номер лінії часу (timeline).
            role["timeline"] = int(db.scalar(
                conn, "SELECT substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8)"), 16)
            role["lsn"] = db.scalar(conn, "SELECT pg_current_wal_lsn()")
            role["followers"] = [r["application_name"] for r in db.rows(
                conn, "SELECT application_name FROM pg_stat_replication ORDER BY 1")]
        return role


def cmd_roles(args) -> int:
    roles = [node_role(h) for h in config.parse_hosts(args.nodes)]
    leaders = [r["name"] for r in roles if r.get("role") == "leader"]
    db.dump({"at": round(time.time(), 3), "nodes_accepting_writes": leaders,
             "two_leaders_at_once": len(leaders) > 1, "nodes": roles}, args.out)
    return 0


def cmd_promote(args) -> int:
    """Робить новим лідером той фоловер, який отримав від старого лідера найбільше WAL.

    Після підвищення створює на новому лідері слоти для решти вузлів, щоб він утримував
    для них WAL, поки вони перемикаються.
    """
    candidates = []
    for host in config.parse_hosts(args.candidates):
        conn = db.connect(host, admin=True)
        info = db.one(conn, """
            SELECT pg_is_in_recovery() AS in_recovery,
                   pg_last_wal_receive_lsn() AS receive_lsn, pg_last_wal_replay_lsn() AS replay_lsn""")
        if not info["in_recovery"]:
            raise SystemExit(f"{host} is already a leader")
        position = info["receive_lsn"] or info["replay_lsn"]
        candidates.append({"host": host, "name": config.node_name(host), "conn": conn,
                           "receive_lsn": info["receive_lsn"], "replay_lsn": info["replay_lsn"],
                           "position": db.lsn_to_int(position)})

    chosen = max(candidates, key=lambda c: c["position"])  # за рівних позицій — перший у списку
    conn = chosen["conn"]
    started = time.time()
    promoted = db.scalar(conn, "SELECT pg_promote(true, 60)")
    finished = time.time()
    if not promoted:
        raise SystemExit(f"pg_promote on {chosen['host']} did not finish within 60 s")
    # Контрольна точка фіксує нову лінію часу в pg_control: це потрібно pg_rewind.
    conn.execute("CHECKPOINT")

    all_nodes = config.parse_hosts(args.all_nodes)
    slots = []
    for host in all_nodes:
        if host == chosen["host"]:
            continue
        slot = f"{config.node_name(host)}_slot"
        conn.execute("""
            SELECT pg_create_physical_replication_slot(%s, true)
            WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = %s)""", (slot, slot))
        slots.append(slot)

    result = {
        "new_leader": chosen["host"],
        "new_leader_name": chosen["name"],
        "promote_started_at": round(started, 6),
        "promote_finished_at": round(finished, 6),
        "promote_seconds": round(finished - started, 3),
        "candidates": [{k: c[k] for k in ("host", "name", "receive_lsn", "replay_lsn")} for c in candidates],
        "other_followers": [c["host"] for c in candidates if c["host"] != chosen["host"]],
        "slots_created_on_new_leader": slots,
        "new_leader_lsn": db.scalar(conn, "SELECT pg_current_wal_lsn()"),
    }
    for c in candidates:
        c["conn"].close()
    db.dump(result, args.out)
    return 0


def cmd_probe_write(args) -> int:
    """Пробує записати одну нотатку прямо на вказаний вузол: чи приймає він записи."""
    result = {"node": args.node, "at": round(time.time(), 6)}
    try:
        with db.connect(args.node, connect_timeout=3) as conn:
            result["cluster_name"] = db.scalar(conn, "SHOW cluster_name")
            result["note_id"] = NotesService(conn).create(1, args.title, "written directly to this node")
            result["accepted"] = True
    except psycopg.Error as exc:
        result["accepted"] = False
        result["error"] = f"{type(exc).__name__}: {str(exc).strip().splitlines()[0]}"
    db.dump(result, args.out)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.failover")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("roles")
    s.add_argument("--nodes")
    s.add_argument("--out")
    s.set_defaults(func=cmd_roles)

    s = sub.add_parser("promote")
    s.add_argument("--candidates", required=True, help="followers that may become the leader")
    s.add_argument("--all-nodes", help="all nodes of the cluster (default NOTES_NODES)")
    s.add_argument("--out")
    s.set_defaults(func=cmd_promote)

    s = sub.add_parser("probe-write")
    s.add_argument("--node", required=True)
    s.add_argument("--title", default="split-brain probe")
    s.add_argument("--out")
    s.set_defaults(func=cmd_probe_write)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
