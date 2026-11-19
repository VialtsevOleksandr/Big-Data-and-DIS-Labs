"""Службові команди: стан вузлів, очікування реплікації, порівняння даних, зміна параметрів.

  python -m notes.admin status
  python -m notes.admin wait-streaming --leader repa-pg1 --names pg2,pg3
  python -m notes.admin wait-sync --leader repa-pg1
  python -m notes.admin compare --nodes repa-pg1,repa-pg2
  python -m notes.admin set --node repa-pg1 synchronous_standby_names "FIRST 1 (pg2)"
  python -m notes.admin reset --node repa-pg1 synchronous_standby_names
"""
from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path

import psycopg
from psycopg import sql

from . import config, db

# Контрольна сума таблиці: md5 від md5 кожного рядка, рядки в порядку id.
CHECKSUM_SQL = """
SELECT count(*) AS notes,
       coalesce(max(id), 0) AS max_id,
       coalesce(md5(string_agg(
           md5(concat_ws('|', id, author_id, title, body, extract(epoch FROM updated_at))),
           '' ORDER BY id)), '') AS checksum
FROM notes
"""

REPLICAS_SQL = """
SELECT application_name, client_addr::text AS client_addr, state, sync_state,
       sent_lsn, write_lsn, flush_lsn, replay_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint AS replay_lag_bytes,
       write_lag::text AS write_lag, flush_lag::text AS flush_lag, replay_lag::text AS replay_lag
FROM pg_stat_replication
ORDER BY application_name
"""

SLOTS_SQL = """
SELECT slot_name, slot_type, active, restart_lsn, wal_status, safe_wal_size, invalidation_reason,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS retained_bytes
FROM pg_replication_slots
ORDER BY slot_name
"""


def node_status(host: str) -> dict:
    try:
        conn = db.connect(host, admin=True, connect_timeout=3)
    except psycopg.OperationalError as exc:
        text = str(exc).strip()
        return {"host": host, "reachable": False, "error": text.splitlines()[-1] if text else "error"}
    with conn:
        info = db.one(conn, "SELECT pg_is_in_recovery() AS in_recovery, current_setting('cluster_name') AS name")
        status = {"host": host, "reachable": True, "name": info["name"],
                  "role": "follower" if info["in_recovery"] else "leader",
                  "notes": db.scalar(conn, "SELECT count(*) FROM notes")}
        if info["in_recovery"]:
            status["receive_lsn"] = db.scalar(conn, "SELECT pg_last_wal_receive_lsn()")
            status["replay_lsn"] = db.scalar(conn, "SELECT pg_last_wal_replay_lsn()")
            status["wal_receiver"] = db.one(
                conn, "SELECT status, sender_host, slot_name, received_tli FROM pg_stat_wal_receiver")
            status["recovery_min_apply_delay"] = db.scalar(conn, "SHOW recovery_min_apply_delay")
        else:
            status["current_lsn"] = db.scalar(conn, "SELECT pg_current_wal_lsn()")
            status["synchronous_standby_names"] = db.scalar(conn, "SHOW synchronous_standby_names")
            status["replicas"] = db.rows(conn, REPLICAS_SQL)
        return status


def cmd_status(args) -> int:
    db.dump([node_status(h) for h in config.parse_hosts(args.nodes)], args.out)
    return 0


def cmd_wait_streaming(args) -> int:
    """Чекає, доки лідер бачить усі названі фоловери у стані streaming (і sync, якщо задано)."""
    names = [n for n in args.names.split(",") if n]
    sync_names = [n for n in (args.sync_names or "").split(",") if n]
    deadline = time.monotonic() + args.timeout
    last = "no answer from leader"
    while time.monotonic() < deadline:
        try:
            with db.connect(args.leader, admin=True, connect_timeout=3) as conn:
                if db.scalar(conn, "SELECT pg_is_in_recovery()"):
                    last = f"{args.leader} is a follower"
                else:
                    replicas = {r["application_name"]: r for r in db.rows(conn, REPLICAS_SQL)}
                    missing = [n for n in names if replicas.get(n, {}).get("state") != "streaming"]
                    not_sync = [n for n in sync_names if replicas.get(n, {}).get("sync_state") != "sync"]
                    if not missing and not not_sync:
                        for name in names:
                            r = replicas[name]
                            print(f"  {name}: {r['state']} {r['sync_state']} "
                                  f"replay_lag={r['replay_lag_bytes']} bytes", flush=True)
                        return 0
                    last = f"not streaming: {missing}; not sync: {not_sync}"
        except psycopg.OperationalError as exc:
            last = str(exc).strip().splitlines()[-1]
        time.sleep(1.0)
    print(f"TIMEOUT waiting for followers of {args.leader}: {last}", file=sys.stderr)
    return 1


def cmd_wait_sync(args) -> int:
    """Чекає, доки всі під'єднані фоловери відтворять WAL до поточної позиції лідера."""
    names = [n for n in (args.names or "").split(",") if n]
    with db.connect(args.leader, admin=True) as conn:
        target = db.scalar(conn, "SELECT pg_current_wal_lsn()")
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            replicas = db.rows(conn, """
                SELECT application_name, state, replay_lsn,
                       coalesce(replay_lsn >= %s::pg_lsn, false) AS done
                FROM pg_stat_replication""", (target,))
            wanted = [r for r in replicas if not names or r["application_name"] in names]
            present = {r["application_name"] for r in wanted}
            if wanted and all(r["done"] for r in wanted) and all(n in present for n in names):
                print(f"  followers {sorted(present)} replayed up to {target}", flush=True)
                return 0
            time.sleep(0.2)
    print(f"TIMEOUT: followers did not reach {target}", file=sys.stderr)
    return 1


def checksums(hosts: list[str]) -> list[dict]:
    result = []
    for host in hosts:
        try:
            with db.connect(host, admin=True, connect_timeout=3) as conn:
                row = db.one(conn, CHECKSUM_SQL)
                result.append({"host": host, "name": db.scalar(conn, "SHOW cluster_name"), **row})
        except psycopg.OperationalError as exc:
            result.append({"host": host, "error": str(exc).strip().splitlines()[-1]})
    return result


def cmd_compare(args) -> int:
    """Кількість нотаток і контрольна сума на кожному вузлі; equal = усі збіглися."""
    hosts = config.parse_hosts(args.nodes)
    deadline = time.monotonic() + args.until_equal
    while True:
        result = checksums(hosts)
        equal = (all("error" not in r for r in result)
                 and len({(r["notes"], r["checksum"]) for r in result}) == 1)
        if equal or time.monotonic() >= deadline:
            break
        time.sleep(1.0)
    db.dump({"equal": equal, "nodes": result}, args.out)
    return 0


def cmd_counts(args) -> int:
    result = []
    for host in config.parse_hosts(args.nodes):
        try:
            with db.connect(host, admin=True, connect_timeout=3) as conn:
                row = db.one(conn, "SELECT count(*) AS notes, coalesce(max(id), 0) AS max_id FROM notes")
                result.append({"host": host, "at": round(time.time(), 3), **row})
        except psycopg.OperationalError as exc:
            result.append({"host": host, "error": str(exc).strip().splitlines()[-1]})
    db.dump(result, args.out)
    return 0


def cmd_catchup(args) -> int:
    """Міряє, за який час фоловер наздоганяє лідера після відновлення зв'язку.

    Запускається ДО відновлення мережі (у фоні), бо наздоганяння на одній машині триває частки
    секунди і запізнілий вимір його просто не застане:
      1. фіксує ціль — позицію WAL лідера, тобто все, що накопичилося за час розриву,
         і позицію фоловера; пише --ready-file, що готовий;
      2. кожні 10 мс опитує фоловер: коли відновився потік WAL і коли відтворено ціль;
      3. момент відновлення мережі бере з --since або з файлу --since-file, який пише
         скрипт PowerShell перед `docker network connect`, і рахує час від нього.
    """
    leader = db.connect(args.leader, admin=True)
    follower = db.connect(args.follower, admin=True)
    target = db.scalar(leader, "SELECT pg_current_wal_lsn()")
    start = db.one(follower, "SELECT pg_last_wal_replay_lsn() AS replay, pg_last_wal_receive_lsn() AS receive")
    leader_notes = db.scalar(leader, "SELECT count(*) FROM notes")
    follower_notes = db.scalar(follower, "SELECT count(*) FROM notes")
    backlog = db.lsn_to_int(target) - db.lsn_to_int(start["replay"])
    captured_at = time.time()
    result = {
        "leader": args.leader, "follower": args.follower,
        "captured_at": round(captured_at, 6),
        "target_lsn": target, "follower_replay_lsn_at_start": start["replay"],
        "backlog_mib": db.mib(backlog),
        "notes_behind_at_start": leader_notes - follower_notes,
        "since": None, "resumed_at": None, "caught_up_at": None,
        "streaming_resumed_after_s": None, "caught_up_after_s": None,
        "replay_after_resume_s": None, "caught_up": False,
    }
    print(f"catch-up: target {target}, backlog {result['backlog_mib']} MiB, "
          f"{result['notes_behind_at_start']} notes behind", flush=True)
    if args.ready_file:
        Path(args.ready_file).write_text(f"{captured_at:.6f}\n", encoding="utf-8")

    deadline = time.monotonic() + args.timeout
    last_report = time.monotonic()
    row = {"replay": start["replay"]}
    while time.monotonic() < deadline:
        row = db.one(follower, """
            SELECT pg_last_wal_replay_lsn() AS replay,
                   pg_last_wal_replay_lsn() >= %s::pg_lsn AS done,
                   (SELECT status FROM pg_stat_wal_receiver) AS receiver""", (target,))
        now = time.time()
        if result["resumed_at"] is None and row["receiver"] == "streaming":
            result["resumed_at"] = round(now, 6)
        if row["done"]:
            result["caught_up_at"] = round(now, 6)
            result["caught_up"] = True
            break
        if time.monotonic() - last_report >= 5.0:
            last_report = time.monotonic()
            left = db.lsn_to_int(target) - db.lsn_to_int(row["replay"])
            print(f"  receiver={row['receiver']} remaining={db.mib(left)} MiB", flush=True)
        time.sleep(0.01)

    since = None
    if args.since:
        since = float(args.since)
    elif args.since_file:
        path = Path(args.since_file)
        for _ in range(300):
            if path.exists() and path.read_text(encoding="utf-8").strip():
                since = float(path.read_text(encoding="utf-8").strip())
                break
            time.sleep(0.1)
    result["since"] = since
    if since is not None:
        if result["resumed_at"] is not None:
            result["streaming_resumed_after_s"] = round(result["resumed_at"] - since, 3)
        if result["caught_up_at"] is not None:
            result["caught_up_after_s"] = round(result["caught_up_at"] - since, 3)
    if result["resumed_at"] is not None and result["caught_up_at"] is not None:
        result["replay_after_resume_s"] = round(result["caught_up_at"] - result["resumed_at"], 3)
        if result["replay_after_resume_s"] > 0:
            result["replay_mib_per_s"] = round(result["backlog_mib"] / result["replay_after_resume_s"], 3)
    leader.close()
    follower.close()
    db.dump(result, args.out)
    return 0 if result["caught_up"] else 1


def slot_report(conn) -> dict:
    wal = db.one(conn, "SELECT count(*) AS files, coalesce(sum(size), 0)::bigint AS bytes FROM pg_ls_waldir()")
    slots = db.rows(conn, SLOTS_SQL)
    for slot in slots:
        slot["retained_mib"] = None if slot["retained_bytes"] is None else db.mib(slot["retained_bytes"])
        slot["safe_wal_mib"] = None if slot["safe_wal_size"] is None else db.mib(slot["safe_wal_size"])
    return {
        "at": round(time.time(), 3),
        "max_slot_wal_keep_size": db.scalar(conn, "SHOW max_slot_wal_keep_size"),
        "pg_wal_files": wal["files"],
        "pg_wal_mib": db.mib(wal["bytes"]),
        "slots": slots,
    }


def cmd_slots(args) -> int:
    with db.connect(args.node, admin=True) as conn:
        db.dump(slot_report(conn), args.out)
    return 0


def cmd_watch_slot(args) -> int:
    """Стежить за слотом, доки лідер не оголосить його втраченим (wal_status = lost).

    Старий WAL видаляється на контрольній точці, тому щойно слот переходить у стан unreserved
    (перевищив max_slot_wal_keep_size), команда сама виконує CHECKPOINT.
    """
    history = []
    deadline = time.monotonic() + args.timeout
    with db.connect(args.node, admin=True) as conn:
        while True:
            report = slot_report(conn)
            slot = next((s for s in report["slots"] if s["slot_name"] == args.slot), None)
            if slot is None:
                print(f"slot {args.slot} not found on {args.node}", file=sys.stderr)
                return 1
            entry = {"at": report["at"], "wal_status": slot["wal_status"], "active": slot["active"],
                     "retained_mib": slot["retained_mib"], "safe_wal_mib": slot["safe_wal_mib"],
                     "pg_wal_mib": report["pg_wal_mib"], "checkpoint_issued": False}
            if slot["wal_status"] == "unreserved":
                conn.execute("CHECKPOINT")
                entry["checkpoint_issued"] = True
            history.append(entry)
            print(f"  {time.strftime('%H:%M:%S')} slot={args.slot} wal_status={entry['wal_status']} "
                  f"retained={entry['retained_mib']} MiB safe={entry['safe_wal_mib']} MiB "
                  f"pg_wal={entry['pg_wal_mib']} MiB"
                  f"{' -> CHECKPOINT' if entry['checkpoint_issued'] else ''}", flush=True)
            if slot["wal_status"] == "lost":
                db.dump({"slot": args.slot, "lost": True, "history": history}, args.out)
                return 0
            if time.monotonic() >= deadline:
                db.dump({"slot": args.slot, "lost": False, "history": history}, args.out)
                return 1
            # Після CHECKPOINT результат видно одразу, чекати повний інтервал не треба.
            time.sleep(1.0 if entry["checkpoint_issued"] else args.interval)


def cmd_drop_slots(args) -> int:
    """Видаляє неактивні слоти (залишки від попередньої ролі вузла)."""
    with db.connect(args.node, admin=True) as conn:
        dropped = [r["slot_name"] for r in db.rows(
            conn, "SELECT slot_name FROM pg_replication_slots WHERE NOT active")]
        for name in dropped:
            conn.execute("SELECT pg_drop_replication_slot(%s)", (name,))
    db.dump({"node": args.node, "dropped": dropped})
    return 0


def cmd_activity(args) -> int:
    """Запити, що зараз чекають підтвердження від синхронного фоловера."""
    with db.connect(args.node, admin=True) as conn:
        waiting = db.rows(conn, """
            SELECT pid, application_name, state, wait_event_type, wait_event,
                   round(extract(epoch FROM clock_timestamp() - query_start)::numeric, 3) AS waiting_s,
                   left(query, 70) AS query
            FROM pg_stat_activity
            WHERE wait_event = 'SyncRep'""")
    db.dump({"node": args.node, "waiting_for_sync_standby": waiting}, args.out)
    return 0


def _check_name(name: str) -> str:
    if not re.fullmatch(r"[a-z_]+", name):
        raise SystemExit(f"bad parameter name: {name}")
    return name


def cmd_set(args) -> int:
    """ALTER SYSTEM SET + pg_reload_conf(): значення потрапляє в postgresql.auto.conf вузла."""
    name = _check_name(args.name)
    with db.connect(args.node, admin=True) as conn:
        conn.execute(sql.SQL("ALTER SYSTEM SET {} = {}").format(sql.Identifier(name), sql.Literal(args.value)))
        conn.execute("SELECT pg_reload_conf()")
        time.sleep(0.5)
        print(f"  {args.node}: {name} = {db.scalar(conn, sql.SQL('SHOW {}').format(sql.Identifier(name)))}",
              flush=True)
    return 0


def cmd_reset(args) -> int:
    name = _check_name(args.name)
    with db.connect(args.node, admin=True) as conn:
        conn.execute(sql.SQL("ALTER SYSTEM RESET {}").format(sql.Identifier(name)))
        conn.execute("SELECT pg_reload_conf()")
        time.sleep(0.5)
        print(f"  {args.node}: {name} = {db.scalar(conn, sql.SQL('SHOW {}').format(sql.Identifier(name)))}",
              flush=True)
    return 0


def cmd_reload(args) -> int:
    with db.connect(args.node, admin=True) as conn:
        conn.execute("SELECT pg_reload_conf()")
    print(f"  {args.node}: configuration reloaded", flush=True)
    return 0


def cmd_checkpoint(args) -> int:
    with db.connect(args.node, admin=True) as conn:
        conn.execute("CHECKPOINT")
    print(f"  {args.node}: checkpoint done", flush=True)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.admin")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("status")
    s.add_argument("--nodes")
    s.add_argument("--out")
    s.set_defaults(func=cmd_status)

    s = sub.add_parser("wait-streaming")
    s.add_argument("--leader", required=True)
    s.add_argument("--names", required=True, help="follower names, e.g. pg2,pg3")
    s.add_argument("--sync-names", help="followers that must be synchronous")
    s.add_argument("--timeout", type=float, default=120)
    s.set_defaults(func=cmd_wait_streaming)

    s = sub.add_parser("wait-sync")
    s.add_argument("--leader", required=True)
    s.add_argument("--names", help="only these followers (default: all connected)")
    s.add_argument("--timeout", type=float, default=300)
    s.set_defaults(func=cmd_wait_sync)

    s = sub.add_parser("compare")
    s.add_argument("--nodes")
    s.add_argument("--until-equal", type=float, default=0, help="keep polling up to this many seconds")
    s.add_argument("--out")
    s.set_defaults(func=cmd_compare)

    s = sub.add_parser("counts")
    s.add_argument("--nodes")
    s.add_argument("--out")
    s.set_defaults(func=cmd_counts)

    s = sub.add_parser("catchup")
    s.add_argument("--leader", required=True)
    s.add_argument("--follower", required=True)
    s.add_argument("--since", help="Unix time when the network was reconnected")
    s.add_argument("--since-file", help="file where the reconnect time (Unix time) will appear")
    s.add_argument("--ready-file", help="file to create once the target position is captured")
    s.add_argument("--timeout", type=float, default=1800)
    s.add_argument("--out")
    s.set_defaults(func=cmd_catchup)

    s = sub.add_parser("slots")
    s.add_argument("--node", required=True)
    s.add_argument("--out")
    s.set_defaults(func=cmd_slots)

    s = sub.add_parser("watch-slot")
    s.add_argument("--node", required=True)
    s.add_argument("--slot", required=True)
    s.add_argument("--interval", type=float, default=10)
    s.add_argument("--timeout", type=float, default=900)
    s.add_argument("--out")
    s.set_defaults(func=cmd_watch_slot)

    s = sub.add_parser("drop-slots")
    s.add_argument("--node", required=True)
    s.set_defaults(func=cmd_drop_slots)

    s = sub.add_parser("activity")
    s.add_argument("--node", required=True)
    s.add_argument("--out")
    s.set_defaults(func=cmd_activity)

    s = sub.add_parser("set")
    s.add_argument("--node", required=True)
    s.add_argument("name")
    s.add_argument("value")
    s.set_defaults(func=cmd_set)

    s = sub.add_parser("reset")
    s.add_argument("--node", required=True)
    s.add_argument("name")
    s.set_defaults(func=cmd_reset)

    s = sub.add_parser("reload")
    s.add_argument("--node", required=True)
    s.set_defaults(func=cmd_reload)

    s = sub.add_parser("checkpoint")
    s.add_argument("--node", required=True)
    s.set_defaults(func=cmd_checkpoint)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
