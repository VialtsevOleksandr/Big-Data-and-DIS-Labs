"""Конфігурація B: налаштування двосторонньої логічної реплікації та її стан.

  python -m notes.multi setup            створити підписки в обидва боки
  python -m notes.multi ids --mode oddeven|naive
  python -m notes.multi status
"""
from __future__ import annotations

import argparse
import sys
import time

from psycopg import sql

from . import config, db

PUBLICATION = "notes_pub"


def subscription_name(subscriber: str, publisher: str) -> str:
    return f"sub_{config.node_name(subscriber)}_from_{config.node_name(publisher)}"


def pairs(hosts: list[str]) -> list[tuple[str, str]]:
    """(підписник, видавець) для кожного напрямку між двома лідерами."""
    if len(hosts) != 2:
        raise SystemExit(f"configuration B needs exactly two nodes, got {hosts}")
    return [(hosts[0], hosts[1]), (hosts[1], hosts[0])]


def cmd_setup(args) -> int:
    hosts = config.parse_hosts(args.nodes)
    created = []
    for subscriber, publisher in pairs(hosts):
        name = subscription_name(subscriber, publisher)
        with db.connect(subscriber, admin=True) as conn:
            if db.scalar(conn, "SELECT count(*) FROM pg_subscription WHERE subname = %s", (name,)):
                created.append({"subscription": name, "on": subscriber, "status": "already exists"})
                continue
            connection = (f"host={publisher} port={config.PORT} dbname={config.DB} "
                          f"user={config.REPL_USER} password={config.REPL_PASSWORD} application_name={name}")
            # CONNECTION     — куди підписник іде по зміни (walsender іншого лідера).
            # PUBLICATION    — який набір таблиць забирати.
            # copy_data=false — початкову копію таблиці не робити: обидва вузли стартують порожніми,
            #                  а копіювання назустріч одне одному продублювало б рядки.
            # origin=none    — просити у видавця лише зміни, які виникли на ньому самому.
            #                  Без цього зміна, отримана з pg2, повернулася б із pg1 назад на pg2 по колу.
            # Команда також створює на видавці логічний слот з іменем підписки.
            conn.execute(sql.SQL(
                "CREATE SUBSCRIPTION {} CONNECTION {} PUBLICATION {} WITH (copy_data = false, origin = none)"
            ).format(sql.Identifier(name), sql.Literal(connection), sql.Identifier(PUBLICATION)))
            created.append({"subscription": name, "on": subscriber, "from": publisher, "status": "created"})

    # Чекаємо, доки обидва apply worker під'єднаються.
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        ready = True
        for host in hosts:
            with db.connect(host, admin=True) as conn:
                if not db.scalar(conn, "SELECT count(*) FROM pg_stat_subscription WHERE pid IS NOT NULL"):
                    ready = False
        if ready:
            break
        time.sleep(0.5)
    db.dump({"subscriptions": created, "apply_workers_running": ready})
    return 0 if ready else 1


def set_id_mode(hosts: list[str], mode: str) -> dict:
    """Налаштовує генератор id на обох лідерах.

    naive   — обидві послідовності йдуть з кроком 1 з одного й того самого значення,
              як після клонування вузла: два лідери видають однакові id.
    oddeven — крок 2: перший лідер видає лише непарні id, другий — лише парні.

    Стан послідовностей логічна реплікація не передає, тому кожен вузол налаштовується окремо.
    """
    conns = [db.connect(h, admin=True) for h in hosts]
    highest = 0
    for conn in conns:
        highest = max(highest, db.scalar(conn, "SELECT coalesce(max(id), 0) FROM notes"))
        highest = max(highest, db.scalar(
            conn, "SELECT coalesce(last_value, 0) FROM pg_sequences "
                  "WHERE schemaname = 'public' AND sequencename = 'notes_id_seq'") or 0)
    result = {"mode": mode, "highest_id_so_far": highest, "nodes": []}
    for index, (host, conn) in enumerate(zip(hosts, conns)):
        if mode == "naive":
            step, start = 1, highest + 1
        else:
            step = 2
            start = highest + 1
            wanted_parity = 1 if index == 0 else 0  # перший вузол — непарні, другий — парні
            if start % 2 != wanted_parity:
                start += 1
        conn.execute(sql.SQL("ALTER TABLE notes ALTER COLUMN id SET INCREMENT BY {} RESTART WITH {}").format(
            sql.Literal(step), sql.Literal(start)))
        result["nodes"].append({"host": host, "increment_by": step, "next_id": start})
        conn.close()
    return result


def cmd_ids(args) -> int:
    db.dump(set_id_mode(config.parse_hosts(args.nodes), args.mode))
    return 0


def node_replication_state(conn) -> dict:
    """Стан підписок, слотів і лічильники конфліктів вузла."""
    return {
        "name": db.scalar(conn, "SHOW cluster_name"),
        "notes": db.scalar(conn, "SELECT count(*) FROM notes"),
        "sequence": db.one(conn, "SELECT increment_by, last_value FROM pg_sequences "
                                 "WHERE schemaname = 'public' AND sequencename = 'notes_id_seq'"),
        "subscriptions": db.rows(conn, """
            SELECT s.subname, s.subenabled, st.pid IS NOT NULL AS worker_running,
                   st.received_lsn, st.latest_end_lsn, st.last_msg_receipt_time
            FROM pg_subscription s LEFT JOIN pg_stat_subscription st ON st.subid = s.oid
            ORDER BY s.subname"""),
        # У PostgreSQL 18 тут є лічильники confl_* для кожного виду конфлікту.
        "subscription_stats": db.rows(conn, "SELECT * FROM pg_stat_subscription_stats ORDER BY subname"),
        "slots_for_other_node": db.rows(conn, """
            SELECT slot_name, active, wal_status, confirmed_flush_lsn,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)::bigint AS not_confirmed_bytes
            FROM pg_replication_slots WHERE slot_type = 'logical' ORDER BY slot_name"""),
    }


def cmd_status(args) -> int:
    result = []
    for host in config.parse_hosts(args.nodes):
        with db.connect(host, admin=True) as conn:
            state = node_replication_state(conn)
            state["host"] = host
            state["track_commit_timestamp"] = db.scalar(conn, "SHOW track_commit_timestamp")
            state["wal_level"] = db.scalar(conn, "SHOW wal_level")
            state["published_tables"] = db.rows(
                conn, "SELECT pubname, schemaname, tablename FROM pg_publication_tables")
            result.append(state)
    db.dump(result, args.out)
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.multi")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("setup")
    s.add_argument("--nodes")
    s.set_defaults(func=cmd_setup)

    s = sub.add_parser("ids")
    s.add_argument("--mode", choices=["naive", "oddeven"], required=True)
    s.add_argument("--nodes")
    s.set_defaults(func=cmd_ids)

    s = sub.add_parser("status")
    s.add_argument("--nodes")
    s.add_argument("--out")
    s.set_defaults(func=cmd_status)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
