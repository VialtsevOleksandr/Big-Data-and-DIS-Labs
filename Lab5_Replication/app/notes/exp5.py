"""Експеримент 5: конфлікти записів між двома лідерами.

Фази (мережу між фазами розриває й відновлює run_exp5.ps1):
  python -m notes.exp5 prepare  --id-mode oddeven --dir /results/exp5/oddeven
  python -m notes.exp5 conflict --dir ...     під час розриву: конфліктні зміни на обох лідерах
  python -m notes.exp5 observe  --dir ...     після відновлення: що фактично сталося
  python -m notes.exp5 resolve  --dir ...     лише якщо реплікація зупинилася з помилкою

Порядок змін під час розриву навмисний: 1) два різні заголовки однієї нотатки,
2) видалення проти зміни тексту, 3) нові нотатки. Якщо нові нотатки отримують однакові id,
реплікація зупиняється на першій з них, і все, що в журналі стоїть після неї, вже не застосується.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from psycopg import sql

from . import config, db
from .admin import checksums
from .multi import node_replication_state, set_id_mode, subscription_name
from .service import NotesService

ROW_SQL = ("SELECT id, title, body, updated_at, pg_xact_commit_timestamp(xmin) AS committed_at "
           "FROM notes WHERE id = %s")


def load_state(directory: Path) -> dict:
    return json.loads((directory / "state.json").read_text(encoding="utf-8"))


def save_state(directory: Path, state: dict) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "state.json").write_text(json.dumps(state, indent=2, default=str), encoding="utf-8")


def wait_for(conn, note_id: int, timeout: float = 15.0) -> float | None:
    """Чекає появи нотатки на вузлі; повертає час очікування в секундах або None."""
    started = time.perf_counter()
    while time.perf_counter() - started < timeout:
        if db.scalar(conn, "SELECT count(*) FROM notes WHERE id = %s", (note_id,)):
            return round(time.perf_counter() - started, 3)
        time.sleep(0.02)
    return None


def cmd_prepare(args) -> int:
    hosts = config.parse_hosts(args.nodes)
    out = Path(args.dir)
    run = f"{out.name}-{time.strftime('%H%M%S')}"
    c1, c2 = (db.connect(h) for h in hosts)
    n1, n2 = config.node_name(hosts[0]), config.node_name(hosts[1])
    s1, s2 = NotesService(c1), NotesService(c2)
    naive = args.id_mode == "naive"

    result = {"run": run, "id_mode": args.id_mode, "ids_setup": set_id_mode(hosts, args.id_mode)}

    # Перевірка: запис на будь-якому лідері з'являється на іншому.
    first = s1.create(1, f"{run}/check written on {n1}", "replication check")
    seen_on_2 = wait_for(c2, first)
    if naive:
        set_id_mode(hosts, "naive")  # вирівняти послідовності, інакше другий лідер видасть той самий id
    second = s2.create(1, f"{run}/check written on {n2}", "replication check")
    seen_on_1 = wait_for(c1, second)
    result["replication_check"] = {
        f"written_on_{n1}": {"note_id": first, f"visible_on_{n2}_after_s": seen_on_2},
        f"written_on_{n2}": {"note_id": second, f"visible_on_{n1}_after_s": seen_on_1},
        "works_both_ways": seen_on_2 is not None and seen_on_1 is not None,
    }
    if naive:
        set_id_mode(hosts, "naive")

    # Нотатки, навколо яких будуть конфлікти.
    title_note = s1.create(1, f"{run}/title target (original)", "both leaders will change the title")
    delete_note = s1.create(1, f"{run}/delete-vs-update target (original)", "original body")
    marker_note = s1.create(1, f"{run}/partition marker", "original body")
    replicated = all(wait_for(c2, note) is not None for note in (title_note, delete_note, marker_note))
    if naive:
        result["ids_before_partition"] = set_id_mode(hosts, "naive")

    state = {"run": run, "id_mode": args.id_mode, "hosts": hosts, "names": [n1, n2],
             "title_note": title_note, "delete_note": delete_note, "marker_note": marker_note}
    save_state(out, state)
    result["targets"] = {"title_note": title_note, "delete_note": delete_note,
                         "marker_note": marker_note, "replicated_to_both": replicated}
    c1.close()
    c2.close()
    db.dump(result, out / "prepare.json")
    return 0 if result["replication_check"]["works_both_ways"] and replicated else 1


def cmd_conflict(args) -> int:
    out = Path(args.dir)
    state = load_state(out)
    hosts = state["hosts"]
    n1, n2 = state["names"]
    run = state["run"]
    c1, c2 = (db.connect(h) for h in hosts)
    s1, s2 = NotesService(c1), NotesService(c2)
    result = {"run": run, "at": round(time.time(), 3)}

    # Переконуємося, що зв'язку справді немає: зміна на першому лідері не доходить до другого.
    s1.update_body(state["marker_note"], f"changed on {n1} during the partition")
    time.sleep(args.partition_check_seconds)
    marker_on_2 = s2.get(state["marker_note"])
    result["partition_confirmed"] = marker_on_2 is not None and marker_on_2["body"] == "original body"

    # Випадок 1: обидва лідери змінюють заголовок однієї нотатки на різні значення.
    title_1 = f"{run}/title set on {n1}"
    title_2 = f"{run}/title set on {n2}"
    s1.update_title(state["title_note"], title_1)
    time.sleep(1.0)  # зміна на другому лідері свідомо пізніша: за last-write-wins перемогла б вона
    s2.update_title(state["title_note"], title_2)
    result["case1_title_vs_title"] = {
        "note_id": state["title_note"],
        n1: db.one(c1, ROW_SQL, (state["title_note"],)),
        n2: db.one(c2, ROW_SQL, (state["title_note"],)),
    }

    # Випадок 3 із завдання: один лідер видаляє нотатку, інший змінює її текст.
    body_2 = f"body changed on {n2} while {n1} deleted the note"
    deleted = s1.delete(state["delete_note"])
    updated = s2.update_body(state["delete_note"], body_2)
    result["case3_delete_vs_update"] = {
        "note_id": state["delete_note"],
        f"deleted_on_{n1}": deleted,
        f"updated_on_{n2}": updated,
        f"{n2}_row": db.one(c2, ROW_SQL, (state["delete_note"],)),
    }

    # Випадок 2 із завдання: обидва лідери створюють нові нотатки, id генерує база.
    created = []
    for k in range(1, args.inserts + 1):
        for name, service in ((n1, s1), (n2, s2)):
            title = f"{run}/new on {name} #{k}"
            created.append({"node": name, "note_id": service.create(1, title, f"created on {name}"),
                            "title": title})
    ids_1 = [c["note_id"] for c in created if c["node"] == n1]
    ids_2 = [c["note_id"] for c in created if c["node"] == n2]
    result["case2_inserts"] = {
        f"ids_from_{n1}": ids_1, f"ids_from_{n2}": ids_2,
        "same_id_issued_by_both": sorted(set(ids_1) & set(ids_2)),
    }

    state.update({"title_1": title_1, "title_2": title_2, "body_2": body_2, "created": created})
    save_state(out, state)
    c1.close()
    c2.close()
    db.dump(result, out / "conflict.json")
    return 0


def all_rows(conn) -> dict[int, dict]:
    return {r["id"]: r for r in db.rows(conn, "SELECT id, title, body, updated_at FROM notes")}


def cmd_observe(args) -> int:
    out = Path(args.dir)
    state = load_state(out)
    hosts = state["hosts"]
    n1, n2 = state["names"]

    # Даємо вузлам час обмінятися змінами: чекаємо збігу контрольних сум, але не довше --settle.
    deadline = time.monotonic() + args.settle
    waited_from = time.monotonic()
    while True:
        sums = checksums(hosts)
        equal = len({(s.get("notes"), s.get("checksum")) for s in sums}) == 1
        if equal or time.monotonic() >= deadline:
            break
        time.sleep(1.0)

    c1, c2 = (db.connect(h, admin=True) for h in hosts)
    rows_1, rows_2 = all_rows(c1), all_rows(c2)
    result = {"run": state["run"], "id_mode": state["id_mode"],
              "waited_s": round(time.monotonic() - waited_from, 1),
              "nodes_have_identical_data": equal, "checksums": sums}

    # Випадок 1.
    t1 = rows_1.get(state["title_note"], {}).get("title")
    t2 = rows_2.get(state["title_note"], {}).get("title")
    if t1 == t2:
        winner = n1 if t1 == state["title_1"] else n2 if t1 == state["title_2"] else "neither"
        verdict = f"converged: the value written on {winner} is on both nodes, the other one is lost"
    elif t1 == state["title_2"] and t2 == state["title_1"]:
        verdict = "diverged: each node now holds the title written on the OTHER node (values swapped)"
    else:
        verdict = "diverged: each node kept a different title"
    result["case1_title_vs_title"] = {"note_id": state["title_note"], f"title_on_{n1}": t1,
                                      f"title_on_{n2}": t2, "verdict": verdict}

    # Випадок 3.
    on_1 = rows_1.get(state["delete_note"])
    on_2 = rows_2.get(state["delete_note"])
    if on_1 is None and on_2 is None:
        verdict = (f"converged: the note is deleted on both nodes; "
                   f"the text change made on {n2} is lost")
    elif on_1 is not None and on_2 is not None and on_1["body"] == on_2["body"]:
        verdict = "converged: the note exists on both nodes, the delete is lost"
    else:
        verdict = (f"diverged: note is {'present' if on_1 else 'absent'} on {n1} "
                   f"and {'present' if on_2 else 'absent'} on {n2}")
    result["case3_delete_vs_update"] = {
        "note_id": state["delete_note"],
        f"on_{n1}": None if on_1 is None else on_1["body"],
        f"on_{n2}": None if on_2 is None else on_2["body"],
        "verdict": verdict,
    }

    # Випадок 2.
    replicated = missing = collisions = 0
    examples = []
    for note in state.get("created", []):
        other = rows_2 if note["node"] == n1 else rows_1
        found = other.get(note["note_id"])
        if found is None:
            missing += 1
        elif found["title"] == note["title"]:
            replicated += 1
        else:
            collisions += 1
            if len(examples) < 3:
                examples.append({"note_id": note["note_id"], "created_on": note["node"],
                                 "title_here": note["title"], "title_on_other_node": found["title"]})
    total = len(state.get("created", []))
    if replicated == total:
        verdict = "all new notes reached the other node"
    elif collisions:
        verdict = ("both leaders issued the same ids: the incoming rows hit the primary key, "
                   "replication stopped with an error and the nodes keep different notes under one id")
    else:
        verdict = "some new notes did not reach the other node"
    result["case2_inserts"] = {"created": total, "replicated": replicated, "not_replicated": missing,
                               "same_id_different_note": collisions, "examples": examples,
                               "verdict": verdict}

    # Чим відрізняються вузли в цілому.
    differing = sorted(i for i in set(rows_1) | set(rows_2)
                       if (rows_1.get(i) or {}).get("title") != (rows_2.get(i) or {}).get("title")
                       or (rows_1.get(i) or {}).get("body") != (rows_2.get(i) or {}).get("body"))
    result["differing_note_ids"] = {"count": len(differing), "first": differing[:20]}
    result[f"replication_state_{n1}"] = node_replication_state(c1)
    result[f"replication_state_{n2}"] = node_replication_state(c2)
    c1.close()
    c2.close()
    db.dump(result, out / (args.name + ".json"))
    return 0


def cmd_resolve(args) -> int:
    """Знімає зупинку реплікації, пропускаючи все, що видавець накопичив для підписника.

    На підписнику: вимкнути підписку, пересунути її позицію (replication origin) на поточний
    кінець WAL видавця, увімкнути знову. Пропущені зміни на цей вузол уже не потраплять,
    тому вузли лишаються з різними даними: це ручне «кожен лишається при своєму».
    """
    state = load_state(Path(args.dir))
    hosts = state["hosts"]
    result = []
    for subscriber, publisher in ((hosts[0], hosts[1]), (hosts[1], hosts[0])):
        name = subscription_name(subscriber, publisher)
        with db.connect(publisher, admin=True) as pub, db.connect(subscriber, admin=True) as sub:
            skip_to = db.scalar(pub, "SELECT pg_current_wal_lsn()")
            sub.execute(sql.SQL("ALTER SUBSCRIPTION {} DISABLE").format(sql.Identifier(name)))
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if not db.scalar(sub, "SELECT count(*) FROM pg_stat_subscription "
                                      "WHERE subname = %s AND pid IS NOT NULL", (name,)):
                    break
                time.sleep(0.2)
            time.sleep(0.5)
            sub.execute("SELECT pg_replication_origin_advance('pg_' || oid, %s::pg_lsn) "
                        "FROM pg_subscription WHERE subname = %s", (skip_to, name))
            sub.execute(sql.SQL("ALTER SUBSCRIPTION {} ENABLE").format(sql.Identifier(name)))
            result.append({"subscription": name, "on": subscriber, "skipped_up_to_publisher_lsn": skip_to})
    db.dump({"resolved": result}, Path(args.dir) / "resolve.json")
    return 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.exp5")
    sub = p.add_subparsers(dest="command", required=True)

    s = sub.add_parser("prepare")
    s.add_argument("--dir", required=True)
    s.add_argument("--id-mode", choices=["naive", "oddeven"], default="oddeven")
    s.add_argument("--nodes")
    s.set_defaults(func=cmd_prepare)

    s = sub.add_parser("conflict")
    s.add_argument("--dir", required=True)
    s.add_argument("--inserts", type=int, default=20, help="new notes per leader")
    s.add_argument("--partition-check-seconds", type=float, default=3.0)
    s.set_defaults(func=cmd_conflict)

    s = sub.add_parser("observe")
    s.add_argument("--dir", required=True)
    s.add_argument("--settle", type=float, default=60.0, help="how long to wait for the nodes to converge")
    s.add_argument("--name", default="observe", help="result file name without extension")
    s.set_defaults(func=cmd_observe)

    s = sub.add_parser("resolve")
    s.add_argument("--dir", required=True)
    s.set_defaults(func=cmd_resolve)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
