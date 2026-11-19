"""Звірка журналу застосунку з тим, що фактично є на кожному вузлі.

  python -m notes.verify --journal /results/exp4/run/journal.csv --nodes repa-pg2,repa-pg3

Запис із журналу вважається наявним на вузлі, якщо там є нотатка з тим самим заголовком
"<run_id>/<seq>" і тим самим id. Звіряти лише за id не можна: після failover новий лідер
може видати id утраченої нотатки вже іншій нотатці.

Для кожного вузла:
  acked_present / acked_missing    — підтверджені базою записи (status = ok);
  unacked_present                  — записи без підтвердження (status = error), які все ж збереглися;
  missing_by_acking_node           — хто підтвердив записи, яких тут немає.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import psycopg

from . import config, db
from .journal import read_journal


def load_node_rows(host: str, run_ids: set[str], acked_ids: list[int]) -> tuple[dict[str, int], dict[int, str]]:
    """(title -> id для нотаток запусків із журналу, id -> title для всіх нотаток вузла з id із журналу)."""
    found: dict[str, int] = {}
    with db.connect(host, admin=True, connect_timeout=3) as conn:
        for run_id in sorted(run_ids):
            for row in db.rows(conn, "SELECT id, title FROM notes WHERE title LIKE %s", (run_id + "/%",)):
                found[row["title"]] = row["id"]
        by_id = {row["id"]: row["title"] for row in db.rows(
            conn, "SELECT id, title FROM notes WHERE id = ANY(%s)", (acked_ids,))}
    return found, by_id


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.verify")
    p.add_argument("--journal", required=True, help="journal.csv (several files separated by commas)")
    p.add_argument("--nodes", help="nodes to check (default NOTES_NODES)")
    p.add_argument("--out", help="write the result (JSON) here")
    args = p.parse_args(argv)

    journal = []
    for path in args.journal.split(","):
        journal.extend(read_journal(path.strip()))
    acked = [r for r in journal if r["status"] == "ok"]
    unacked = [r for r in journal if r["status"] == "error"]
    run_ids = {r["title"].split("/", 1)[0] for r in journal if r["title"]}

    acked_by_node: dict[str, int] = {}
    for r in acked:
        acked_by_node[r["node"]] = acked_by_node.get(r["node"], 0) + 1

    result = {
        "journal": args.journal,
        "attempts": len(journal),
        "acked": len(acked),
        "acked_by_node": acked_by_node,
        "unacked": len(unacked),
        "no_leader": sum(1 for r in journal if r["status"] == "no_leader"),
        "nodes": [],
    }

    out_dir = Path(args.out).parent if args.out else None
    for host in config.parse_hosts(args.nodes):
        name = config.node_name(host)
        try:
            node_rows, by_id = load_node_rows(host, run_ids, [r["note_id"] for r in acked])
        except psycopg.OperationalError as exc:
            result["nodes"].append({"host": host, "reachable": False,
                                    "error": str(exc).strip().splitlines()[-1]})
            continue

        missing = [r for r in acked if node_rows.get(r["title"]) != r["note_id"]]
        missing_by_node: dict[str, int] = {}
        for r in missing:
            missing_by_node[r["node"]] = missing_by_node.get(r["node"], 0) + 1
        # Той самий id на вузлі зайнятий іншою нотаткою: ознака того, що історії розійшлися.
        reused = [r for r in missing if r["note_id"] in by_id]
        id_reused = len(reused)
        unacked_present = [r for r in unacked if r["title"] in node_rows]
        journal_titles = {r["title"] for r in journal}
        unknown_rows = [t for t in node_rows if t not in journal_titles]

        entry = {
            "host": host, "name": name, "reachable": True,
            "rows_of_these_runs_on_node": len(node_rows),
            "acked_present": len(acked) - len(missing),
            "acked_missing": len(missing),
            "missing_by_acking_node": missing_by_node,
            "missing_whose_id_is_reused_by_another_note": id_reused,
            "reused_ids": [{"note_id": r["note_id"], "acked_title": r["title"],
                            "title_on_node": by_id[r["note_id"]]} for r in reused[:10]],
            "first_missing": [{"note_id": r["note_id"], "title": r["title"], "acked_by": r["node"],
                               "acked_at": r["acked_at"]} for r in missing[:10]],
            "unacked_present": len(unacked_present),
            "rows_on_node_absent_from_journal": len(unknown_rows),
        }
        if missing and out_dir:
            listing = out_dir / f"missing_on_{name}.csv"
            with listing.open("w", encoding="utf-8") as handle:
                handle.write("note_id,title,acked_by,acked_at\n")
                for r in missing:
                    handle.write(f"{r['note_id']},{r['title']},{r['node']},{r['acked_at']}\n")
            entry["missing_list"] = str(listing)
        result["nodes"].append(entry)

    db.dump(result, args.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
