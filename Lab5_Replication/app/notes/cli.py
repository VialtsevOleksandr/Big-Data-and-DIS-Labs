"""Ручна робота з нотатками: операції сервісу з командного рядка.

  python -m notes.cli create --author 7 --title "Buy milk" --body "2 l"
  python -m notes.cli get 123 --node repa-pg2
  python -m notes.cli set-title 123 "New title"
  python -m notes.cli set-body 123 "New text"
  python -m notes.cli delete 123
  python -m notes.cli list --author 7 --node repa-pg3

--node — вузол, до якого йде запит (за замовчуванням перший із NOTES_NODES, тобто лідер).
"""
from __future__ import annotations

import argparse
import sys

from . import config, db
from .service import NotesService


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m notes.cli")
    sub = p.add_subparsers(dest="command", required=True)

    def add(name: str) -> argparse.ArgumentParser:
        s = sub.add_parser(name)
        s.add_argument("--node", help="node to talk to (default: first of NOTES_NODES)")
        return s

    s = add("create")
    s.add_argument("--author", type=int, required=True)
    s.add_argument("--title", required=True)
    s.add_argument("--body", default="")
    s = add("get")
    s.add_argument("id", type=int)
    s = add("set-title")
    s.add_argument("id", type=int)
    s.add_argument("title")
    s = add("set-body")
    s.add_argument("id", type=int)
    s.add_argument("body")
    s = add("delete")
    s.add_argument("id", type=int)
    s = add("list")
    s.add_argument("--author", type=int, required=True)
    s.add_argument("--limit", type=int, default=20)

    args = p.parse_args(argv)
    with db.connect(args.node or config.default_leader()) as conn:
        service = NotesService(conn)
        node = db.scalar(conn, "SHOW cluster_name")
        if args.command == "create":
            result = {"created_id": service.create(args.author, args.title, args.body)}
        elif args.command == "get":
            result = {"note": service.get(args.id)}
        elif args.command == "set-title":
            result = {"changed": service.update_title(args.id, args.title)}
        elif args.command == "set-body":
            result = {"changed": service.update_body(args.id, args.body)}
        elif args.command == "delete":
            result = {"deleted": service.delete(args.id)}
        else:
            result = {"notes": service.list_by_author(args.author, args.limit)}
        db.dump({"node": node, **result})
    return 0


if __name__ == "__main__":
    sys.exit(main())
