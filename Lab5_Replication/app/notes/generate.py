"""Генератор початкових даних для сервісу нотаток.

Три способи роботи:
  1. Згенерувати CSV на своїй машині (Docker не потрібен, лише Python 3.10+):
       python generate_data.py --notes 100000 --seed 42 --out data
     -> data/notes_seed42_100000.csv і поруч data/notes_seed42_100000.json (параметри й контрольна сума)
  2. Завантажити готовий файл у базу (у контейнері застосунку папка data/ змонтована як /data):
       python -m notes.generate --load /data/notes_seed42_100000.csv --host repa-pg1
     або одразу під час розгортання:  .\\setup_single.ps1 -DataFile data\\notes_seed42_100000.csv
  3. Згенерувати й одразу записати в базу, без файлу:
       python -m notes.generate --notes 100000 --seed 42 --host repa-pg1

Інші приклади:
  python generate_data.py --target-mib 64 --seed 7 --body-chars 300-1200 --out data
  python generate_data.py --notes 5000 --dry-run --sample 3
  python generate_data.py --load data/notes_seed42_100000.csv --dry-run     (лише перевірити файл)

Властивості:
  * той самий seed і ті самі параметри тексту дають ті самі нотатки; більший --notes
    з тим самим seed лише дописує нові нотатки після вже відомих;
  * для кожного набору рахується SHA-256; файл із --out має поруч .json із цією сумою,
    і --load перед завантаженням перевіряє, що файл не змінився;
  * у базу генератор пише лише на лідера і лише в порожню таблицю (якщо не сказано інакше).
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from . import config, textgen

MIB = 1048576
CSV_FIELDS = ["author_id", "title", "body", "updated_at"]


def parse_range(text: str, name: str) -> tuple[int, int]:
    """'120-600' -> (120, 600); '300' -> (300, 300)."""
    try:
        parts = [int(p) for p in text.split("-")]
    except ValueError:
        raise SystemExit(f"--{name}: expected MIN-MAX or a single number, got '{text}'")
    if len(parts) == 1:
        parts = [parts[0], parts[0]]
    if len(parts) != 2 or parts[0] < 1 or parts[1] < parts[0]:
        raise SystemExit(f"--{name}: expected 1 <= MIN <= MAX, got '{text}'")
    return parts[0], parts[1]


def note_rows(seed: int, authors: int, skew: float, title_words: tuple[int, int],
              body_chars: tuple[int, int], base: datetime, days: int):
    """Нескінченний детермінований потік нотаток: (author_id, title, body, updated_at у форматі ISO 8601)."""
    rng = random.Random(seed)
    span_ms = max(1, days * 86400 * 1000)
    while True:
        # skew = 0: автори рівномірні; skew > 0: у небагатьох авторів багато нотаток.
        author_id = min(authors, int(authors * rng.random() ** (1.0 + skew)) + 1)
        title = textgen.title(rng, *title_words)
        body = textgen.body(rng, *body_chars)
        updated_at = base - timedelta(milliseconds=rng.randrange(span_ms))
        yield author_id, title, body, updated_at.isoformat()


def row_bytes(row) -> int:
    """Обсяг тексту нотатки в байтах UTF-8 (без службових заголовків рядка PostgreSQL)."""
    return len(row[1].encode("utf-8")) + len(row[2].encode("utf-8"))


def digest_line(row) -> bytes:
    return f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\n".encode("utf-8")


# ---------- Файли ----------

def resolve_out(value: str, seed: int, total: int) -> Path:
    """Папка (або шлях без розширення) -> <папка>/notes_seed<seed>_<n>.csv; інакше — шлях як є."""
    path = Path(value)
    if value.endswith(("/", "\\")) or path.is_dir() or not path.suffix:
        path = path / f"notes_seed{seed}_{total}.csv"
    return path


def manifest_for(csv_path: Path) -> Path:
    return csv_path.with_suffix(".json")


def read_csv(path: Path):
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if header != CSV_FIELDS:
            raise SystemExit(f"{path}: expected header {','.join(CSV_FIELDS)}, got {header}")
        for line_no, row in enumerate(reader, start=2):
            if len(row) != len(CSV_FIELDS):
                raise SystemExit(f"{path}, line {line_no}: expected {len(CSV_FIELDS)} columns, got {len(row)}")
            try:
                author_id = int(row[0])
            except ValueError:
                raise SystemExit(f"{path}, line {line_no}: author_id is not a number: {row[0]!r}")
            yield author_id, row[1], row[2], row[3]


def scan_file(path: Path) -> tuple[int, str]:
    """Кількість рядків і SHA-256 набору у файлі (рахується так само, як під час генерації)."""
    digest = hashlib.sha256()
    count = 0
    for row in read_csv(path):
        digest.update(digest_line(row))
        count += 1
    return count, digest.hexdigest()


# ---------- База ----------

class DbTarget:
    """Таблиця notes на лідері, куди нотатки пишуться пакетами COPY."""

    def __init__(self, host: str, if_not_empty: str):
        from . import db  # імпорт тут: генерація у файл працює без psycopg

        self.db = db
        self.host = host
        self.conn = db.connect(host)
        info = db.one(self.conn, "SELECT pg_is_in_recovery() AS in_recovery, "
                                 "current_setting('cluster_name') AS name")
        if info["in_recovery"]:
            raise SystemExit(f"{host} ({info['name']}) is a follower and is read-only: point --host at the leader")
        self.name = info["name"]
        self.before = db.scalar(self.conn, "SELECT count(*) FROM notes")
        if self.before and if_not_empty == "abort":
            raise SystemExit(f"table notes on {host} already has {self.before} rows. "
                             f"Use --if-not-empty append or --if-not-empty truncate")
        if self.before and if_not_empty == "truncate":
            print(f"truncating {self.before} existing rows on {host}", flush=True)
            self.conn.execute("TRUNCATE notes")
            self.before = 0
        self.start_lsn = db.scalar(self.conn, "SELECT pg_current_wal_lsn()")
        self.cursor = self.conn.cursor()

    def write(self, batch) -> None:
        # Один COPY = одна транзакція: лідер передає дані фоловерам порціями, а не одним шматком.
        with self.cursor.copy("COPY notes (author_id, title, body, updated_at) FROM STDIN") as copy:
            for row in batch:
                copy.write_row(row)

    def finish(self, written: int) -> dict:
        after = self.db.one(self.conn, """
            SELECT count(*) AS notes, min(id) AS min_id, max(id) AS max_id,
                   pg_total_relation_size('notes') AS relation_bytes,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), %s::pg_lsn)::bigint AS wal_bytes
            FROM notes""", (self.start_lsn,))
        self.conn.close()
        if after["notes"] != self.before + written:
            print(f"WARNING: expected {self.before + written} rows, table has {after['notes']}", file=sys.stderr)
        return {
            "host": self.host,
            "rows_before": self.before,
            "rows_after": after["notes"],
            "min_id": after["min_id"],
            "max_id": after["max_id"],
            "table_with_indexes_mib": self.db.mib(after["relation_bytes"]),
            "wal_generated_mib": self.db.mib(after["wal_bytes"]),
        }


# ---------- Спільний цикл ----------

def write_all(rows, total: int | None, batch_size: int, target: DbTarget | None = None,
              writer=None, sample: int = 0) -> dict:
    """Проганяє потік нотаток пакетами: рахує статистику й суму, пише у файл та/або в базу."""
    digest = hashlib.sha256()
    text_bytes = 0
    authors = set()
    samples = []
    done = 0
    started = time.perf_counter()
    last_report = started
    reported = -1

    def progress() -> None:
        nonlocal reported
        if done == reported:
            return
        reported = done
        rate = done / max(time.perf_counter() - started, 1e-9)
        print(f"  {done}{'' if total is None else f'/{total}'} notes, {rate:,.0f} notes/s", flush=True)

    while total is None or done < total:
        size = batch_size if total is None else min(batch_size, total - done)
        batch = list(itertools.islice(rows, size))
        if not batch:
            break
        for row in batch:
            digest.update(digest_line(row))
            text_bytes += row_bytes(row)
            authors.add(row[0])
            if len(samples) < sample:
                samples.append(row)
        if writer is not None:
            writer.writerows(batch)
        if target is not None:
            target.write(batch)
        done += len(batch)
        if time.perf_counter() - last_report >= 2.0:
            progress()
            last_report = time.perf_counter()
    progress()

    for row in samples:
        print(json.dumps({"author_id": row[0], "title": row[1], "body": row[2][:80] + "...",
                          "updated_at": row[3]}, ensure_ascii=False), flush=True)
    return {
        "notes": done,
        "authors_used": len(authors),
        "text_mib": round(text_bytes / MIB, 3),
        "avg_note_text_bytes": round(text_bytes / done, 1) if done else 0,
        "dataset_sha256": digest.hexdigest(),
        "seconds": round(time.perf_counter() - started, 3),
    }


# ---------- Режими ----------

def generate(args) -> dict:
    title_words = parse_range(args.title_words, "title-words")
    body_chars = parse_range(args.body_chars, "body-chars")
    try:
        base = datetime.fromisoformat(args.base_date).replace(tzinfo=timezone.utc)
    except ValueError:
        raise SystemExit(f"--base-date: expected YYYY-MM-DD, got '{args.base_date}'")

    def stream():
        return note_rows(args.seed, args.authors, args.author_skew, title_words, body_chars, base, args.days)

    if args.target_mib is not None:
        probe = stream()
        sample_n = 2000
        avg = sum(row_bytes(next(probe)) for _ in range(sample_n)) / sample_n
        total = max(1, math.ceil(args.target_mib * MIB / avg))
        print(f"target {args.target_mib} MiB of text, average note {avg:.0f} bytes -> {total} notes", flush=True)
    else:
        total = args.notes if args.notes is not None else 100000

    params = {
        "seed": args.seed,
        "authors_requested": args.authors,
        "author_skew": args.author_skew,
        "title_words": list(title_words),
        "body_chars": list(body_chars),
        "base_date": args.base_date,
        "days": args.days,
    }

    out_path = None
    target = None
    if args.out:
        out_path = resolve_out(args.out, args.seed, total)
        if out_path.exists() and not args.force:
            raise SystemExit(f"{out_path} already exists: choose another --out or add --force to overwrite")
        out_path.parent.mkdir(parents=True, exist_ok=True)
        print(f"writing {total} notes to {out_path}, seed {args.seed}", flush=True)
        with out_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(CSV_FIELDS)
            stats = write_all(stream(), total, args.batch, writer=writer, sample=args.sample)
    else:
        if not args.dry_run:
            target = DbTarget(args.host or config.default_leader(), args.if_not_empty)
            print(f"writing {total} notes to {target.host} ({target.name}), seed {args.seed}", flush=True)
        else:
            print(f"dry run: {total} notes, seed {args.seed}, nothing is written", flush=True)
        stats = write_all(stream(), total, args.batch, target=target, sample=args.sample)

    summary = {**params, "notes_generated": stats.pop("notes"), **stats, "dry_run": args.dry_run}
    if out_path is not None:
        summary["file"] = out_path.as_posix()
        summary["file_mib"] = round(out_path.stat().st_size / MIB, 3)
        manifest_for(out_path).write_text(json.dumps(summary, indent=2, ensure_ascii=True) + "\n",
                                          encoding="utf-8")
        print(f"manifest: {manifest_for(out_path)}", flush=True)
    if target is not None:
        summary.update(target.finish(summary["notes_generated"]))
    return summary


def load(args) -> dict:
    path = Path(args.load)
    if not path.is_file():
        raise SystemExit(f"file not found: {path} (inside the container the lab's data folder is /data)")
    mpath = manifest_for(path)
    manifest = json.loads(mpath.read_text(encoding="utf-8")) if mpath.is_file() else None

    print(f"checking {path}", flush=True)
    count, sha = scan_file(path)
    check = {"source_file": path.as_posix(), "rows_in_file": count, "file_sha256": sha}
    if manifest is not None:
        same = count == manifest.get("notes_generated") and sha == manifest.get("dataset_sha256")
        check.update({"manifest": mpath.as_posix(), "seed": manifest.get("seed"), "matches_manifest": same})
        if not same:
            message = (f"{path} does not match {mpath.name}: file has {count} rows, sha {sha[:12]}; "
                       f"manifest says {manifest.get('notes_generated')} rows, "
                       f"sha {str(manifest.get('dataset_sha256'))[:12]}")
            if not args.force:
                raise SystemExit(message + ". Regenerate the file or add --force")
            print("WARNING: " + message, file=sys.stderr)
        else:
            print(f"  file matches {mpath.name}: {count} rows, seed {manifest.get('seed')}", flush=True)
    else:
        check["matches_manifest"] = None
        print(f"  no {mpath.name} next to the file, nothing to compare with", flush=True)

    if args.dry_run:
        return {**check, "dry_run": True}

    target = DbTarget(args.host or config.default_leader(), args.if_not_empty)
    print(f"loading {count} notes into {target.host} ({target.name})", flush=True)
    stats = write_all(read_csv(path), count, args.batch, target=target, sample=args.sample)
    summary = {**check, "notes_loaded": stats["notes"], "seconds": stats["seconds"], "dry_run": False}
    summary.update(target.finish(stats["notes"]))
    return summary


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="generate_data.py / python -m notes.generate",
        description="Deterministic data generator for the notes service: to a CSV file, "
                    "from a CSV file into the database, or straight into the database.",
    )
    size = p.add_mutually_exclusive_group()
    size.add_argument("--notes", type=int, help="number of notes to generate (default 100000)")
    size.add_argument("--target-mib", type=float,
                      help="generate as many notes as needed for about this many MiB of note text")
    p.add_argument("--seed", type=int, default=42, help="random seed (default 42)")
    p.add_argument("--authors", type=int, default=1000, help="number of distinct authors (default 1000)")
    p.add_argument("--author-skew", type=float, default=1.0,
                   help="0 = notes spread evenly, larger = few authors own most notes (default 1.0)")
    p.add_argument("--title-words", default="2-7", help="words per title, MIN-MAX (default 2-7)")
    p.add_argument("--body-chars", default="120-600", help="characters per body, MIN-MAX (default 120-600)")
    p.add_argument("--days", type=int, default=365,
                   help="updated_at is spread over this many days before --base-date (default 365)")
    p.add_argument("--base-date", default="2026-01-01",
                   help="latest possible updated_at, YYYY-MM-DD, fixed so that data is reproducible")

    p.add_argument("--out", metavar="PATH",
                   help="write a CSV file instead of the database: a file name, or a folder "
                        "(then the name is notes_seed<seed>_<n>.csv); a .json manifest is written next to it")
    p.add_argument("--load", metavar="CSV",
                   help="load notes from a CSV made with --out instead of generating them")
    p.add_argument("--force", action="store_true",
                   help="overwrite an existing --out file; load a file that does not match its manifest")

    p.add_argument("--host", help="node to write to (default: first node of NOTES_NODES)")
    p.add_argument("--if-not-empty", choices=["abort", "append", "truncate"], default="abort",
                   help="what to do when the table already has notes (default abort)")
    p.add_argument("--batch", type=int, default=10000, help="rows per COPY transaction (default 10000)")
    p.add_argument("--dry-run", action="store_true",
                   help="touch neither the database nor files: generate and report, or with --load only check the file")
    p.add_argument("--sample", type=int, default=0, help="print this many notes")
    p.add_argument("--manifest", help="also write the run summary (JSON) to this file")
    return p


def validate(args) -> None:
    if args.notes is not None and args.notes < 1:
        raise SystemExit("--notes must be at least 1")
    if args.target_mib is not None and args.target_mib <= 0:
        raise SystemExit("--target-mib must be positive")
    if args.authors < 1:
        raise SystemExit("--authors must be at least 1")
    if args.author_skew < 0:
        raise SystemExit("--author-skew must be 0 or more")
    if args.days < 1:
        raise SystemExit("--days must be at least 1")
    if args.batch < 1:
        raise SystemExit("--batch must be at least 1")
    if args.load and (args.notes is not None or args.target_mib is not None or args.out):
        raise SystemExit("--load takes the notes from the file: drop --notes, --target-mib and --out")
    if args.out and args.dry_run:
        raise SystemExit("--out already writes only a file; drop --dry-run")
    if args.out and args.host:
        raise SystemExit("--out writes a file and does not touch the database; drop --host")


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    validate(args)
    summary = load(args) if args.load else generate(args)
    text = json.dumps(summary, indent=2, ensure_ascii=True)
    print(text, flush=True)
    if args.manifest:
        path = Path(args.manifest)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
