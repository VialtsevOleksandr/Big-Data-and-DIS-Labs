"""Потоковий валідатор інваріантів результату генерації.

Пам'ять: довідники (≤ десятків тисяч рядків) читаються цілком; чеки й позиції —
потоково, злиттям двох упорядкованих потоків (receipts × receipt_items),
у пам'яті одночасно лише один чек.
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

from .config import INTEGER_MAX, MAX_LINES_PER_RECEIPT, SMALLINT_MAX
from .money import MAX_PRICE_KOP, MAX_QTY_MILLI, parse_fixed
from .schema import header

TS_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\+00:00$")
NUM14_2_MAX = 99_999_999_999_999       # NUMERIC(14,2) у копійках
NUM10_2_MAX = 9_999_999_999            # NUMERIC(10,2)
NUM10_3_MAX = 9_999_999_999            # NUMERIC(10,3)
UNITS_EXPECTED = [("1", "pcs", "штука", "0"), ("2", "kg", "кілограм", "3"), ("3", "l", "літр", "3")]
EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)


class Errors:
    def __init__(self, limit_examples: int = 5):
        self.counts: Counter = Counter()
        self.examples: dict[str, list[str]] = {}
        self.limit = limit_examples

    def add(self, kind: str, detail: str) -> None:
        self.counts[kind] += 1
        ex = self.examples.setdefault(kind, [])
        if len(ex) < self.limit:
            ex.append(detail)

    def check(self, cond: bool, kind: str, detail: str) -> bool:
        if not cond:
            self.add(kind, detail)
        return cond

    @property
    def ok(self) -> bool:
        return not self.counts


def _int(s: str, lo: int, hi: int) -> int:
    if not s.isascii() or not s.isdigit() or (len(s) > 1 and s[0] == "0"):
        raise ValueError(f"не ціле: {s!r}")
    v = int(s)
    if not lo <= v <= hi:
        raise ValueError(f"поза діапазоном [{lo}, {hi}]: {v}")
    return v


def _text(s: str, maxlen: int | None, nullable: bool) -> str | None:
    if s == "":
        if not nullable:
            raise ValueError("порожнє обов'язкове поле")
        return None
    if s.strip() != s:
        raise ValueError(f"пробіли на краях: {s!r}")
    if maxlen is not None and len(s) > maxlen:
        raise ValueError(f"довжина {len(s)} > {maxlen}")
    return s


def _read_small(path: Path, table: str, err: Errors) -> list[list[str]]:
    raw = path.read_bytes()
    err.check(not raw.startswith(b"\xef\xbb\xbf"), "bom", str(path))
    text = raw.decode("utf-8")
    err.check("\r" not in text, "crlf", str(path))
    rows = list(csv.reader(text.splitlines(keepends=False)))
    err.check(rows and ",".join(rows[0]) == header(table), "header", f"{path}: {rows[0] if rows else None}")
    return rows[1:]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _stream(paths: list[Path], table: str, err: Errors, per_file: Counter):
    """Рядки великих таблиць (без текстових полів, без лапок) — поточково."""
    for path in paths:
        with open(path, "rb") as fb:
            err.check(fb.read(3) != b"\xef\xbb\xbf", "bom", str(path))
        with open(path, "r", encoding="utf-8", newline="") as f:
            h = f.readline()
            err.check(h == header(table) + "\n", "header", f"{path}: {h!r}")
            for line in f:
                if not line.endswith("\n") or line.endswith("\r\n") or '"' in line:
                    err.add("line_format", f"{path.name}: {line!r}")
                per_file[path] += 1
                yield path, line.rstrip("\r\n").split(",")


def validate(out_dir: str | Path, check_hashes: bool = True, log=print) -> tuple[Errors, dict]:
    out = Path(out_dir)
    m = json.loads((out / "manifest.json").read_text(encoding="utf-8"))["data"]
    cfg = m["config"]
    err = Errors()
    files = m["files"]
    by_table: dict[str, list[dict]] = {}
    for f in files:
        by_table.setdefault(f["table"], []).append(f)

    # --- файли, хеші, кількість рядків ---
    for f in files:
        p = out / f["path"]
        if not err.check(p.exists(), "missing_file", f["path"]):
            continue
        if check_hashes:
            err.check(p.stat().st_size == f["bytes"], "file_size", f["path"])
            err.check(sha256_file(p) == f["sha256"], "sha256", f["path"])
    if "missing_file" in err.counts:
        return err, {}

    # --- довідники ---
    units = _read_small(out / by_table["units"][0]["path"], "units", err)
    err.check([tuple(r) for r in units] == UNITS_EXPECTED, "units_fixed", repr(units))
    unit_scale = {1: 0, 2: 3, 3: 3}

    cat_ids, cat_names = set(), set()
    for r in _read_small(out / by_table["categories"][0]["path"], "categories", err):
        try:
            cid = _int(r[0], 1, SMALLINT_MAX)
            name = _text(r[1], 100, False)
            _text(r[2], None, True)
            err.check(cid not in cat_ids, "pk_categories", r[0])
            err.check(name not in cat_names, "unique_category_name", name)
            cat_ids.add(cid); cat_names.add(name)
        except (ValueError, IndexError) as e:
            err.add("categories_row", f"{r}: {e}")

    type_ids, type_names = set(), set()
    for r in _read_small(out / by_table["product_types"][0]["path"], "product_types", err):
        try:
            tid = _int(r[0], 1, SMALLINT_MAX)
            cid = _int(r[1], 1, SMALLINT_MAX)
            name = _text(r[2], 100, False)
            _text(r[3], None, True)
            err.check(tid not in type_ids, "pk_product_types", r[0])
            err.check(cid in cat_ids, "fk_type_category", r[1])
            err.check((cid, name) not in type_names, "unique_type_name_in_category", name)
            type_ids.add(tid); type_names.add((cid, name))
        except (ValueError, IndexError) as e:
            err.add("product_types_row", f"{r}: {e}")

    store_ids = set()
    for r in _read_small(out / by_table["stores"][0]["path"], "stores", err):
        try:
            sid = _int(r[0], 1, SMALLINT_MAX)
            _text(r[1], 120, False)
            err.check((r[2] == "") == (r[3] == ""), "coords_pair", str(r[:4]))
            if r[2] != "":
                la, lo = parse_fixed(r[2], 6), parse_fixed(r[3], 6)
                err.check(-90_000_000 <= la <= 90_000_000, "latitude_range", r[2])
                err.check(-180_000_000 <= lo <= 180_000_000, "longitude_range", r[3])
            _text(r[4], None, True)
            err.check(sid not in store_ids, "pk_stores", r[0])
            store_ids.add(sid)
        except (ValueError, IndexError) as e:
            err.add("stores_row", f"{r}: {e}")

    prod_unit: dict[int, int] = {}
    prod_type: dict[int, int] = {}
    for r in _read_small(out / by_table["products"][0]["path"], "products", err):
        try:
            pid = _int(r[0], 1, SMALLINT_MAX)
            tid = _int(r[1], 1, SMALLINT_MAX)
            uid = _int(r[2], 1, SMALLINT_MAX)
            _text(r[3], 200, False)
            _text(r[4], None, True)
            price = parse_fixed(r[5], 2)
            err.check(0 <= price <= MAX_PRICE_KOP, "current_price_range", r[5])
            err.check(pid not in prod_unit, "pk_products", r[0])
            err.check(tid in type_ids, "fk_product_type", r[1])
            err.check(uid in unit_scale, "fk_product_unit", r[2])
            prod_unit[pid] = uid; prod_type[pid] = tid
        except (ValueError, IndexError) as e:
            err.add("products_row", f"{r}: {e}")

    err.check(len(store_ids) == cfg["n_stores"], "count_stores", f"{len(store_ids)} != {cfg['n_stores']}")
    err.check(len(prod_unit) == cfg["n_products"], "count_products", f"{len(prod_unit)} != {cfg['n_products']}")
    used_types = set(prod_type.values())
    err.check(used_types == type_ids, "types_without_products", str(sorted(type_ids - used_types)[:10]))

    # --- чеки × позиції (злиття двох упорядкованих потоків) ---
    t_lo = int((dt.datetime.fromisoformat(m["time_range_utc"]["start_inclusive"]) - EPOCH).total_seconds())
    t_hi = int((dt.datetime.fromisoformat(m["time_range_utc"]["end_exclusive"]) - EPOCH).total_seconds())
    rec_paths = [out / f["path"] for f in by_table["receipts"]]
    item_paths = [out / f["path"] for f in by_table["receipt_items"]]
    per_file: Counter = Counter()
    items = _stream(item_paths, "receipt_items", err, per_file)
    pending = next(items, None)

    n_rec = n_items = 0
    prev_key = (-1, 0)
    expected_id = 1
    sold_stores, sold_products = set(), set()
    len_hist = Counter()
    unit_lines = Counter()

    for path, r in _stream(rec_paths, "receipts", err, per_file):
        n_rec += 1
        try:
            if len(r) != 4:
                raise ValueError(f"полів {len(r)}")
            rid = _int(r[0], 1, INTEGER_MAX)
            sid = _int(r[1], 1, SMALLINT_MAX)
            mt = TS_RE.match(r[2])
            if not mt:
                raise ValueError(f"формат часу {r[2]!r}")
            ts = int((dt.datetime.fromisoformat(r[2]) - EPOCH).total_seconds())
            total = parse_fixed(r[3], 2)
        except (ValueError, IndexError) as e:
            err.add("receipts_row", f"{path.name}: {r}: {e}")
            continue
        err.check(rid == expected_id, "receipt_id_sequence", f"{rid} (очікувався {expected_id})")
        expected_id = rid + 1
        err.check((ts, rid) > prev_key, "receipts_order", f"{rid} {r[2]}")
        prev_key = (ts, rid)
        err.check(t_lo <= ts < t_hi, "sold_at_range", r[2])
        err.check(path.stem.endswith(r[2][:7]), "receipt_part_month", f"{path.name}: {r[2]}")
        err.check(sid in store_ids, "fk_receipt_store", r[1])
        err.check(0 <= total <= NUM14_2_MAX, "total_range", r[3])
        sold_stores.add(sid)

        # позиції цього чека
        while pending is not None and pending[1] and pending[1][0].isdigit() and int(pending[1][0]) < rid:
            err.add("orphan_or_misordered_item", str(pending[1]))
            pending = next(items, None)
        lines, seen, s = 0, set(), 0
        while pending is not None:
            ip, it = pending
            try:
                if len(it) != 6:
                    raise ValueError(f"полів {len(it)}")
                irid = _int(it[0], 1, INTEGER_MAX)
            except ValueError as e:
                err.add("items_row", f"{ip.name}: {it}: {e}")
                pending = next(items, None)
                continue
            if irid != rid:
                break
            pending = next(items, None)
            lines += 1
            n_items += 1
            try:
                ln = _int(it[1], 1, SMALLINT_MAX)
                pid = _int(it[2], 1, SMALLINT_MAX)
                q = parse_fixed(it[3], 3)
                up = parse_fixed(it[4], 2)
                la = parse_fixed(it[5], 2)
            except ValueError as e:
                err.add("items_row", f"{ip.name}: {it}: {e}")
                continue
            err.check(ln == lines, "line_no_sequence", f"чек {rid}: line_no {ln}, очікувався {lines}")
            err.check(pid in prod_unit, "fk_item_product", it[2])
            err.check(pid not in seen, "duplicate_product_in_receipt", f"чек {rid}: товар {pid}")
            seen.add(pid)
            err.check(0 < q <= MAX_QTY_MILLI and q <= NUM10_3_MAX, "quantity_range", f"чек {rid}: {it[3]}")
            u = prod_unit.get(pid)
            if u == 1:
                err.check(q % 1000 == 0, "pcs_quantity_integer", f"чек {rid}: {it[3]}")
            err.check(0 <= up <= MAX_PRICE_KOP and up <= NUM10_2_MAX, "unit_price_range", it[4])
            err.check(0 <= la <= NUM14_2_MAX, "line_amount_range", it[5])
            err.check(la == (q * up + 500) // 1000, "line_amount_rounding", f"чек {rid}: {it}")
            s += la
            sold_products.add(pid)
            unit_lines[u] += 1
        err.check(lines >= 1, "empty_receipt", str(rid))
        err.check(lines <= MAX_LINES_PER_RECEIPT, "receipt_too_long", f"{rid}: {lines}")
        err.check(s == total, "receipt_total", f"чек {rid}: сума позицій {s} коп. != {total} коп.")
        len_hist[lines] += 1
    while pending is not None:
        err.add("orphan_or_misordered_item", str(pending[1]))
        pending = next(items, None)

    # --- підсумкові кількості й покриття ---
    for f in by_table["receipts"] + by_table["receipt_items"]:
        err.check(per_file[out / f["path"]] == f["rows"], "rows_per_file", f"{f['path']}: {per_file[out / f['path']]}")
    small_rows = {"units": len(units), "categories": len(cat_ids), "product_types": len(type_ids),
                  "stores": len(store_ids), "products": len(prod_unit)}
    for t, n in small_rows.items():
        err.check(n == by_table[t][0]["rows"], "rows_per_file", f"{t}: {n}")
    rc = m["row_counts"]
    err.check(n_rec == cfg["n_receipts"] == rc["receipts"], "count_receipts", f"{n_rec} / {cfg['n_receipts']}")
    err.check(n_items == rc["receipt_items"], "count_items", f"{n_items} != {rc['receipt_items']}")
    if cfg["n_receipts"] >= 3_000_000:
        err.check(n_items >= 10_000_000, "full_profile_min_items", str(n_items))
    err.check(sold_stores == store_ids, "stores_without_sales", str(sorted(store_ids - sold_stores)[:10]))
    err.check(sold_products == set(prod_unit), "products_without_sales",
              f"{len(set(prod_unit) - sold_products)} шт.")
    summary = {"receipts": n_rec, "receipt_items": n_items, "mean_lines": n_items / max(n_rec, 1),
               "stores_with_sales": len(sold_stores), "products_with_sales": len(sold_products),
               "max_lines": max(len_hist) if len_hist else 0}
    return err, summary
