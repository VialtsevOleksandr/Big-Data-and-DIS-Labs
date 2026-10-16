"""Запис CSV (частинами за календарними місяцями) і manifest.json."""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import io
import json
import platform
import sys
import time
from pathlib import Path

import numpy as np

from . import config as C
from .generate import World, build_world, generate_day
from .money import FRAC2, FRAC3, fmt_kop, fmt_micro_signed
from .schema import CSV_FORMAT, header, schema_for_manifest
from .world import expected_basket_length

HMS = [f"{h:02d}:{m:02d}:{s:02d}" for h in range(24) for m in range(60) for s in range(60)]


class HashingWriter:
    """Текстовий запис UTF-8 з одночасним підрахунком SHA-256, байтів і рядків."""

    def __init__(self, path: Path, table: str, part: str | None):
        self.path, self.table, self.part = path, table, part
        path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(path, "wb")
        self.h = hashlib.sha256()
        self.bytes = 0
        self.rows = 0
        self.write(header(table) + "\n", rows=0)

    def write(self, text: str, rows: int) -> None:
        b = text.encode("utf-8")
        self.f.write(b)
        self.h.update(b)
        self.bytes += len(b)
        self.rows += rows

    def close(self, root: Path) -> dict:
        self.f.close()
        return {"table": self.table, "part": self.part, "path": self.path.relative_to(root).as_posix(),
                "rows": self.rows, "bytes": self.bytes, "sha256": self.h.hexdigest()}


def _csv_text(rows) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
    for r in rows:
        w.writerow(["" if v is None else v for v in r])
    return buf.getvalue()


def write_dimensions(world: World, data_dir: Path, root: Path) -> list[dict]:
    cat, st, pr = world.cat, world.stores, world.prices
    tables = {
        "units": [tuple(u) for u in cat.units],
        "categories": cat.categories,
        "product_types": cat.product_types,
        "stores": [(sid, name, None if la is None else fmt_micro_signed(la),
                    None if lo is None else fmt_micro_signed(lo), desc) for sid, name, la, lo, desc in st.rows],
        "products": [(pid, tid, uid, name, desc, fmt_kop(int(pr.current_kop[pid - 1])))
                     for pid, tid, uid, name, desc in cat.products],
    }
    out = []
    for t in ("units", "categories", "product_types", "stores", "products"):
        w = HashingWriter(data_dir / f"{t}.csv", t, None)
        w.write(_csv_text(tables[t]), rows=len(tables[t]))
        out.append(w.close(root))
    return out


def _fmt_receipts(dd, a: int, b: int, date_prefix: str) -> str:
    ids = dd.receipt_id[a:b].tolist()
    sids = dd.store_id[a:b].tolist()
    secs = (dd.ts[a:b] % 86400).tolist()
    ti, tf = np.divmod(dd.total_kop[a:b], 100)
    return "".join(f"{i},{s},{date_prefix}{HMS[x]}+00:00,{u}.{FRAC2[f]}\n"
                   for i, s, x, u, f in zip(ids, sids, secs, ti.tolist(), tf.tolist()))


def _fmt_items(dd, a: int, b: int) -> str:
    lo, hi = int(dd.item_start[a]), int(dd.item_start[b])
    rid = np.repeat(dd.receipt_id[a:b], np.diff(dd.item_start[a:b + 1])).tolist()
    ln = dd.item_line_no[lo:hi].tolist()
    pid = dd.item_product_id[lo:hi].tolist()
    qi, qf = np.divmod(dd.item_q_milli[lo:hi], 1000)
    ui, uf = np.divmod(dd.item_unit_kop[lo:hi], 100)
    li, lf = np.divmod(dd.item_line_kop[lo:hi], 100)
    return "".join(f"{r},{n},{p},{a1}.{FRAC3[a2]},{b1}.{FRAC2[b2]},{c1}.{FRAC2[c2]}\n"
                   for r, n, p, a1, a2, b1, b2, c1, c2 in zip(
                       rid, ln, pid, qi.tolist(), qf.tolist(), ui.tolist(), uf.tolist(), li.tolist(), lf.tolist()))


def peak_rss_bytes() -> int | None:
    try:
        import resource
        r = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(r if sys.platform == "darwin" else r * 1024)
    except ImportError:
        pass
    if sys.platform == "win32":
        try:
            import ctypes
            from ctypes import wintypes as wt

            class PMC(ctypes.Structure):
                _fields_ = [("cb", wt.DWORD), ("PageFaultCount", wt.DWORD),
                            ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
                            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
                            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
                            ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t)]
            k32 = ctypes.WinDLL("kernel32")
            psapi = ctypes.WinDLL("psapi")
            k32.GetCurrentProcess.restype = wt.HANDLE
            psapi.GetProcessMemoryInfo.argtypes = [wt.HANDLE, ctypes.POINTER(PMC), wt.DWORD]
            pmc = PMC()
            pmc.cb = ctypes.sizeof(PMC)
            if psapi.GetProcessMemoryInfo(k32.GetCurrentProcess(), ctypes.byref(pmc), pmc.cb):
                return int(pmc.PeakWorkingSetSize)
        except Exception:
            return None
    return None


def run(cfg: C.Config, out_dir: Path, overwrite: bool = False, log=print) -> dict:
    out_dir = Path(out_dir).resolve()
    nonempty = out_dir.exists() and any(out_dir.iterdir())
    if nonempty and not overwrite:
        raise FileExistsError(f"Каталог {out_dir} не порожній (використайте --overwrite)")
    t0 = time.perf_counter()
    started = dt.datetime.now(dt.timezone.utc)

    # Спочатку вся конфігурація й побудова моделі: помилка не псує попередній набір.
    world = build_world(cfg)
    data_dir = (out_dir / "data").resolve()
    if not data_dir.is_relative_to(out_dir):
        raise ValueError("Каталог data має залишатися всередині каталогу результату")
    owned = set()
    if nonempty:
        manifest_path = out_dir / "manifest.json"
        if not manifest_path.is_file():
            raise ValueError("--overwrite дозволений лише для набору з manifest.json salesgen")
        old = json.loads(manifest_path.read_text(encoding="utf-8"))["data"]
        if old.get("generator") != "salesgen":
            raise ValueError("Каталог не є набором salesgen")
        for entry in old["files"]:
            path = (out_dir / entry["path"]).resolve()
            if not path.is_relative_to(data_dir) or path.suffix != ".csv":
                raise ValueError("Небезпечний шлях CSV у попередньому маніфесті")
            if path.exists() and not path.is_file():
                raise ValueError("Очікувався файл CSV")
            owned.add(path)
    months = {(cfg.start + dt.timedelta(days=d)).strftime("%Y-%m") for d in range(cfg.n_days)}
    expected = {data_dir / f"{t}.csv" for t in ("units", "categories", "product_types", "stores", "products")}
    expected.update(data_dir / t / f"{t}_{month}.csv" for t in ("receipts", "receipt_items") for month in months)
    for path in expected:
        if not path.resolve().is_relative_to(data_dir):
            raise ValueError("Шлях нового CSV виходить за межі data/")
        if path.exists() and path.resolve() not in owned:
            raise ValueError(f"Генерація перезаписала б сторонній файл: {path.name}")
    # Видаляємо тільки CSV попереднього набору, а не будь-який *.csv у data/.
    for path in sorted(owned):
        if path.exists():
            path.unlink()
    log(f"Світ побудовано за {time.perf_counter() - t0:.1f} с: {cfg.n_products} товарів, "
        f"{world.cat.n_types} типів, {cfg.n_stores} магазинів, {cfg.n_days} днів")
    files = write_dimensions(world, data_dir, out_dir)

    rec_parts, item_parts = [], []
    cur_month, rw, iw = None, None, None
    n_items = 0
    for d in range(cfg.n_days):
        date = cfg.start + dt.timedelta(days=d)
        month = f"{date.year:04d}-{date.month:02d}"
        if month != cur_month:
            if rw:
                rec_parts.append(rw.close(out_dir)); item_parts.append(iw.close(out_dir))
                log(f"  {cur_month}: готово, {time.perf_counter() - t0:.1f} с, позицій усього {n_items:,}")
            cur_month = month
            rw = HashingWriter(data_dir / "receipts" / f"receipts_{month}.csv", "receipts", month)
            iw = HashingWriter(data_dir / "receipt_items" / f"receipt_items_{month}.csv", "receipt_items", month)
        dd = generate_day(world, d)
        prefix = date.isoformat() + "T"
        n = len(dd.receipt_id)
        for a in range(0, n, cfg.batch_size):
            b = min(n, a + cfg.batch_size)
            rw.write(_fmt_receipts(dd, a, b, prefix), rows=b - a)
            iw.write(_fmt_items(dd, a, b), rows=int(dd.item_start[b] - dd.item_start[a]))
        n_items += int(dd.item_start[n])
    rec_parts.append(rw.close(out_dir)); item_parts.append(iw.close(out_dir))
    log(f"  {cur_month}: готово, {time.perf_counter() - t0:.1f} с, позицій усього {n_items:,}")
    files += rec_parts + item_parts

    fp = hashlib.sha256()
    for f in files:
        fp.update(f"{f['path']}\t{f['sha256']}\n".encode())
    elapsed = time.perf_counter() - t0
    manifest = {
        "data": {   # усе в цьому розділі детерміноване seed + конфігурацією + версіями
            "generator": "salesgen", "generator_version": C.GENERATOR_VERSION,
            "seed": cfg.seed, "config": cfg.data_params(),
            "dependencies": {"python": platform.python_version(), "numpy": np.__version__},
            "rng": {"bit_generator": "PCG64", "seed_sequence": "SeedSequence(entropy=seed, spawn_key=(stream, key))",
                    "streams": {"catalog": C.STREAM_CATALOG, "stores": C.STREAM_STORES,
                                "calendar(date_ordinal)": C.STREAM_CALENDAR, "prices": C.STREAM_PRICES,
                                "coverage": C.STREAM_COVERAGE, "day(date_ordinal)": C.STREAM_DAY}},
            "time_range_utc": {"start_inclusive": f"{cfg.start_date}T00:00:00+00:00",
                               "end_exclusive": f"{cfg.end_date}T00:00:00+00:00"},
            "currency": "UAH",
            "load_order": ["units", "categories", "product_types", "stores", "products", "receipts", "receipt_items"],
            "files": files,
            "row_counts": {t: sum(f["rows"] for f in files if f["table"] == t)
                           for t in ("units", "categories", "product_types", "stores", "products",
                                     "receipts", "receipt_items")},
            "expected_mean_lines_per_receipt_model": round(expected_basket_length(), 4),
            "data_fingerprint_sha256": fp.hexdigest(),
            "schema": schema_for_manifest(),
            "csv_format": CSV_FORMAT,
        },
        "run": {    # метадані запуску — можуть відрізнятися між запусками
            "started_at_utc": started.isoformat(timespec="seconds"),
            "finished_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
            "elapsed_seconds": round(elapsed, 2),
            "batch_size": cfg.batch_size,
            "total_bytes": sum(f["bytes"] for f in files),
            "peak_rss_bytes": peak_rss_bytes(),
            "platform": platform.platform(), "machine": platform.machine(),
            "throughput_receipts_per_s": round(cfg.n_receipts / elapsed, 1) if elapsed else None,
        },
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    log(f"Готово: {cfg.n_receipts:,} чеків, {n_items:,} позицій, "
        f"{manifest['run']['total_bytes'] / 2**20:.1f} МіБ, {elapsed:.1f} с")
    return manifest
