import csv
import datetime as dt
import json
import shutil
from collections import defaultdict
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import numpy as np

from salesgen.catalog import build_catalog, largest_remainder
from salesgen.config import Config
from salesgen.validate import validate
from salesgen.world import day_counts, orthodox_easter, special_factor


def manifest(out: Path) -> dict:
    return json.loads((out / "manifest.json").read_text(encoding="utf-8"))


def fingerprint(out: Path) -> str:
    return manifest(out)["data"]["data_fingerprint_sha256"]


def rows(out: Path, table: str):
    for f in manifest(out)["data"]["files"]:
        if f["table"] == table:
            with open(out / f["path"], encoding="utf-8", newline="") as fh:
                yield from csv.DictReader(fh)


# ---------- відтворюваність ----------
def test_same_seed_same_data(tiny, make):
    again = make("tiny_again")
    assert fingerprint(again) == fingerprint(tiny)
    a = [(f["path"], f["sha256"]) for f in manifest(tiny)["data"]["files"]]
    b = [(f["path"], f["sha256"]) for f in manifest(again)["data"]["files"]]
    assert a == b


def test_other_seed_other_data(tiny, make):
    other = make("tiny_seed2", seed=20050407)
    assert fingerprint(other) != fingerprint(tiny)


def test_batch_size_does_not_change_data(tiny, make):
    for bs in (1, 37, 1_000_000):
        assert fingerprint(make(f"tiny_bs{bs}", batch_size=bs)) == fingerprint(tiny)


def test_manifest_data_section_has_no_run_metadata(tiny):
    m = manifest(tiny)
    assert "batch_size" not in m["data"]["config"]
    assert m["data"]["seed"] == 20050406
    assert {"elapsed_seconds", "total_bytes", "started_at_utc"} <= set(m["run"])


# ---------- інваріанти ----------
def test_validator_passes(tiny):
    err, summary = validate(tiny)
    assert err.ok, (dict(err.counts), err.examples)
    assert summary["products_with_sales"] == 300 and summary["stores_with_sales"] == 20


def test_validator_detects_corruption(tiny, tmp_path):
    bad = tmp_path / "bad"
    shutil.copytree(tiny, bad)
    part = sorted((bad / "data" / "receipt_items").glob("*.csv"))[3]
    lines = part.read_text(encoding="utf-8").splitlines(keepends=True)
    f = lines[5].rstrip("\n").split(",")
    f[5] = f"{Decimal(f[5]) + Decimal('0.01'):.2f}"           # зіпсована сума рядка
    lines[5] = ",".join(f) + "\n"
    g = lines[7].rstrip("\n").split(",")
    h = lines[8].rstrip("\n").split(",")
    if g[0] == h[0]:
        h[2] = g[2]                                            # повтор товару в чеку
        lines[8] = ",".join(h) + "\n"
    part.write_text("".join(lines), encoding="utf-8")
    err, _ = validate(bad, check_hashes=False)
    assert "line_amount_rounding" in err.counts and "receipt_total" in err.counts
    err2, _ = validate(bad, check_hashes=True)
    assert "sha256" in err2.counts or "file_size" in err2.counts


def test_exact_amounts_with_decimal(tiny):
    sums = defaultdict(Decimal)
    for r in rows(tiny, "receipt_items"):
        q, p, la = Decimal(r["quantity"]), Decimal(r["unit_price"]), Decimal(r["line_amount"])
        assert la == (q * p).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        sums[r["receipt_id"]] += la
    for r in rows(tiny, "receipts"):
        assert Decimal(r["total_amount"]) == sums[r["receipt_id"]]


def test_quantities_by_unit(tiny):
    unit = {r["product_id"]: r["unit_id"] for r in rows(tiny, "products")}
    pcs, frac = [], []
    for r in rows(tiny, "receipt_items"):
        q = Decimal(r["quantity"])
        assert 0 < q <= 20 and len(r["quantity"].split(".")[1]) == 3
        if unit[r["product_id"]] == "1":
            assert q == q.to_integral_value()
            pcs.append(q)
        else:
            frac.append(q)
    assert sum(1 for q in frac if q != q.to_integral_value()) > 0.9 * len(frac)
    assert sum(1 for q in pcs if q > 1) > 0.05 * len(pcs)


def test_quantity_depends_on_type():
    """Розподіл кількості задано на рівні фасування/типу: картопля важча за часник."""
    cat = build_catalog(Config())
    tk = cat.type_keys
    def med(key):
        idx = [p for p in range(cat.n_products) if tk[cat.prod_type[p]] == key and cat.prod_unit[p] == 2]
        return float(np.median(np.exp(cat.q_mu[idx])))
    assert med("potato") > 5 * med("garlic")


def test_no_duplicate_products_and_line_numbers(tiny):
    seen = defaultdict(set)
    last = {}
    for r in rows(tiny, "receipt_items"):
        rid, ln = int(r["receipt_id"]), int(r["line_no"])
        assert r["product_id"] not in seen[rid]
        seen[rid].add(r["product_id"])
        assert ln == last.get(rid, 0) + 1
        last[rid] = ln
    assert len(last) == 3000 and max(last.values()) <= 30


def test_ids_and_order(tiny):
    prev = None
    ids = []
    for r in rows(tiny, "receipts"):
        key = (dt.datetime.fromisoformat(r["sold_at"]), int(r["receipt_id"]))
        assert prev is None or key > prev
        prev = key
        ids.append(key[1])
    assert ids == list(range(1, 3001))


def test_year_coverage(tiny):
    days = {r["sold_at"][:10] for r in rows(tiny, "receipts")}
    times = [r["sold_at"] for r in rows(tiny, "receipts")]
    assert len(days) == 365
    assert times[0].startswith("2025-01-01T") and times[-1].startswith("2025-12-31T")
    assert all(t.endswith("+00:00") for t in times)


def test_day_counts_exact_and_control_day_not_special():
    cfg = Config()
    c = day_counts(cfg)
    assert c.sum() == 3_000_000 and (c > 0).all()
    assert special_factor(dt.date(2025, 7, 15)) == 1.0
    assert orthodox_easter(2025) == dt.date(2025, 4, 20)
    assert largest_remainder(np.array([1.0, 1.0, 1.0]), 10).tolist() == [4, 3, 3]
