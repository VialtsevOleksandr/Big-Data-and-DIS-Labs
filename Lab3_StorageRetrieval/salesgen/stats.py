"""Статистика якості згенерованого набору (потоково).

Пари типів рахуються по ВСІХ чеках через матрицю спільної появи (T×T);
трійки/четвірки — на відтворюваній рівномірній випадковій вибірці чеків,
бо їх кількість комбінаторно зростає з довжиною кошика.
Рівень аналізу кошика — product_type_id: множина унікальних типів чека.
"""
from __future__ import annotations

import csv
import datetime as dt
import json
import math
from collections import Counter
from itertools import combinations
from pathlib import Path

import numpy as np

from .catalog_spec import CATEGORIES, MISSIONS
from .config import STREAM_STATS, rng_for

SIZE_STRATA = (1, 2, 3, 4, 6, 9, 13, 18, 31)   # межі страт за кількістю різних типів у чеку
MIN_PAIR_SUPPORT = 30                           # мінімальна підтримка для оцінки lift


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    den = 1 + z * z / n
    c = (p + z * z / (2 * n)) / den
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return (c - h, c + h)


def _gini(x: np.ndarray) -> float:
    x = np.sort(np.asarray(x, dtype=np.float64))
    n = len(x)
    return float((2 * np.arange(1, n + 1) - n - 1).dot(x) / (n * x.sum())) if x.sum() else 0.0


def compute(out_dir: str | Path, itemset_sample: int = 20_000, log=print) -> dict:
    if itemset_sample < 1:
        raise ValueError("itemset_sample має бути додатним")
    out = Path(out_dir)
    m = json.loads((out / "manifest.json").read_text(encoding="utf-8"))["data"]
    files = m["files"]
    path_of = {f["table"]: out / f["path"] for f in files if f["part"] is None}

    with open(path_of["products"], encoding="utf-8", newline="") as f:
        prods = list(csv.DictReader(f))
    with open(path_of["product_types"], encoding="utf-8", newline="") as f:
        types = list(csv.DictReader(f))
    P, T = len(prods), len(types)
    p_type = np.zeros(P + 1, dtype=np.int64)
    p_unit = np.zeros(P + 1, dtype=np.int64)
    for r in prods:
        p_type[int(r["product_id"])] = int(r["product_type_id"]) - 1
        p_unit[int(r["product_id"])] = int(r["unit_id"])
    tname = [r["name"] for r in types]
    n_receipts = m["row_counts"]["receipts"]
    sample_ids = set(int(i) + 1 for i in rng_for(m["seed"], STREAM_STATS).choice(
        n_receipts, size=min(n_receipts, itemset_sample), replace=False))

    # ---------- прохід 1: чеки (магазини, час) ----------
    by_store = Counter(); by_month = Counter(); by_wd = Counter(); by_hour = Counter(); by_day = Counter()
    for f in (x for x in files if x["table"] == "receipts"):
        with open(out / f["path"], encoding="utf-8") as fh:
            next(fh)
            for line in fh:
                _, sid, ts, _ = line.split(",")
                by_store[int(sid)] += 1
                by_month[ts[:7]] += 1
                by_day[ts[:10]] += 1
                by_hour[int(ts[11:13])] += 1
    for day, c in by_day.items():
        by_wd[dt.date.fromisoformat(day).weekday()] += c

    # ---------- прохід 2: позиції (кошики) ----------
    line_cnt = np.zeros(P + 1, dtype=np.int64)
    unit_lines = Counter(); unit_rev = Counter(); unit_q = Counter(); pcs_q1 = 0
    len_hist = Counter(); ntypes_hist = Counter()
    co = np.zeros((T, T), dtype=np.int64)
    n_strata = len(SIZE_STRATA) - 1
    strat_cnt = np.zeros((n_strata, T), dtype=np.int64)
    strat_n = np.zeros(n_strata, dtype=np.int64)
    triples: Counter = Counter()
    quads: Counter = Counter()
    batch_rows, batch_cols, batch_k = [], [], 0
    sample_n = 0

    def flush():
        nonlocal batch_rows, batch_cols, batch_k, co
        if batch_k:
            B = np.zeros((batch_k, T), dtype=np.float32)
            B[batch_rows, batch_cols] = 1.0
            co += np.rint(B.T @ B).astype(np.int64)
        batch_rows, batch_cols, batch_k = [], [], 0

    def finish(rid: int, plist: list[int]):
        nonlocal batch_k, sample_n
        len_hist[len(plist)] += 1
        ts = sorted(set(p_type[plist].tolist()))
        k = len(ts)
        ntypes_hist[k] += 1
        s = int(np.searchsorted(SIZE_STRATA, k, side="right") - 1)
        strat_n[s] += 1
        strat_cnt[s, ts] += 1
        batch_rows.extend([batch_k] * k); batch_cols.extend(ts); batch_k += 1
        if batch_k >= 20_000:
            flush()
        if rid in sample_ids:
            sample_n += 1
            if k >= 3:
                triples.update(combinations(ts, 3))
            if k >= 4:
                quads.update(combinations(ts, 4))

    cur, plist = None, []
    for f in (x for x in files if x["table"] == "receipt_items"):
        with open(out / f["path"], encoding="utf-8") as fh:
            next(fh)
            for line in fh:
                rid_s, _, pid_s, q_s, _, la_s = line.split(",")
                rid, pid = int(rid_s), int(pid_s)
                if rid != cur:
                    if cur is not None:
                        finish(cur, plist)
                    cur, plist = rid, []
                plist.append(pid)
                line_cnt[pid] += 1
                u = p_unit[pid]
                unit_lines[u] += 1
                unit_rev[u] += int(la_s.replace(".", ""))
                unit_q[u] += int(q_s.replace(".", ""))
                if u == 1 and q_s == "1.000":
                    pcs_q1 += 1
    if cur is not None:
        finish(cur, plist)
    flush()

    # ---------- агрегати ----------
    n_items = int(line_cnt.sum())
    hist = np.array([len_hist.get(i, 0) for i in range(1, 31)])
    cdf = np.cumsum(hist) / hist.sum()
    type_lines = np.bincount(p_type[1:], weights=line_cnt[1:], minlength=T)
    type_receipts = np.diag(co)
    prod_lines = line_cnt[1:]
    order_t = np.argsort(-type_lines)
    order_p = np.argsort(-prod_lines)
    store_counts = np.array([by_store.get(i, 0) for i in range(1, len(by_store) + 1)])

    # очікувана спільна поява за незалежності В МЕЖАХ страт розміру кошика
    exp_co = np.zeros((T, T))
    for s in range(n_strata):
        if strat_n[s]:
            c = strat_cnt[s].astype(np.float64)
            exp_co += np.outer(c, c) / strat_n[s]
    N = n_receipts
    iu = np.triu_indices(T, 1)
    pair_n = co[iu]
    lift_raw = pair_n * N / np.maximum(type_receipts[iu[0]] * type_receipts[iu[1]], 1)
    lift_strat = pair_n / np.maximum(exp_co[iu], 1e-12)
    mission_pairs = {tuple(sorted((a, b))) for ms in MISSIONS for a, b in combinations(
        [_type_index(k) for k in ms.types], 2)}
    is_mission = np.array([(int(a), int(b)) in mission_pairs for a, b in zip(*iu)])
    ok = pair_n >= MIN_PAIR_SUPPORT
    base_raw = float(np.median(lift_raw[ok & ~is_mission])) if (ok & ~is_mission).any() else float("nan")
    base_strat = float(np.median(lift_strat[ok & ~is_mission])) if (ok & ~is_mission).any() else float("nan")

    mission_rows = []
    for ms in MISSIONS:
        idx = [_type_index(k) for k in ms.types]
        for a, b in combinations(idx, 2):
            na, nb, nab = int(type_receipts[a]), int(type_receipts[b]), int(co[a, b])
            lo, hi = wilson(nab, na)
            mission_rows.append({
                "mission": ms.name, "A": tname[a], "B": tname[b], "n_A": na, "n_B": nb, "n_AB": nab,
                "P(B)": nb / N, "P(B|A)": nab / na if na else float("nan"), "P(B|A)_95ci": [lo, hi],
                "lift": nab * N / (na * nb) if na and nb else float("nan"),
                "lift_size_adjusted": nab / exp_co[a, b] if exp_co[a, b] else float("nan"),
                "enough_data": nab >= MIN_PAIR_SUPPORT,
            })

    top_pairs = sorted(zip(pair_n.tolist(), iu[0].tolist(), iu[1].tolist()), reverse=True)[:15]
    res = {
        "receipts": N, "receipt_items": n_items, "mean_lines": n_items / N,
        "length": {"hist_1_30": hist.tolist(), "median": int(np.searchsorted(cdf, 0.5) + 1),
                   "p90": int(np.searchsorted(cdf, 0.9) + 1), "share_le_3": float(cdf[2]),
                   "share_ge_20": float(1 - cdf[18]), "max": int(max(len_hist))},
        "distinct_types_per_receipt_mean": sum(k * v for k, v in ntypes_hist.items()) / N,
        "types": {
            "top10": [(tname[t], int(type_lines[t]), float(type_lines[t] / n_items)) for t in order_t[:10]],
            "bottom5": [(tname[t], int(type_lines[t])) for t in order_t[-5:]],
            "top10_share": float(type_lines[order_t[:10]].sum() / n_items),
            "gini": _gini(type_lines),
        },
        "products": {
            "sold": int((prod_lines > 0).sum()), "total": P,
            "top1_share": float(prod_lines[order_p[0]] / n_items),
            "top1": prods[order_p[0]]["name"],
            "top1pct_share": float(prod_lines[order_p[:max(1, P // 100)]].sum() / n_items),
            "top10pct_share": float(prod_lines[order_p[:max(1, P // 10)]].sum() / n_items),
            "lines_min_median_max": [int(prod_lines.min()), float(np.median(prod_lines)), int(prod_lines.max())],
            "gini": _gini(prod_lines),
        },
        "stores": {"with_sales": int((store_counts > 0).sum()), "min": int(store_counts.min()),
                   "median": float(np.median(store_counts)), "max": int(store_counts.max()),
                   "max_to_min": float(store_counts.max() / max(store_counts.min(), 1))},
        "time": {"by_month": dict(sorted(by_month.items())),
                 "by_weekday_mon0": {k: by_wd[k] for k in range(7)},
                 "by_hour_utc": {h: by_hour.get(h, 0) for h in range(24)},
                 "days_with_sales": len(by_day), "min_day": min(by_day.values()), "max_day": max(by_day.values()),
                 "control_2025_07_receipts": by_month.get("2025-07", 0),
                 "control_2025_07_15_receipts": by_day.get("2025-07-15", 0),
                 "mean_tuesday_receipts": float(np.mean([c for d_, c in by_day.items()
                                                          if dt.date.fromisoformat(d_).weekday() == 1] or [0]))},
        "units": {u: {"lines_share": unit_lines[k] / n_items, "revenue_share": unit_rev[k] / sum(unit_rev.values()),
                      "mean_quantity": unit_q[k] / 1000 / max(unit_lines[k], 1)}
                  for k, u in ((1, "pcs"), (2, "kg"), (3, "l"))},
        "pcs_quantity_1_share": pcs_q1 / max(unit_lines[1], 1),
        "itemsets": {
            "pairs_all_receipts_top15": [(tname[a], tname[b], n) for n, a, b in top_pairs],
            "sample_receipts": sample_n,
            "sampling": "uniform_without_replacement",
            "sample_seed": m["seed"], "sample_rng_stream": STREAM_STATS,
            "triples_top10": [([tname[t] for t in k], v) for k, v in triples.most_common(10)],
            "quads_top10": [([tname[t] for t in k], v) for k, v in quads.most_common(10)],
        },
        "lift": {"min_pair_support": MIN_PAIR_SUPPORT,
                 "baseline_median_lift_non_mission_pairs": base_raw,
                 "baseline_median_size_adjusted_lift_non_mission_pairs": base_strat,
                 "mission_pairs": mission_rows},
    }
    return res


_TYPE_KEYS = [t.key for c in CATEGORIES for t in c.types]


def _type_index(key: str) -> int:
    return _TYPE_KEYS.index(key)


def to_markdown(s: dict) -> str:
    L = []
    a = L.append
    a(f"# Статистика якості\n\nЧеків: **{s['receipts']:,}**, позицій: **{s['receipt_items']:,}**, "
      f"середня довжина: **{s['mean_lines']:.3f}**, різних типів у чеку в середньому: "
      f"**{s['distinct_types_per_receipt_mean']:.2f}**\n")
    ln = s["length"]
    a("## Довжина чека\n")
    a(f"медіана {ln['median']}, p90 {ln['p90']}, max {ln['max']}, частка ≤3: {ln['share_le_3']:.1%}, "
      f"частка ≥20: {ln['share_ge_20']:.1%}\n")
    a("| довжина | " + " | ".join(str(i) for i in range(1, 31)) + " |")
    a("|" + "---|" * 31)
    tot = sum(ln["hist_1_30"])
    a("| % | " + " | ".join(f"{v / tot * 100:.1f}" for v in ln["hist_1_30"]) + " |\n")
    t = s["types"]
    a("## Популярність типів (за позиціями)\n")
    a(f"Частка топ-10 типів: {t['top10_share']:.1%}; Gini по типах: {t['gini']:.3f}\n")
    a("| тип | позицій | частка |\n|---|---:|---:|")
    for n, c, sh in t["top10"]:
        a(f"| {n} | {c:,} | {sh:.2%} |")
    a(f"\nНайрідкісніші типи: " + ", ".join(f"{n} ({c})" for n, c in t["bottom5"]) + "\n")
    p = s["products"]
    a("## Товари\n")
    a(f"Продано {p['sold']} з {p['total']}; топ-1 товар: {p['top1']} — {p['top1_share']:.2%} позицій; "
      f"топ-1% товарів: {p['top1pct_share']:.1%}; топ-10%: {p['top10pct_share']:.1%}; "
      f"позицій на товар min/медіана/max: {p['lines_min_median_max']}; Gini: {p['gini']:.3f}\n")
    st = s["stores"]
    a("## Магазини\n")
    a(f"З продажами: {st['with_sales']}; чеків на магазин min/медіана/max: {st['min']} / {st['median']:.0f} / "
      f"{st['max']} (max/min = {st['max_to_min']:.1f})\n")
    tm = s["time"]
    a("## Час (UTC)\n")
    a("| місяць | чеків |\n|---|---:|")
    for k, v in tm["by_month"].items():
        a(f"| {k} | {v:,} |")
    wd = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Нд"]
    a("\n| день тижня | " + " | ".join(wd) + " |\n|" + "---|" * 8)
    a("| чеків | " + " | ".join(f"{tm['by_weekday_mon0'][i]:,}" for i in range(7)) + " |\n")
    a("| година UTC | " + " | ".join(str(h) for h in range(24)) + " |\n|" + "---|" * 25)
    a("| чеків | " + " | ".join(str(tm["by_hour_utc"][h]) for h in range(24)) + " |\n")
    a(f"Днів із продажами: {tm['days_with_sales']}; чеків на день min/max: {tm['min_day']} / {tm['max_day']}. "
      f"Контроль: липень 2025 — {tm['control_2025_07_receipts']:,} чеків; 15.07.2025 (вівторок) — "
      f"{tm['control_2025_07_15_receipts']:,} при середньому вівторку {tm['mean_tuesday_receipts']:.1f}\n")
    a("## Одиниці\n\n| одиниця | частка позицій | частка виручки | середня кількість |\n|---|---:|---:|---:|")
    for u, v in s["units"].items():
        a(f"| {u} | {v['lines_share']:.2%} | {v['revenue_share']:.2%} | {v['mean_quantity']:.3f} |")
    a(f"\nЧастка pcs-позицій з quantity = 1: {s['pcs_quantity_1_share']:.1%}\n")
    it = s["itemsets"]
    a("## Найчастіші комбінації типів\n")
    a("Пари — по всіх чеках:\n\n| пара | чеків |\n|---|---:|")
    for x, y, n in it["pairs_all_receipts_top15"]:
        a(f"| {x} + {y} | {n:,} |")
    a(f"\nТрійки та четвірки — відтворювана рівномірна випадкова вибірка без повторів "
      f"({it['sample_receipts']:,} чеків, seed {it['sample_seed']}). "
      f"Це діагностика генератора, а не повні результати SQL-запитів 8–9."
      f"\n\n| трійка | чеків |\n|---|---:|")
    for k, v in it["triples_top10"]:
        a(f"| {' + '.join(k)} | {v} |")
    a("\n| четвірка | чеків |\n|---|---:|")
    for k, v in it["quads_top10"]:
        a(f"| {' + '.join(k)} | {v} |")
    lf = s["lift"]
    a("\n## Закладені зв'язки: умовна частота і lift\n")
    a(f"Базова лінія (медіана по всіх «незакладених» парах з n_AB ≥ {lf['min_pair_support']}): "
      f"lift = {lf['baseline_median_lift_non_mission_pairs']:.2f}, lift з поправкою на розмір кошика = "
      f"{lf['baseline_median_size_adjusted_lift_non_mission_pairs']:.2f}. Звичайний lift > 1 навіть для "
      f"незалежних товарів, бо великі чеки містять «усього більше»; тому порівнюємо з базовою лінією і "
      f"наводимо lift, стратифікований за кількістю різних типів у чеку.\n")
    a("| місія | A → B | n_A | n_AB | P(B) | P(B\\|A) [95% CI Вілсона] | lift | lift (страт.) | даних достатньо |")
    a("|---|---|---:|---:|---:|---|---:|---:|---|")
    for r in lf["mission_pairs"]:
        lo, hi = r["P(B|A)_95ci"]
        a(f"| {r['mission']} | {r['A']} → {r['B']} | {r['n_A']:,} | {r['n_AB']:,} | {r['P(B)']:.3f} | "
          f"{r['P(B|A)']:.3f} [{lo:.3f}; {hi:.3f}] | {r['lift']:.2f} | {r['lift_size_adjusted']:.2f} | "
          f"{'так' if r['enough_data'] else 'ні'} |")
    return "\n".join(L) + "\n"


def run(out_dir: str | Path, itemset_sample: int = 20_000, log=print) -> dict:
    s = compute(out_dir, itemset_sample, log)
    out = Path(out_dir)
    (out / "stats.json").write_text(json.dumps(s, ensure_ascii=False, indent=1, default=float), encoding="utf-8")
    (out / "stats.md").write_text(to_markdown(s), encoding="utf-8")
    return s
