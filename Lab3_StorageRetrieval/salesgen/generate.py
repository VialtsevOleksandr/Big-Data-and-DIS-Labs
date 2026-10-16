"""Ядро генерації: «світ» (каталог, ціни, магазини, календар) + чеки дня.

Кожен день генерується власним RNG-потоком (seed, STREAM_DAY, ординал дати)
і повністю у векторизованому вигляді (NumPy). Від розміру пакета запису
дані не залежать: пакет лише визначає, скільки чеків форматується й
записується за раз (див. writer.py).
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np

from .catalog import Catalog, build_catalog, n_spec_types
from .config import MAX_LINES_PER_RECEIPT, STREAM_COVERAGE, STREAM_DAY, Config, rng_for
from .pricing import Prices, build_prices
from .world import HOUR_PROFILES, HOUR_TYPES, Stores, basket_length_pmf, build_stores, day_counts

# Параметри місій; самі набори типів описано в catalog_spec.py.
P_MISSION = 0.55               # P(чек довжини ≥2 має основну місію)
SECOND_MISSION_MIN_LEN = 12    # друга місія можлива лише у великих чеках
P_SECOND_MISSION = 0.30
EXTRA_MARGIN_ABS = 2           # запас кандидатів-«додаткових» товарів понад потрібну кількість
EXTRA_MARGIN_REL = 0.15
MAX_TOPUP_ROUNDS = 6           # обмежена кількість дозаповнень; далі — детермінований fallback

EPOCH = dt.date(1970, 1, 1)


@dataclass
class World:
    cfg: Config
    cat: Catalog
    prices: Prices
    stores: Stores
    counts: np.ndarray            # чеків на день
    offsets: np.ndarray           # кількість чеків до початку дня (len = n_days + 1)
    length_pmf: np.ndarray
    store_cov: dict               # day -> [(індекс у порядку жеребкування, store_idx)]
    prod_cov: dict                # day -> [(позиція у відсортованому дні, product_idx)]


def build_world(cfg: Config) -> World:
    cfg.validate(n_spec_types())
    if cfg.n_receipts * MAX_LINES_PER_RECEIPT < cfg.n_products:
        raise ValueError("забагато товарів для такої кількості чеків: покриття неможливе")
    cat = build_catalog(cfg)
    prices = build_prices(cfg, cat)
    stores = build_stores(cfg)
    counts = day_counts(cfg)
    offsets = np.concatenate([[0], np.cumsum(counts)])

    # Покриття: кожному магазину — один чек, кожному товару — одна позиція
    # у випадково обраному чеку (рівномірно по всьому періоду).
    rng = rng_for(cfg.seed, STREAM_COVERAGE)
    N, S, P = cfg.n_receipts, cfg.n_stores, cfg.n_products
    store_g = rng.choice(N, size=S, replace=False)
    reps = -(-P // N)                                   # ceil(P / N) ≤ 30 — не більше стількох на чек
    prod_g = np.concatenate([rng.permutation(N) for _ in range(reps)])[:P] if reps > 1 \
        else rng.permutation(N)[:P]
    store_cov: dict[int, list] = {}
    for s, g in enumerate(store_g):
        d = int(np.searchsorted(offsets, g, side="right") - 1)
        store_cov.setdefault(d, []).append((int(g - offsets[d]), s))
    prod_cov: dict[int, list] = {}
    for p, g in enumerate(prod_g):
        d = int(np.searchsorted(offsets, g, side="right") - 1)
        prod_cov.setdefault(d, []).append((int(g - offsets[d]), p))
    return World(cfg, cat, prices, stores, counts, offsets, basket_length_pmf(), store_cov, prod_cov)


@dataclass
class DayData:
    receipt_id: np.ndarray        # int64, зростає
    store_id: np.ndarray          # int64
    ts: np.ndarray                # int64, секунди Unix-епохи UTC, неспадні
    total_kop: np.ndarray         # int64
    item_start: np.ndarray        # int64, len = n + 1: позиції чека i — [item_start[i], item_start[i+1])
    item_line_no: np.ndarray
    item_product_id: np.ndarray
    item_q_milli: np.ndarray
    item_unit_kop: np.ndarray
    item_line_kop: np.ndarray


def _within_type_cdf(cat: Catalog, w: np.ndarray) -> np.ndarray:
    """Конкатенована CDF «тип t + частка в типі» — монотонна, для searchsorted."""
    first = cat.type_first_product
    cs = np.cumsum(w)
    before = np.concatenate([[0.0], cs])[first[:-1]]
    tot = np.concatenate([[0.0], cs])[first[1:]] - before
    t = cat.prod_type
    cdf = t + (cs - before[t]) / tot[t]
    cdf[first[1:] - 1] = np.arange(cat.n_types) + 1.0      # точний кінець кожного типу
    return cdf


def _store_cdf(cat: Catalog, stores: Stores, gw: np.ndarray) -> np.ndarray:
    """Конкатенована CDF товарів для кожного магазину: значення в (s, s+1]."""
    S = len(stores.visit_weight)
    m = stores.cat_pref[:, cat.type_category[cat.prod_type]] * gw[None, :]
    cs = np.cumsum(m, axis=1)
    cs /= cs[:, -1:]
    cs += np.arange(S)[:, None]
    cs[:, -1] = np.arange(S) + 1.0
    return cs.ravel()


def _dedupe_take(rec, prod, need, n, P):
    """Залишає першу появу (чек, товар) у порядку пріоритету і перші need[r] з них."""
    order = np.argsort(rec, kind="stable")
    r, p = rec[order], prod[order]
    _, first_idx = np.unique(r * P + p, return_index=True)
    keep = np.zeros(len(r), dtype=bool)
    keep[first_idx] = True
    r, p = r[keep], p[keep]
    starts = np.searchsorted(r, np.arange(n))
    rank = np.arange(len(r)) - starts[r]
    sel = rank < need[r]
    return r[sel], p[sel]


def generate_day(world: World, d: int) -> DayData:
    cfg, cat, st, pr = world.cfg, world.cat, world.stores, world.prices
    n = int(world.counts[d])
    P = cat.n_products
    date = cfg.start + dt.timedelta(days=d)
    rng = rng_for(cfg.seed, STREAM_DAY, date.toordinal())
    weekend = date.weekday() >= 5

    # 1) магазин кожного чека (порядок жеребкування), потім гарантоване покриття
    sw = st.visit_weight * (st.weekend_factor if weekend else 1.0)
    store = rng.choice(len(sw), size=n, p=sw / sw.sum())
    for i, s in world.store_cov.get(d, ()):
        store[i] = s

    # 2) час: година за профілем типу магазину, секунда — рівномірно в межах години
    hour = np.zeros(n, dtype=np.int64)
    for ht, name in enumerate(HOUR_TYPES):
        mask = st.hour_type[store] == ht
        k = int(mask.sum())
        if k:
            prof = HOUR_PROFILES[name][1 if weekend else 0]
            hour[mask] = rng.choice(24, size=k, p=prof / prof.sum())
    sec = hour * 3600 + rng.integers(0, 3600, n)
    order = np.lexsort((np.arange(n), sec))
    store, sec = store[order], sec[order]

    # 3) цільова довжина чека
    L = rng.choice(MAX_LINES_PER_RECEIPT, size=n, p=world.length_pmf) + 1

    # 4) кандидати в порядку пріоритету: покриття → місії → додаткові товари
    boost = pr.demand_boost(d)
    w_in = cat.prod_weight_in_type * boost
    type_cdf = _within_type_cdf(cat, w_in)
    base_in = np.bincount(cat.prod_type, weights=cat.prod_weight_in_type, minlength=cat.n_types)
    gw = cat.type_weight[cat.prod_type] * w_in / base_in[cat.prod_type]
    store_cdf = _store_cdf(cat, st, gw)

    cand_r, cand_p = [], []
    cov = world.prod_cov.get(d, ())
    if cov:
        fr = np.array([i for i, _ in cov], dtype=np.int64)
        fp = np.array([p for _, p in cov], dtype=np.int64)
        cand_r.append(fr); cand_p.append(fp)
        L = np.maximum(L, np.bincount(fr, minlength=n))       # покриття ніколи не відкидається

    mw = cat.mission_weight / cat.mission_weight.sum()
    has1 = (L >= 2) & (rng.random(n) < P_MISSION)
    m1 = rng.choice(len(mw), size=n, p=mw)
    has2 = (L >= SECOND_MISSION_MIN_LEN) & (rng.random(n) < P_SECOND_MISSION)
    m2 = rng.choice(len(mw), size=n, p=mw)
    mrec = np.concatenate([np.flatnonzero(has1), np.flatnonzero(has2)])
    mid = np.concatenate([m1[has1], m2[has2]])
    mtypes = cat.mission_types[mid]                              # (k, 4)
    mkeep = rng.random(mtypes.shape) < cat.mission_keep[mid]
    mkeep &= mtypes >= 0
    mshuffle = rng.random(mtypes.shape)                         # випадковий порядок елементів місії
    rr = np.repeat(mrec, 4).reshape(-1, 4)
    sel = mkeep.ravel()
    t_sel = mtypes.ravel()[sel]
    r_sel = rr.ravel()[sel]
    sh = mshuffle.ravel()[sel]
    u = rng.random(len(t_sel))
    p_sel = np.searchsorted(type_cdf, t_sel + u, side="right")
    o = np.lexsort((sh, r_sel))
    cand_r.append(r_sel[o]); cand_p.append(p_sel[o])

    def draw_extras(recs: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        k = L[recs] + EXTRA_MARGIN_ABS + np.ceil(EXTRA_MARGIN_REL * L[recs]).astype(np.int64)
        rx = np.repeat(recs, k)
        ux = rng.random(len(rx))
        px = np.searchsorted(store_cdf, store[rx] + ux, side="right") - store[rx] * P
        return rx, px

    rx, px = draw_extras(np.arange(n))
    cand_r.append(rx); cand_p.append(px)
    all_r, all_p = np.concatenate(cand_r), np.concatenate(cand_p)
    r, p = _dedupe_take(all_r, all_p, L, n, P)
    for _ in range(MAX_TOPUP_ROUNDS):
        cnt = np.bincount(r, minlength=n)
        short = np.flatnonzero(cnt < L)
        if len(short) == 0:
            break
        rx, px = draw_extras(short)
        all_r, all_p = np.concatenate([all_r, rx]), np.concatenate([all_p, px])
        r, p = _dedupe_take(all_r, all_p, L, n, P)
    cnt = np.bincount(r, minlength=n)
    short = np.flatnonzero(cnt < L)
    if len(short):                                              # детермінований fallback, без циклів «до успіху»
        add_r, add_p = [], []
        for i in short:
            prob = np.diff(np.concatenate([[store[i]], store_cdf[store[i] * P:(store[i] + 1) * P]]))
            prob[p[r == i]] = 0.0
            extra = rng.choice(P, size=int(L[i] - cnt[i]), replace=False, p=prob / prob.sum())
            add_r.append(np.full(len(extra), i)); add_p.append(extra)
        r = np.concatenate([r] + add_r); p = np.concatenate([p] + add_p)

    # 5) порядок рядків у чеку — випадковий (порядок сканування)
    o = np.lexsort((rng.random(len(r)), r))
    r, p = r[o], p[o]
    starts = np.searchsorted(r, np.arange(n + 1))
    line_no = np.arange(len(r)) - starts[r] + 1

    # 6) кількість: pcs — геометрична (ціла), kg/l — лог-нормальна з округленням до 0,001
    q = np.empty(len(p), dtype=np.int64)
    pcs = cat.prod_unit[p] == 1
    ip = np.flatnonzero(pcs)
    ic = np.flatnonzero(~pcs)
    g = rng.geometric(cat.q_geom_p[p[ip]])
    q[ip] = np.minimum(g, cat.q_max[p[ip]]) * 1000
    x = rng.lognormal(cat.q_mu[p[ic]], cat.q_sigma[p[ic]])
    q[ic] = np.clip(np.rint(x * 1000).astype(np.int64), cat.q_lo_milli[p[ic]], cat.q_hi_milli[p[ic]])

    # 7) ціни й суми — тільки цілі числа
    unit = pr.unit[d, p]
    line = (q * unit + 500) // 1000
    total = np.add.reduceat(line, starts[:-1]) if n else np.zeros(0, dtype=np.int64)

    day_epoch = (date - EPOCH).days * 86400
    return DayData(
        receipt_id=world.offsets[d] + 1 + np.arange(n, dtype=np.int64),
        store_id=store.astype(np.int64) + 1, ts=day_epoch + sec, total_kop=total,
        item_start=starts.astype(np.int64), item_line_no=line_no, item_product_id=p + 1,
        item_q_milli=q, item_unit_kop=unit, item_line_kop=line,
    )
