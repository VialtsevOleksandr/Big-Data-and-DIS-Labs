"""Магазини, календарна модель (місяці/дні тижня/свята/години) і довжина чека."""
from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass

import numpy as np

from .catalog import largest_remainder
from .catalog_spec import CATEGORIES
from .config import MAX_LINES_PER_RECEIPT, STREAM_CALENDAR, STREAM_STORES, Config, rng_for

# ---------------------------------------------------------------------------
# Години роботи. УСІ ГОДИНИ — У UTC (конвенція проєкту: «годинник магазину»
# дорівнює UTC, без переходу на літній час). Ваги — відносна інтенсивність
# потоку покупців у годину [h, h+1).
# ---------------------------------------------------------------------------
_STD_WD = {6: .4, 7: .8, 8: 1.0, 9: 1.0, 10: 1.0, 11: 1.2, 12: 1.3, 13: 1.2, 14: 1.1, 15: 1.3,
           16: 1.7, 17: 1.9, 18: 1.6, 19: 1.1, 20: .6}
_STD_WE = {6: .3, 7: .6, 8: 1.0, 9: 1.4, 10: 1.6, 11: 1.6, 12: 1.5, 13: 1.4, 14: 1.3, 15: 1.3,
           16: 1.4, 17: 1.4, 18: 1.2, 19: .9, 20: .5}


def _vec(d: dict) -> np.ndarray:
    v = np.zeros(24)
    for h, w in d.items():
        v[h] = w
    return v


HOUR_PROFILES = {
    # тип: (робочі дні, вихідні, підпис)
    "standard": (_vec(_STD_WD), _vec(_STD_WE), "06:00–21:00"),
    "extended": (_vec({**_STD_WD, 5: .25, 21: .35}), _vec({**_STD_WE, 5: .15, 21: .3}), "05:00–22:00"),
    "24h": (_vec({**_STD_WD, 0: .12, 1: .08, 2: .06, 3: .06, 4: .1, 5: .25, 21: .45, 22: .3, 23: .2}),
            _vec({**_STD_WE, 0: .18, 1: .12, 2: .08, 3: .06, 4: .08, 5: .15, 21: .4, 22: .3, 23: .22}),
            "цілодобово"),
}
HOUR_TYPES = ("standard", "extended", "24h")
HOUR_TYPE_P = (0.6, 0.3, 0.1)

STORE_VISIT_SIGMA = 0.4        # лог-нормальна відвідуваність магазинів
STORE_PREF_SIGMA = 0.2         # вподобання магазину щодо категорій
STORE_PREF_CLIP = (0.6, 1.6)
LOCATIONS = (("житловий район", 1.12), ("змішана забудова", 1.0), ("діловий центр", 0.82))
LOCATION_P = (0.5, 0.3, 0.2)
NULL_COORD_SHARE = 0.04
NAME_WORDS = ("Колосок", "Сонячний", "Добрий Кошик", "Затишок", "Лелека", "Калина", "Вишневий", "Світанок",
              "Джерело", "Злагода", "Барвінок", "Смачний Двір", "Сусідський", "Ятрань", "Оберіг", "Дніпровий",
              "Мрія", "Каштан", "Пролісок", "Соняшник")


@dataclass
class Stores:
    rows: list[tuple]                 # (store_id, name, lat_micro|None, lon_micro|None, description|None)
    visit_weight: np.ndarray          # (S,)
    weekend_factor: np.ndarray        # (S,)
    hour_type: np.ndarray             # (S,) індекс у HOUR_TYPES
    cat_pref: np.ndarray              # (S, n_categories)


def build_stores(cfg: Config) -> Stores:
    rng = rng_for(cfg.seed, STREAM_STORES)
    S = cfg.n_stores
    visit = rng.lognormal(0.0, STORE_VISIT_SIGMA, S)
    htype = rng.choice(len(HOUR_TYPES), size=S, p=HOUR_TYPE_P)
    visit = visit * np.where(htype == 2, 1.3, 1.0)
    loc = rng.choice(len(LOCATIONS), size=S, p=LOCATION_P)
    wkend = np.array([LOCATIONS[i][1] for i in loc])
    pref = np.clip(rng.lognormal(0.0, STORE_PREF_SIGMA, (S, len(CATEGORIES))), *STORE_PREF_CLIP)

    centers = np.column_stack([rng.uniform(45.5, 51.5, 6), rng.uniform(23.5, 39.5, 6)])
    cl = rng.integers(0, 6, S)
    lat = centers[cl, 0] + rng.normal(0, 0.06, S)
    lon = centers[cl, 1] + rng.normal(0, 0.09, S)
    null_coord = rng.random(S) < NULL_COORD_SHARE
    has_desc = rng.random(S) < 0.5
    words = rng.integers(0, len(NAME_WORDS), S)
    rank = np.argsort(np.argsort(-visit, kind="stable"), kind="stable") / S   # 0 = найбільший

    rows = []
    for i in range(S):
        fmt = "Гіпермаркет" if rank[i] < 0.15 else ("Супермаркет" if rank[i] < 0.5 else "Магазин біля дому")
        name = f"{fmt} «{NAME_WORDS[words[i]]}» №{i + 1}"
        if null_coord[i]:
            la = lo = None
        else:
            la = int(np.rint(np.clip(lat[i], -90, 90) * 1e6))
            lo = int(np.rint(np.clip(lon[i], -180, 180) * 1e6))
        desc = (f"Синтетичний магазин. Формат: {fmt.lower()}; години роботи (UTC): "
                f"{HOUR_PROFILES[HOUR_TYPES[htype[i]]][2]}; локація: {LOCATIONS[loc[i]][0]}."
                if has_desc[i] else None)
        rows.append((i + 1, name, la, lo, desc))
    return Stores(rows=rows, visit_weight=visit, weekend_factor=wkend, hour_type=htype, cat_pref=pref)


# ---------------------------------------------------------------------------
# Календар
# ---------------------------------------------------------------------------
MONTH_FACTOR = np.array([0.92, 0.90, 0.97, 1.00, 1.02, 1.00, 1.00, 1.02, 0.98, 1.00, 1.03, 1.15])
WEEKDAY_FACTOR = np.array([0.93, 0.94, 0.96, 0.99, 1.08, 1.15, 0.95])   # Пн..Нд
DAY_NOISE_SIGMA = 0.04
FIXED_SPECIAL = {(1, 1): 0.55, (1, 2): 0.85, (1, 6): 1.15, (3, 7): 1.15, (12, 24): 1.20,
                 (12, 30): 1.35, (12, 31): 1.40}


def orthodox_easter(year: int) -> dt.date:
    """Алгоритм Міуса (юліанський календар) + 13 днів (коректно для 1900–2099)."""
    a, b, c = year % 4, year % 7, year % 19
    d = (19 * c + 15) % 30
    e = (2 * a + 4 * b - d + 34) % 7
    month = (d + e + 114) // 31
    day = (d + e + 114) % 31 + 1
    return dt.date(year, month, day) + dt.timedelta(days=13)


def special_factor(day: dt.date) -> float:
    f = FIXED_SPECIAL.get((day.month, day.day), 1.0)
    easter = orthodox_easter(day.year)
    if day == easter - dt.timedelta(days=1):
        f *= 1.25
    elif day == easter:
        f *= 0.85
    if day.month == 12 and 18 <= day.day <= 29:
        f *= 1.0 + 0.01 * (day.day - 17)        # передноворічне наростання
    return f


def day_weights(cfg: Config) -> np.ndarray:
    w = np.empty(cfg.n_days)
    for i in range(cfg.n_days):
        d = cfg.start + dt.timedelta(days=i)
        noise = rng_for(cfg.seed, STREAM_CALENDAR, d.toordinal()).lognormal(0.0, DAY_NOISE_SIGMA)
        w[i] = MONTH_FACTOR[d.month - 1] * WEEKDAY_FACTOR[d.weekday()] * special_factor(d) * noise
    return w


def day_counts(cfg: Config) -> np.ndarray:
    """Рівно cfg.n_receipts чеків, розподілених по днях пропорційно вагам."""
    return largest_remainder(day_weights(cfg), cfg.n_receipts)


# ---------------------------------------------------------------------------
# Довжина чека: суміш трьох «типів походу» (зміщені від'ємні біноміальні),
# обрізана до [1, 30] перенормуванням (без «стовпчика» на 30).
# ---------------------------------------------------------------------------
TRIP_MIX = (  # (вага, середнє додаткових позицій понад 1, r — параметр форми)
    (0.35, 1.6, 1.5),    # швидкий похід
    (0.44, 10.5, 4.0),   # звичайна закупівля
    (0.21, 22.0, 8.0),   # велика закупівля
)


def _negbin_pmf(k: np.ndarray, mean: float, r: float) -> np.ndarray:
    p = r / (r + mean)
    lg = np.array([math.lgamma(x + r) - math.lgamma(r) - math.lgamma(x + 1) for x in k])
    return np.exp(lg + r * math.log(p) + k * math.log1p(-p))


def basket_length_pmf() -> np.ndarray:
    """pmf довжини 1..30 (індекс 0 ↔ довжина 1)."""
    k = np.arange(MAX_LINES_PER_RECEIPT)          # додаткові позиції 0..29
    pmf = np.zeros(MAX_LINES_PER_RECEIPT)
    for w, mean, r in TRIP_MIX:
        comp = _negbin_pmf(k, mean, r)
        pmf += w * comp / comp.sum()
    return pmf / pmf.sum()


def expected_basket_length() -> float:
    pmf = basket_length_pmf()
    return float((np.arange(1, MAX_LINES_PER_RECEIPT + 1) * pmf).sum())
