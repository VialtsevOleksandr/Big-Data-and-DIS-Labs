"""Модель цін: базова ціна → кілька планових змін → сезонний індекс → акції.

Результат — матриці [день, товар] у копійках (int64):
  regular — регулярна ціна (без акції);
  unit    — фактична ціна продажу (після товарної знижки, якщо діє акція);
  promo_pct — відсоток знижки (0 = немає акції).
Ціна не залежить від магазину (одна цінова політика мережі).
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import numpy as np

from .catalog import Catalog
from .config import STREAM_PRICES, Config, rng_for
from .money import MAX_PRICE_KOP, apply_bp, apply_discount_pct, snap_price

# Регулярні зміни цін
N_CHANGES_P = (0.15, 0.40, 0.30, 0.15)     # P(0,1,2,3 зміни за рік)
CHANGE_MEAN_BP, CHANGE_SD_BP = 350, 250    # +3,5 % ± 2,5 % за одну зміну
CHANGE_MIN_BP, CHANGE_MAX_BP = -600, 1200
# Сезонний індекс свіжих овочів/фруктів за місяцями (bp, 10000 = 1.0)
SEASON_BP = np.array([11000, 11500, 12000, 12500, 11800, 10500, 9200, 8500, 8500, 9000, 9800, 10500])
SEASON_AMP_RANGE = (0.4, 1.0)              # амплітуда сезонності конкретного товару
# Акції
PROMO_SHARE = 0.30                         # частка товарів, що мають хоча б одну акцію
PROMO_WINDOWS_P = (0.55, 0.30, 0.15)       # 1, 2, 3 вікна акцій
PROMO_LEN = (7, 14)                        # тривалість, днів (включно)
PROMO_PCT = np.array([10, 15, 20, 25, 30])
PROMO_PCT_P = np.array([0.25, 0.30, 0.25, 0.12, 0.08])
PROMO_DEMAND_PER_PCT = 0.03                # +3 % попиту за кожен відсоток знижки


@dataclass
class Prices:
    regular: np.ndarray       # (n_days, n_products) int64
    unit: np.ndarray          # (n_days, n_products) int64
    promo_pct: np.ndarray     # (n_days, n_products) int16
    current_kop: np.ndarray   # (n_products,) регулярна ціна в останній день періоду

    def demand_boost(self, day: int) -> np.ndarray:
        return 1.0 + PROMO_DEMAND_PER_PCT * self.promo_pct[day].astype(np.float64)


def build_prices(cfg: Config, cat: Catalog) -> Prices:
    rng = rng_for(cfg.seed, STREAM_PRICES)
    D, P = cfg.n_days, cat.n_products
    days = np.arange(D)

    regular = np.empty((D, P), dtype=np.int64)
    n_changes = rng.choice(4, size=P, p=N_CHANGES_P)
    e99 = cat.prod_base_kop % 100 == 99           # стиль цінника товару (.99 чи цілі)
    for p in range(P):
        k = int(min(n_changes[p], max(D - 1, 0)))
        level = int(cat.prod_base_kop[p])
        col = np.full(D, level, dtype=np.int64)
        if k:
            cdays = np.sort(rng.choice(np.arange(1, D), size=k, replace=False))
            deltas = np.clip(np.rint(rng.normal(CHANGE_MEAN_BP, CHANGE_SD_BP, k)), CHANGE_MIN_BP, CHANGE_MAX_BP)
            for cd, dbp in zip(cdays, deltas.astype(np.int64)):
                level = int(snap_price(apply_bp(level, 10000 + int(dbp)), e99[p]))
                col[cd:] = level
        regular[:, p] = col

    # сезонність: індекс місяця кожного дня
    o0 = cfg.start.toordinal()
    month_idx = np.array([dt.date.fromordinal(o0 + int(d)).month - 1 for d in days])
    amp = rng.uniform(*SEASON_AMP_RANGE, P)
    seasonal_prod = cat.type_seasonal[cat.prod_type]
    for p in np.flatnonzero(seasonal_prod):
        bp = 10000 + np.rint(amp[p] * (SEASON_BP[month_idx] - 10000)).astype(np.int64)
        regular[:, p] = snap_price(apply_bp(regular[:, p], bp), e99[p])
    np.clip(regular, 1, MAX_PRICE_KOP, out=regular)

    promo = np.zeros((D, P), dtype=np.int16)
    has_promo = rng.random(P) < PROMO_SHARE
    n_win = rng.choice(3, size=P, p=PROMO_WINDOWS_P) + 1
    for p in np.flatnonzero(has_promo):
        for _ in range(int(n_win[p])):
            start = int(rng.integers(0, D))
            length = int(rng.integers(PROMO_LEN[0], PROMO_LEN[1] + 1))
            pct = int(rng.choice(PROMO_PCT, p=PROMO_PCT_P))
            seg = promo[start:start + length, p]
            np.maximum(seg, pct, out=seg)

    unit = apply_discount_pct(regular, promo.astype(np.int64))
    return Prices(regular=regular, unit=unit, promo_pct=promo, current_kop=regular[D - 1].copy())
