"""Побудова конкретного каталогу (категорії, типи, одиниці, товари) з seed."""
from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .catalog_spec import CATEGORIES, LINE_PRICE, MISSIONS, PRODUCE_LINES, TIER_WEIGHT
from .config import STREAM_CATALOG, Config, rng_for

UNITS = ((1, "pcs", "штука", 0), (2, "kg", "кілограм", 3), (3, "l", "літр", 3))
UNIT_ID = {"pcs": 1, "kg": 2, "l": 3}

# Параметри нерівномірної популярності типів і конкретних товарів.
TYPE_NOISE_SIGMA = 0.30      # лог-нормальний шум ваги типу навколо рівня A/B/C/D
ZIPF_S_WITHIN_TYPE = 0.9     # Zipf-показник для рангу товару в межах типу
PRODUCT_NOISE_SIGMA = 0.25   # лог-нормальний шум ваги товару
BRAND_PREMIUM_SIGMA = 0.15   # розкид цінового позиціонування брендів
VARIANT_PRICE_SIGMA = 0.08   # розкид ціни конкретного товару


def largest_remainder(weights: np.ndarray, total: int, minimum: int = 0) -> np.ndarray:
    """Цілі частки, що в сумі дають рівно `total` (метод Гамільтона).
    Нічиї розв'язуються за індексом — детерміновано."""
    w = np.asarray(weights, dtype=np.float64)
    n = len(w)
    if minimum * n > total:
        raise ValueError("мінімум не вміщується в total")
    rest = total - minimum * n
    quota = rest * w / w.sum()
    base = np.floor(quota).astype(np.int64)
    short = rest - int(base.sum())
    frac = quota - base
    order = np.lexsort((np.arange(n), -frac))       # більша дробова частина, потім менший індекс
    base[order[:short]] += 1
    return base + minimum


@dataclass
class Catalog:
    # таблиці (списки рядків для CSV)
    categories: list[tuple]
    product_types: list[tuple]
    units: tuple
    products: list[tuple]              # без current_price — він з'являється після моделі цін
    # внутрішні масиви, індекс = product_id - 1
    prod_type: np.ndarray              # індекс типу (0-based)
    prod_unit: np.ndarray              # unit_id
    prod_base_kop: np.ndarray          # базова регулярна ціна на початок періоду, коп.
    prod_weight_in_type: np.ndarray    # відносна популярність у межах типу
    q_geom_p: np.ndarray
    q_max: np.ndarray
    q_mu: np.ndarray
    q_sigma: np.ndarray
    q_lo_milli: np.ndarray
    q_hi_milli: np.ndarray
    # типи, індекс = product_type_id - 1
    type_keys: list[str]
    type_category: np.ndarray          # індекс категорії (0-based)
    type_weight: np.ndarray            # базова популярність типу
    type_seasonal: np.ndarray
    type_first_product: np.ndarray     # товари типу t: [first[t], first[t+1])
    mission_types: np.ndarray          # (M, 4) індекси типів, -1 = порожньо
    mission_keep: np.ndarray           # (M, 4)
    mission_weight: np.ndarray         # (M,)
    mission_names: list[str]

    @property
    def n_products(self) -> int:
        return len(self.prod_type)

    @property
    def n_types(self) -> int:
        return len(self.type_keys)


def n_spec_types() -> int:
    return sum(len(c.types) for c in CATEGORIES)


def _round_price(raw_uah: float, ending_99: bool) -> int:
    kop = int(np.rint(raw_uah * 100))
    if raw_uah < 100:
        step = 10
    elif raw_uah < 1000:
        step = 100
    else:
        step = 1000
    kop = max(step, int(np.rint(kop / step)) * step)
    if ending_99 and step >= 100:
        kop -= 1                                # 129.99, 1 249.99
    return kop


def build_catalog(cfg: Config) -> Catalog:
    rng = rng_for(cfg.seed, STREAM_CATALOG)
    types = [(ci, t) for ci, c in enumerate(CATEGORIES) for t in c.types]
    n_types = len(types)

    categories = [(ci + 1, c.name, c.description) for ci, c in enumerate(CATEGORIES)]
    product_types = []
    for ti, (ci, t) in enumerate(types):
        desc = None if (ti % 3 == 2) else f"Тип товарів «{t.name}» у категорії «{CATEGORIES[ci].name}»"
        product_types.append((ti + 1, ci + 1, t.name, desc))

    type_weight = np.array([TIER_WEIGHT[t.tier] for _, t in types]) * rng.lognormal(0.0, TYPE_NOISE_SIGMA, n_types)
    breadth = np.array([t.breadth for _, t in types])
    per_type = largest_remainder(breadth, cfg.n_products, minimum=1)

    brand_premium = {}
    for c in CATEGORIES:
        for b in c.brands:
            brand_premium[(c.key, b)] = float(rng.lognormal(0.0, BRAND_PREMIUM_SIGMA))

    products, ptype, punit, pbase, pw = [], [], [], [], []
    qp, qmax, qmu, qsig, qlo, qhi = [], [], [], [], [], []
    first = np.zeros(n_types + 1, dtype=np.int64)
    pid = 0
    for ti, (ci, t) in enumerate(types):
        cat = CATEGORIES[ci]
        first[ti] = pid
        k = int(per_type[ti])
        makers = cat.brands if (t.branded and cat.brands) else PRODUCE_LINES
        combos = [(v, m, pk) for v in t.variants for m in makers for pk in t.packs]
        cw = np.array([pk.weight for _, _, pk in combos], dtype=np.float64)
        take = min(k, len(combos))
        chosen = rng.choice(len(combos), size=take, replace=False, p=cw / cw.sum())
        chosen = list(chosen) + [chosen[i % take] for i in range(k - take)]   # запасний варіант для великих n_products
        ranks = rng.permutation(k)
        noise = rng.lognormal(0.0, PRODUCT_NOISE_SIGMA, k)
        vprice = rng.lognormal(0.0, VARIANT_PRICE_SIGMA, k)
        endings = rng.random(k) < 0.6
        has_desc = rng.random(k) < 0.65
        for j in range(k):
            v, maker, pk = combos[chosen[j]]
            if t.branded and cat.brands:
                name = f"{t.name} {v} «{maker}», {pk.label}"
                premium = brand_premium[(cat.key, maker)]
            else:
                name = f"{t.name} {v}{' ' + maker if maker else ''}, {pk.label}"
                premium = LINE_PRICE[maker]
            if j >= take:
                name += f", серія {j - take + 2}"
            raw = t.base * pk.size * pk.factor * premium * vprice[j]
            pid += 1
            desc = (f"Синтетичний товар. Категорія: {cat.name}; тип: {t.name}; фасування: {pk.label}."
                    if has_desc[j] else None)
            products.append((pid, ti + 1, UNIT_ID[pk.unit], name, desc))
            ptype.append(ti)
            punit.append(UNIT_ID[pk.unit])
            pbase.append(_round_price(raw, bool(endings[j])))
            pw.append((ranks[j] + 1.0) ** (-ZIPF_S_WITHIN_TYPE) * noise[j] * pk.weight)
            if pk.unit == "pcs":
                qp.append(pk.geom_p); qmax.append(pk.max_q)
                qmu.append(0.0); qsig.append(0.0); qlo.append(1000); qhi.append(pk.max_q * 1000)
            else:
                qp.append(0.0); qmax.append(0)
                qmu.append(float(np.log(pk.median))); qsig.append(pk.sigma)
                qlo.append(int(round(pk.lo * 1000))); qhi.append(int(round(pk.hi * 1000)))
    first[n_types] = pid
    assert pid == cfg.n_products

    key_to_idx = {t.key: i for i, (_, t) in enumerate(types)}
    M = len(MISSIONS)
    mt = -np.ones((M, 4), dtype=np.int64)
    mk = np.zeros((M, 4))
    for mi, m in enumerate(MISSIONS):
        for j, (tk, kp) in enumerate(zip(m.types, m.keep)):
            mt[mi, j] = key_to_idx[tk]
            mk[mi, j] = kp

    return Catalog(
        categories=categories, product_types=product_types, units=UNITS, products=products,
        prod_type=np.array(ptype, dtype=np.int64), prod_unit=np.array(punit, dtype=np.int64),
        prod_base_kop=np.array(pbase, dtype=np.int64), prod_weight_in_type=np.array(pw),
        q_geom_p=np.array(qp), q_max=np.array(qmax, dtype=np.int64), q_mu=np.array(qmu),
        q_sigma=np.array(qsig), q_lo_milli=np.array(qlo, dtype=np.int64), q_hi_milli=np.array(qhi, dtype=np.int64),
        type_keys=[t.key for _, t in types], type_category=np.array([ci for ci, _ in types], dtype=np.int64),
        type_weight=type_weight, type_seasonal=np.array([t.seasonal for _, t in types]),
        type_first_product=first, mission_types=mt, mission_keep=mk,
        mission_weight=np.array([m.weight for m in MISSIONS]), mission_names=[m.name for m in MISSIONS],
    )
