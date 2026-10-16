"""Конфігурація запуску та детермінована схема RNG-потоків."""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import asdict, dataclass, fields
from pathlib import Path

import numpy as np

GENERATOR_VERSION = "1.0.1"
DEFAULT_SEED = 20050406

SMALLINT_MAX = 32767
INTEGER_MAX = 2_147_483_647
MAX_LINES_PER_RECEIPT = 30          # верхня межа довжини чека (схема/ТЗ)


@dataclass(frozen=True)
class Config:
    seed: int = DEFAULT_SEED
    n_receipts: int = 3_000_000
    n_stores: int = 100
    n_products: int = 2_000
    start_date: str = "2025-01-01"     # включно, 00:00:00 UTC
    end_date: str = "2026-01-01"       # виключно, 00:00:00 UTC
    batch_size: int = 50_000           # чеків на один буфер запису (на дані НЕ впливає)

    # ---- похідні значення ----
    @property
    def start(self) -> dt.date:
        return dt.date.fromisoformat(self.start_date)

    @property
    def end(self) -> dt.date:
        return dt.date.fromisoformat(self.end_date)

    @property
    def n_days(self) -> int:
        return (self.end - self.start).days

    def data_params(self) -> dict:
        """Параметри, від яких залежать дані (batch_size сюди не входить)."""
        d = asdict(self)
        d.pop("batch_size")
        return d

    def validate(self, n_types: int) -> None:
        if self.n_days < 1:
            raise ValueError("end_date має бути пізніше за start_date")
        if not (1 <= self.n_stores <= SMALLINT_MAX):
            raise ValueError("n_stores поза діапазоном SMALLINT")
        if not (n_types <= self.n_products <= SMALLINT_MAX):
            raise ValueError(f"n_products має бути в [{n_types}, {SMALLINT_MAX}] (кожен тип має ≥1 товар)")
        if not (self.n_stores <= self.n_receipts <= INTEGER_MAX):
            raise ValueError("n_receipts має бути ≥ n_stores (кожен магазин має продажі) і ≤ INTEGER")
        if self.batch_size < 1:
            raise ValueError("batch_size ≥ 1")
        if not (0 <= self.seed < 2**63):
            raise ValueError("seed має бути невід'ємним цілим < 2^63")

    @staticmethod
    def from_json(path: str | Path) -> "Config":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        known = {f.name for f in fields(Config)}
        unknown = set(raw) - known - {"_comment"}
        if unknown:
            raise ValueError(f"Невідомі ключі конфігурації: {sorted(unknown)}")
        return Config(**{k: v for k, v in raw.items() if k in known})


# ---------------------------------------------------------------------------
# Детермінована схема RNG: один кореневий seed + spawn_key = (потік, підключ).
# Потоки незалежні, тому зміна, наприклад, кількості споживань в одному дні
# не зсуває випадкові числа інших днів або каталогу.
# ---------------------------------------------------------------------------
STREAM_CATALOG = 1     # каталог: назви, ціни, популярність
STREAM_STORES = 2      # магазини: відвідуваність, вподобання, години
STREAM_CALENDAR = 3    # шум денних ваг (ключ — ординал дати)
STREAM_PRICES = 4      # зміни цін і акції
STREAM_COVERAGE = 5    # гарантоване покриття магазинів і товарів
STREAM_DAY = 10        # генерація чеків дня (ключ — ординал дати)
STREAM_STATS = 20      # незалежна випадкова вибірка для перевірки кошиків


def rng_for(seed: int, stream: int, *key: int) -> np.random.Generator:
    ss = np.random.SeedSequence(entropy=seed, spawn_key=(stream, *key))
    return np.random.Generator(np.random.PCG64(ss))


PROFILES = {
    "full": Config(),
    "pilot": Config(n_receipts=100_000),
    "small": Config(n_receipts=10_000),
}
