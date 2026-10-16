from decimal import ROUND_HALF_UP, Decimal

import numpy as np
import pytest

from salesgen.money import (apply_discount_pct, fmt_kop, fmt_milli, line_amount_kop, parse_fixed,
                            snap_price)


def dec_line(q_milli: int, p_kop: int) -> int:
    """Незалежна реалізація через Decimal: ROUND(quantity × unit_price, 2) HALF_UP."""
    v = (Decimal(q_milli) / 1000) * (Decimal(p_kop) / 100)
    return int(v.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) * 100)


@pytest.mark.parametrize("q,p,expected", [
    (500, 1, 1),        # 0.500 × 0.01 = 0.005 → 0.01 (рівно половина копійки — вгору)
    (1500, 1, 2),       # 0.015 → 0.02
    (2500, 1, 3),       # 0.025 → 0.03 (не «банківське» до парного)
    (499, 1, 0),        # 0.00499 → 0.00
    (750, 4800, 3600),  # 0,750 кг × 48,00 = 36,00
    (2000, 3500, 7000), # 2 шт. × 35,00 = 70,00
    (333, 1999, 666),   # 0.333 × 19.99 = 6.65667 → 6.66
    (1, 1, 0),
])
def test_half_up_edges(q, p, expected):
    assert line_amount_kop(q, p) == expected == dec_line(q, p)


def test_half_up_matches_decimal_on_random_grid():
    rng = np.random.default_rng(1)
    q = rng.integers(1, 20_001, 20_000)
    p = rng.integers(0, 10_000_001, 20_000)
    got = line_amount_kop(q.astype(np.int64), p.astype(np.int64))
    for i in range(0, 20_000, 7):
        assert got[i] == dec_line(int(q[i]), int(p[i]))
    # проміжний добуток у межах int64 навіть на верхніх запасах
    assert line_amount_kop(np.int64(20_000), np.int64(10_000_000)) == 200_000_000


def test_discount_rounding():
    assert apply_discount_pct(4990, 15) == 4242     # 42.415 → 42.42
    assert apply_discount_pct(1000, 30) == 700


def test_snap_price():
    assert snap_price(4567, False) == 4570
    assert snap_price(13410, True) == 13399
    assert snap_price(13460, False) == 13500


def test_format_and_parse_roundtrip():
    for v in (0, 1, 99, 100, 123456):
        assert parse_fixed(fmt_kop(v), 2) == v
        assert parse_fixed(fmt_milli(v), 3) == v
    for bad in ("1.5", "1.500", "+1.50", "1e2", "nan", "inf", " 1.50", "01.50", "1,50", ""):
        with pytest.raises(ValueError):
            parse_fixed(bad, 2)
