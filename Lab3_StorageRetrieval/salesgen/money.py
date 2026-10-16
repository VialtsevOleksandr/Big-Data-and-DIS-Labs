"""Точна цілочисельна арифметика грошей і кількостей.

Кількість зберігається як Q — тисячні частки облікової одиниці (int),
ціна — як P — копійки (int). Усі формули працюють і для Python int,
і для масивів numpy.int64 (переповнення неможливе: Q ≤ 20 000,
P ≤ 10 000 000 → Q·P ≤ 2·10^11 ≪ 9.2·10^18).
"""
from __future__ import annotations

MAX_QTY_MILLI = 20_000            # 20,000 облікових одиниць
MAX_PRICE_KOP = 10_000_000        # 100 000,00 UAH за одиницю


def line_amount_kop(q_milli, p_kop):
    """ROUND_HALF_UP(Q/1000 · P/100 UAH, 2) у копійках для невід'ємних Q, P."""
    return (q_milli * p_kop + 500) // 1000


def apply_discount_pct(p_kop, pct):
    """Ціна після знижки pct% з округленням до копійки HALF_UP."""
    return (p_kop * (100 - pct) + 50) // 100


def apply_bp(p_kop, bp):
    """Множення ціни на bp/10000 (базисні пункти) з округленням HALF_UP."""
    return (p_kop * bp + 5000) // 10000


def snap_price(p_kop, ending_99):
    """Приводить регулярну ціну до «цінової сітки»: < 100 UAH — кратно 0,10;
    ≥ 100 UAH — цілі гривні, а для товарів зі стилем «.99» — X.99."""
    import numpy as np
    p = np.asarray(p_kop, dtype=np.int64)
    small = np.maximum(10, (p + 5) // 10 * 10)
    whole = np.maximum(100, (p + 50) // 100 * 100)
    big = np.where(ending_99, whole - 1, whole)
    return np.where(p < 10000, small, big)


FRAC2 = [f"{i:02d}" for i in range(100)]
FRAC3 = [f"{i:03d}" for i in range(1000)]


def fmt_kop(v: int) -> str:
    """Копійки → '123.45' (невід'ємні значення)."""
    return f"{v // 100}.{FRAC2[v % 100]}"


def fmt_milli(v: int) -> str:
    """Тисячні → '1.250' (невід'ємні значення)."""
    return f"{v // 1000}.{FRAC3[v % 1000]}"


def fmt_micro_signed(v: int) -> str:
    """Мільйонні частки градуса → '-12.345678'."""
    sign = "-" if v < 0 else ""
    a = abs(v)
    return f"{sign}{a // 1_000_000}.{a % 1_000_000:06d}"


def parse_fixed(s: str, scale: int) -> int:
    """'12.340' → 12340 (scale=3). Суворо: рівно `scale` знаків після крапки,
    без знаку '+', експоненти, пробілів. Від'ємні значення допускаються
    лише з '-'. Повертає ціле в одиницях 10^-scale; ValueError інакше."""
    neg = s.startswith("-")
    body = s[1:] if neg else s
    ip, sep, fp = body.partition(".")
    if sep != "." or len(fp) != scale or not ip.isdigit() or not fp.isdigit() \
            or not ip.isascii() or not fp.isascii() or (len(ip) > 1 and ip[0] == "0"):
        raise ValueError(f"невірний формат числа зі scale={scale}: {s!r}")
    v = int(ip) * 10**scale + int(fp)
    return -v if neg else v
