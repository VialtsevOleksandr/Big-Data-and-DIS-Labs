"""Точне перетворення сум між десятковим записом і цілими копійками."""

from decimal import Decimal


def to_cents(text):
    amount = Decimal(text) * 100
    if not amount.is_finite() or amount != amount.to_integral_value():
        raise ValueError("Amount must contain a finite whole number of cents")
    return int(amount)


def format_cents(cents):
    sign = "-" if cents < 0 else ""
    whole, fraction = divmod(abs(cents), 100)
    return f"{sign}{whole}.{fraction:02d}"
