"""Перцентилі та зведення вимірів."""
from __future__ import annotations

import math
import statistics


def percentile(sorted_values: list[float], p: float) -> float | None:
    """Перцентиль методом найближчого рангу: найменше значення, не менше за p % вибірки."""
    if not sorted_values:
        return None
    rank = max(1, math.ceil(p / 100.0 * len(sorted_values)))
    return sorted_values[rank - 1]


def summarize(values, digits: int = 3) -> dict:
    """n, min, p50, p95, p99, max, mean для списку чисел (None пропускаються)."""
    data = sorted(float(v) for v in values if v is not None)
    if not data:
        return {"n": 0}
    return {
        "n": len(data),
        "min": round(data[0], digits),
        "p50": round(percentile(data, 50), digits),
        "p95": round(percentile(data, 95), digits),
        "p99": round(percentile(data, 99), digits),
        "max": round(data[-1], digits),
        "mean": round(statistics.fmean(data), digits),
    }
