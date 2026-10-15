"""
Генератор CSV-логу транзакцій для ДЗ з MapReduce.

Формат рядка:  user_id,timestamp,category,amount
Приклад:       1024,2026-01-03T14:22:00,groceries,45.20

Запуск:
    python generate_transactions.py 250 transactions.csv
        -> 250 000 рядків у файл transactions.csv

    python generate_transactions.py 500 big.csv --seed 7 --days 180 --max-mb 50

Аргументи:
    thousands   кількість рядків у ТИСЯЧАХ (250 = 250 000 рядків)
    output      назва вихідного CSV-файлу
    --seed      зерно генератора (для відтворюваності), за замовч. 42
    --start     перша дата періоду (YYYY-MM-DD), за замовч. 2026-01-01
    --days      тривалість періоду в днях, за замовч. 90
    --max-mb    ліміт розміру файлу в МБ, за замовч. 100;
                якщо оцінка більша — кількість рядків автоматично зменшується
"""

import argparse
import random
import sys
from datetime import datetime, timedelta

# категорія: (вага частоти, мін. сума, макс. сума)
CATEGORIES = {
    "groceries":     (30, 3.00, 150.00),
    "restaurants":   (15, 8.00, 120.00),
    "transport":     (14, 1.50, 60.00),
    "clothing":      (8, 15.00, 300.00),
    "electronics":   (5, 20.00, 1500.00),
    "books":         (6, 5.00, 60.00),
    "entertainment": (8, 5.00, 100.00),
    "health":        (6, 5.00, 250.00),
    "utilities":     (4, 20.00, 200.00),
    "travel":        (4, 50.00, 2000.00),
}

USER_ID_MIN, USER_ID_MAX = 1000, 9999
AVG_ROW_BYTES = 45          # орієнтовна довжина одного рядка разом із '\n'
CHUNK = 50_000              # скільки рядків писати за раз


def parse_args():
    p = argparse.ArgumentParser(description="Генератор CSV транзакцій для MapReduce")
    p.add_argument("thousands", type=int, help="кількість рядків у тисячах (250 = 250 000)")
    p.add_argument("output", help="назва вихідного CSV-файлу")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start", default="2026-01-01")
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--max-mb", type=float, default=100.0)
    args = p.parse_args()

    if args.thousands <= 0:
        p.error("thousands має бути > 0")
    if args.days <= 0:
        p.error("days має бути > 0")
    return args


def main():
    args = parse_args()
    rnd = random.Random(args.seed)

    rows = args.thousands * 1000

    # Обмеження за розміром
    max_bytes = args.max_mb * 1024 * 1024
    max_rows = int(max_bytes // AVG_ROW_BYTES)
    if rows > max_rows:
        print(f"[!] {rows:,} рядків ≈ {rows * AVG_ROW_BYTES / 1024 / 1024:.0f} МБ > {args.max_mb:g} МБ. "
              f"Зменшую до {max_rows:,} рядків.", file=sys.stderr)
        rows = max_rows

    start = datetime.strptime(args.start, "%Y-%m-%d")
    period_sec = args.days * 24 * 60 * 60

    names = list(CATEGORIES)
    weights = [CATEGORIES[c][0] for c in names]

    written = 0
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        f.write("user_id,timestamp,category,amount\n")
        while written < rows:
            n = min(CHUNK, rows - written)
            cats = rnd.choices(names, weights=weights, k=n)
            lines = []
            for cat in cats:
                _, lo, hi = CATEGORIES[cat]
                user_id = rnd.randint(USER_ID_MIN, USER_ID_MAX)
                ts = start + timedelta(seconds=rnd.randrange(period_sec))
                amount = rnd.uniform(lo, hi)
                lines.append(f"{user_id},{ts:%Y-%m-%dT%H:%M:%S},{cat},{amount:.2f}\n")
            f.writelines(lines)
            written += n
            print(f"\r  записано {written:,} / {rows:,}", end="", file=sys.stderr)

    size_mb = 0
    try:
        import os
        size_mb = os.path.getsize(args.output) / 1024 / 1024
    except OSError:
        pass
    print(f"\nГотово: {args.output} — {written:,} рядків, {size_mb:.1f} МБ", file=sys.stderr)


if __name__ == "__main__":
    main()