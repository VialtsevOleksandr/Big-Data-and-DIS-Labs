"""
Генератор CSV-логу транзакцій для ДЗ з MapReduce.

Формат рядка:  user_id,timestamp,category,amount
Приклад:       1024,2026-01-03T14:22:00,groceries,45.20

Запуск:
    python generate_transactions.py 250 transactions.csv
        -> 250 000 рядків у файл transactions.csv

    python generate_transactions.py 500 big.csv --seed 7 --days 180 --max-mb 50

    # Новий режим: гості + довідник користувачів (старий режим не змінюється).
    python generate_transactions.py 5000 transactions.csv --max-mb 400 \
        --guest-percent 25 --users-output users.csv --users 9000 --cities 15

Аргументи:
    thousands   кількість рядків у ТИСЯЧАХ (250 = 250 000 рядків)
    output      назва вихідного CSV-файлу
    --seed      зерно генератора (для відтворюваності), за замовч. 42
    --start     перша дата періоду (YYYY-MM-DD), за замовч. 2026-01-01
    --days      тривалість періоду в днях, за замовч. 90
    --max-mb    ліміт розміру файлу в МБ, за замовч. 100;
                у старому режимі кількість рядків автоматично зменшується;
                у новому режимі перевищення оцінки зупиняє генерацію
    --guest-percent  новий режим: ціла частка гостей від 15 до 35 відсотків
    --users-output   новий режим: шлях до довідника користувачів
    --users          кількість користувачів нового режиму, за замовч. 9000
    --cities         кількість міст нового режиму, за замовч. 15 (від 10 до 20)
    --metadata-output  необов'язковий шлях до JSON параметрів нового режиму
"""

import argparse
import csv
import hashlib
import json
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

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
CITIES = (
    "Kyiv", "Lviv", "Odesa", "Dnipro", "Kharkiv", "Vinnytsia", "Poltava",
    "Chernihiv", "Cherkasy", "Sumy", "Zhytomyr", "Rivne", "Lutsk",
    "Ternopil", "Ivano-Frankivsk", "Uzhhorod", "Chernivtsi", "Khmelnytskyi",
    "Mykolaiv", "Kropyvnytskyi",
)


def parse_args():
    p = argparse.ArgumentParser(description="Генератор CSV транзакцій для MapReduce")
    p.add_argument("thousands", type=int, help="кількість рядків у тисячах (250 = 250 000)")
    p.add_argument("output", help="назва вихідного CSV-файлу")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start", default="2026-01-01")
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--max-mb", type=float, default=100.0)
    p.add_argument("--guest-percent", type=int, help="Новий режим: точна частка гостей, 15–35%%")
    p.add_argument("--users-output", help="Новий режим: CSV user_id,city,signup_date")
    p.add_argument("--users", type=int, default=9000, help="Зареєстровані користувачі нового режиму")
    p.add_argument("--cities", type=int, default=15, help="Кількість міст нового режиму, 10–20")
    p.add_argument("--metadata-output", help="JSON параметрів нового режиму; типово <output>.metadata.json")
    args = p.parse_args()

    if args.thousands <= 0:
        p.error("thousands має бути > 0")
    if args.days <= 0:
        p.error("days має бути > 0")
    if args.max_mb <= 0:
        p.error("max-mb має бути > 0")
    extended = args.guest_percent is not None or args.users_output is not None
    if extended:
        if args.guest_percent is None or args.users_output is None:
            p.error("Для нового режиму задайте разом --guest-percent і --users-output")
        if not 15 <= args.guest_percent <= 35:
            p.error("guest-percent має бути цілим числом від 15 до 35")
        if args.users < 1:
            p.error("users має бути > 0")
        if not 10 <= args.cities <= 20 or args.users < args.cities:
            p.error("Потрібно 10–20 міст і не менше користувачів, ніж міст")
        rows = args.thousands * 1000
        if rows - rows * args.guest_percent // 100 < args.users:
            p.error("Недостатньо негостьових транзакцій, щоб кожен користувач мав покупку")
        paths = [Path(args.output), Path(args.users_output), metadata_path(args)]
        if len({str(path.resolve()).casefold() for path in paths}) != len(paths):
            p.error("Шляхи транзакцій, довідника і метаданих мають відрізнятися")
        if rows * AVG_ROW_BYTES > args.max_mb * 1024 * 1024:
            p.error("Оцінка розміру перевищує --max-mb. Збільште ліміт; новий режим не скорочує дані")
    elif args.metadata_output is not None or args.users != 9000 or args.cities != 15:
        p.error("Нові параметри потребують --guest-percent і --users-output")
    return args


def metadata_path(args):
    return Path(args.metadata_output or str(Path(args.output).with_suffix(".metadata.json")))


def file_info(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return {"path": str(path.resolve()), "bytes": path.stat().st_size, "sha256": digest.hexdigest()}


def generate_extended(args):
    """Окрема гілка, щоб не змінювати послідовність RNG старого генератора."""
    rnd = random.Random(args.seed)
    rows = args.thousands * 1000
    guest_rows = rows * args.guest_percent // 100
    guests_left = guest_rows
    registered_seen = 0
    start = datetime.strptime(args.start, "%Y-%m-%d")
    period_sec = args.days * 24 * 60 * 60
    output = Path(args.output)
    users_output = Path(args.users_output)
    metadata_output = metadata_path(args)
    for path in (output, users_output, metadata_output):
        path.parent.mkdir(parents=True, exist_ok=True)
    names = list(CATEGORIES)
    weights = [CATEGORIES[c][0] for c in names]
    with output.open("w", encoding="utf-8", newline="") as stream:
        stream.write("user_id,timestamp,category,amount\n")
        for offset in range(0, rows, CHUNK):
            n = min(CHUNK, rows - offset)
            cats = rnd.choices(names, weights=weights, k=n)
            lines = []
            for local_index, cat in enumerate(cats):
                index = offset + local_index
                # Послідовна вибірка без повернення дає ТОЧНУ частку гостей.
                guest = rnd.randrange(rows - index) < guests_left
                if guest:
                    user_id = 0
                    guests_left -= 1
                else:
                    # Кожен зареєстрований користувач гарантовано присутній.
                    user_id = (USER_ID_MIN + registered_seen if registered_seen < args.users
                               else rnd.randrange(USER_ID_MIN, USER_ID_MIN + args.users))
                    registered_seen += 1
                # Гарантуємо всі категорії та повний заданий період.
                if index < len(names):
                    cat = names[index]
                second = (0 if index == 0 else period_sec - 1 if index == 1
                          else rnd.randrange(period_sec))
                timestamp = start + timedelta(seconds=second)
                _, lo, hi = CATEGORIES[cat]
                amount = rnd.uniform(lo, hi)
                lines.append(f"{user_id},{timestamp:%Y-%m-%dT%H:%M:%S},{cat},{amount:.2f}\n")
            stream.writelines(lines)
            print(f"\r  записано {offset + n:,} / {rows:,}", end="", file=sys.stderr)

    # Окремий RNG: довідник не змінює згенеровані транзакції.
    users_rnd = random.Random(args.seed)
    cities = [CITIES[index % args.cities] for index in range(args.users)]
    users_rnd.shuffle(cities)
    with users_output.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(("user_id", "city", "signup_date"))
        for index, city in enumerate(cities):
            signup = start - timedelta(days=users_rnd.randint(1, 730))
            writer.writerow((USER_ID_MIN + index, city, signup.date().isoformat()))

    metadata = {
        "mode": "with_guests_and_users", "seed": args.seed,
        "transaction_rows": rows, "registered_users": args.users,
        "guest_percent": args.guest_percent, "guest_rows": guest_rows,
        "registered_transaction_rows": rows - guest_rows,
        "categories": names, "cities": list(CITIES[:args.cities]),
        "start": start.date().isoformat(), "days": args.days,
        "transactions": file_info(output), "users": file_info(users_output),
        "meets_lab_minimums": rows >= 5_000_000 and args.users >= 5000 and args.days >= 90,
    }
    metadata_output.write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("\n" + json.dumps(metadata, ensure_ascii=False, indent=2), flush=True)
    print(f"Metadata: {metadata_output.resolve()}", flush=True)


def main():
    args = parse_args()
    if args.guest_percent is not None:
        generate_extended(args)
        return
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
