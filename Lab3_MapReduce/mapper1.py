import csv
import sys

from amounts import to_cents


def map_transaction(row):
    if row == ["user_id", "timestamp", "category", "amount"]:
        return None
    user_id, timestamp, category, amount = row
    date = timestamp.split("T", 1)[0]
    return f"{date}|{category}", to_cents(amount)


def main():
    for row in csv.reader(sys.stdin):
        pair = map_transaction(row)
        if pair is not None:
            key, cents = pair
            print(f"{key}\t{cents}")


if __name__ == "__main__":
    main()
