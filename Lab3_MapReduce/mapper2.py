import sys


def map_daily_total(line):
    composite_key, cents = line.rstrip("\n").split("\t", 1)
    date, category = composite_key.split("|", 1)
    return category, int(cents)


def main():
    for line in sys.stdin:
        category, cents = map_daily_total(line)
        print(f"{category}\t{cents}")


if __name__ == "__main__":
    main()
