import sys
from itertools import groupby

from amounts import format_cents


def reduce(category, values):
    return category, sum(values)


def main():
    winner = None
    pairs = (line.rstrip("\n").split("\t", 1) for line in sys.stdin)
    for category, group in groupby(pairs, key=lambda pair: pair[0]):
        candidate = reduce(category, (int(pair[1]) for pair in group))
        if winner is None or candidate[1] > winner[1] or (
            candidate[1] == winner[1] and candidate[0] < winner[0]
        ):
            winner = candidate
    if winner is not None:
        print(f"{winner[0]}\t{format_cents(winner[1])}")


if __name__ == "__main__":
    main()
