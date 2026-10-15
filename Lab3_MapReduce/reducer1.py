import sys
from itertools import groupby


def reduce(key, values):
    return key, sum(values)


def main():
    pairs = (line.rstrip("\n").split("\t", 1) for line in sys.stdin)
    for key, group in groupby(pairs, key=lambda pair: pair[0]):
        key, total = reduce(key, (int(pair[1]) for pair in group))
        print(f"{key}\t{total}")


if __name__ == "__main__":
    main()
