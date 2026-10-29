"""Generate a reproducible JSONL dataset; never invoked automatically."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random
from uuid import UUID

from experiments.common import digest, evidence, load_dataset, write_json


def make_orders(count, seed):
    rng = random.Random(seed)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    cities = ("Київ", "Львів", "Одеса", "Дніпро", "Харків")
    comments = ("Передзвоніть перед доставкою", "Подарункове пакування", "Доставка після 18:00")
    for index in range(count):
        order = dict(
            order_id=str(UUID(int=rng.getrandbits(128), version=4)),
            buyer_id=f"buyer-{rng.randrange(1, 501):04d}",
            created_at=(start + timedelta(seconds=index)).isoformat().replace("+00:00", "Z"),
            status="ORDER_STATUS_PENDING",
            shipping_address=dict(country="UA", city=rng.choice(cities),
                                  street=f"вул. Університетська, {rng.randrange(1, 201)}",
                                  postal_code=f"{rng.randrange(1000, 99999):05d}"),
            items=[dict(product_id=f"sku-{rng.randrange(1, 1001):04d}",
                        quantity=rng.randrange(1, 11), unit_price_minor=rng.randrange(100, 100001))
                   for _ in range(rng.randrange(1, 21))],
        )
        if rng.random() < 0.5:
            order["comment"] = rng.choice(comments)
        yield order


def main():
    parser = argparse.ArgumentParser(description="Step 1: fixed-seed order dataset")
    parser.add_argument("--seed", type=int, default=20050406)
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--dataset", default="data/orders.jsonl")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if args.count < 10000:
        parser.error("--count must be at least 10000")
    output = evidence(args.output_dir, "01_dataset", seed=args.seed, requested_count=args.count)
    dataset = Path(args.dataset).resolve()
    dataset.parent.mkdir(parents=True, exist_ok=True)
    # No wall-clock timestamps or random UUID generators enter the data itself.
    with dataset.open("w", encoding="utf-8", newline="\n") as stream:
        for order in make_orders(args.count, args.seed):
            stream.write(json.dumps(order, ensure_ascii=False, separators=(",", ":")) + "\n")
    _, records = load_dataset(dataset)
    counts = [len(order["items"]) for order in records]
    with_comment = sum("comment" in order for order in records)
    summary = dict(seed=args.seed, count=len(records), unique_order_ids=len({r["order_id"] for r in records}),
                   min_items=min(counts), max_items=max(counts), total_items=sum(counts),
                   with_comment=with_comment, without_comment=len(records) - with_comment,
                   dataset=str(dataset), sha256=digest(dataset), file_bytes=dataset.stat().st_size,
                   currency="UAH", price_unit="kopeck", json_encoding="compact UTF-8, no ASCII escaping")
    write_json(output / "dataset_summary.json", summary)
    write_json(dataset.with_suffix(".manifest.json"), summary)
    write_json(output / "sample_order.json", records[0])
    print("\nSTEP 1: REPRODUCIBLE DATASET")
    for key in ("seed", "count", "unique_order_ids", "min_items", "max_items", "total_items", "with_comment", "without_comment"):
        print(f"{key:22} {summary[key]}")
    print(f"SHA256: {summary['sha256']}")
    print(f"Dataset: {dataset}")
    print(f"Results: {output}")


if __name__ == "__main__":
    main()
