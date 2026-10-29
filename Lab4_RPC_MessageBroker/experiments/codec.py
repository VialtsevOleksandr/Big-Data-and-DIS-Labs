"""Compare equivalent JSON and Protobuf payloads, excluding disk IO/model conversion."""
import argparse
import json

from experiments.common import (digest, evidence, load_dataset, load_protobuf, median,
                                require_runs, timed_map, write_csv, write_json)
from experiments.wire_dump import dump


def encode_json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def main():
    parser = argparse.ArgumentParser(description="Step 2: JSON versus Protocol Buffers")
    parser.add_argument("--dataset", default="data/orders.jsonl")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    require_runs(args.runs)
    path, records = load_dataset(args.dataset)
    pb, messages = load_protobuf(records)  # Conversion is explicitly outside timed regions.
    output = evidence(args.output_dir, "02_codec", dataset=str(path), dataset_sha256=digest(path))
    rows = []
    for run in range(-1, args.runs):
        formats = ("JSON", "Protobuf") if run % 2 else ("Protobuf", "JSON")
        for name in formats:
            if name == "JSON":
                payloads, encode_ms = timed_map(encode_json, records)
                decoded, decode_ms = timed_map(json.loads, payloads)
                if decoded != records:
                    raise RuntimeError("JSON roundtrip changed the data")
            else:
                payloads, encode_ms = timed_map(lambda value: value.SerializeToString(), messages)
                decoded, decode_ms = timed_map(pb.Order.FromString, payloads)
                if decoded != messages:
                    raise RuntimeError("Protobuf roundtrip changed the data")
            size = sum(map(len, payloads))
            label = "warmup" if run < 0 else f"run {run + 1}"
            print(f"{label:8} {name:9} bytes={size:10d} encode={encode_ms:10.3f} ms decode={decode_ms:10.3f} ms", flush=True)
            if run >= 0:
                rows.append(dict(run=run + 1, format=name, bytes=size, encode_ms=encode_ms, decode_ms=decode_ms))
            del decoded, payloads
    summaries = []
    for name in ("JSON", "Protobuf"):
        selected = [row for row in rows if row["format"] == name]
        summaries.append(dict(format=name, bytes=median([r["bytes"] for r in selected]),
                              encode_ms=median([r["encode_ms"] for r in selected]),
                              decode_ms=median([r["decode_ms"] for r in selected])))
    ratio = summaries[0]["bytes"] / summaries[1]["bytes"]
    summary = dict(count=len(records), warmup_runs=1, measured_runs=args.runs,
                   dataset_sha256=digest(path), json_to_protobuf_size_ratio=ratio,
                   formats=summaries, runs=rows,
                   methodology="Independent compact UTF-8 JSON payloads, no JSONL separators. Disk IO, dict-to-message conversion and equality checks excluded. JSON decode returns dicts; Protobuf decode returns generated Order objects. Timing includes output allocation. Format order alternates.")
    write_json(output / "codec_summary.json", summary)
    write_csv(output / "codec_runs.csv", rows)
    write_csv(output / "codec_medians.csv", summaries)
    index = min(range(len(messages)), key=lambda i: messages[i].ByteSize())
    small = messages[index]
    (output / "small_order.pb").write_bytes(small.SerializeToString())
    write_json(output / "small_order.json", records[index])
    (output / "small_order_hex.txt").write_text(dump(small), encoding="utf-8")
    print("\nSTEP 2: MEDIANS (warmup excluded)")
    print(f"Orders: {len(records)}; measured runs: {args.runs}")
    print(f"{'Format':10} {'Bytes':>12} {'Encode, ms':>14} {'Decode, ms':>14}")
    for row in summaries:
        print(f"{row['format']:10} {row['bytes']:12.0f} {row['encode_ms']:14.3f} {row['decode_ms']:14.3f}")
    print(f"JSON / Protobuf size: {ratio:.3f}x")
    print(f"Small order: {small.ByteSize()} bytes; annotated dump: small_order_hex.txt")
    print(f"Results: {output}")


if __name__ == "__main__":
    main()
