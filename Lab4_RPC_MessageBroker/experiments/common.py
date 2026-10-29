"""Shared input validation, timing and evidence helpers (no work on import)."""
import csv
import hashlib
from importlib import metadata
import json
from pathlib import Path
import platform
import statistics
import time

ROOT = Path(__file__).resolve().parents[1]


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_csv(path, rows):
    if rows:
        with Path(path).open("w", encoding="utf-8-sig", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def evidence(output_dir, experiment, **details):
    output = Path(output_dir).resolve()
    output.mkdir(parents=True, exist_ok=True)
    versions = {}
    for package in ("grpcio", "grpcio-tools", "protobuf", "pika"):
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = "not installed"
    write_json(output / "environment.json", dict(
        experiment=experiment, utc_time=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        python=platform.python_version(), platform=platform.platform(), libraries=versions,
        schema_v1_sha256=digest(ROOT / "schemas/v1/orders.proto"), **details,
    ))
    return output


def load_dataset(path):
    path = Path(path).resolve()
    with path.open(encoding="utf-8") as stream:
        records = [json.loads(line) for line in stream if line.strip()]
    if len(records) < 10000:
        raise ValueError("Dataset must contain at least 10000 orders. Run 01-generate-data.ps1.")
    ids = [record["order_id"] for record in records]
    if len(set(ids)) != len(ids):
        raise ValueError("Dataset contains duplicate order IDs")
    for record in records:
        if not 1 <= len(record["items"]) <= 20:
            raise ValueError("Every order must have between 1 and 20 items")
    present = sum("comment" in record for record in records)
    if present == 0 or present == len(records):
        raise ValueError("Dataset must contain orders both with and without comments")
    return path, records


def load_protobuf(records):
    from google.protobuf.json_format import ParseDict
    from server.generated.v1 import orders_pb2 as pb
    return pb, [ParseDict(record, pb.Order()) for record in records]


def timed_map(function, values):
    start = time.perf_counter_ns()
    result = [function(value) for value in values]
    return result, (time.perf_counter_ns() - start) / 1_000_000


def require_runs(runs):
    if runs < 5:
        raise ValueError("At least five measured runs are required (plus one warmup)")


def median(values):
    return statistics.median(values)
