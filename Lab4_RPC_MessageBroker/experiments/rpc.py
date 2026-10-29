"""Manage an isolated RPC benchmark server and collect C#/Python measurements."""
import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time

import grpc

from experiments.common import (ROOT, digest, evidence, load_dataset, load_protobuf,
                                median, require_runs, timed_map, write_csv, write_json)


def dotnet_executable():
    local = ROOT.parent / ".cache/dotnet/dotnet.exe"
    value = os.environ.get("DIS_DOTNET") or (str(local) if local.exists() else shutil.which("dotnet"))
    if not value:
        raise RuntimeError(".NET not found. Run scripts/setup.ps1 first.")
    return value


@contextmanager
def isolated_server(output):
    with socket.socket() as available:
        available.bind(("127.0.0.1", 0))
        port = available.getsockname()[1]
    address = f"127.0.0.1:{port}"
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with (output / "server.log").open("w", encoding="utf-8") as log:
        process = subprocess.Popen([sys.executable, "-m", "server.main", "--address", address,
                                    "--schema", "v1", "--quiet"],
                                   cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, creationflags=flags)
        try:
            with grpc.insecure_channel(address) as channel:
                ready = grpc.channel_ready_future(channel)
                until = time.monotonic() + 15
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(f"Benchmark server exited; see {output / 'server.log'}")
                    try:
                        ready.result(timeout=0.25)
                        break
                    except grpc.FutureTimeoutError:
                        if time.monotonic() > until:
                            raise RuntimeError(f"Benchmark server startup timed out; see {output / 'server.log'}")
            yield address, process.pid
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


def server_codec_profile(pb, orders, runs, run_id):
    requests = []
    for index, order in enumerate(orders):
        request = pb.CreateOrderRequest(request_id=f"benchmark-{run_id}-profile-{index}", buyer_id=order.buyer_id,
                                        shipping_address=order.shipping_address, items=order.items)
        if order.HasField("comment"):
            request.comment = order.comment
        requests.append(request.SerializeToString())
    rows = []
    for run in range(-1, runs):
        decoded, decode_ms = timed_map(pb.CreateOrderRequest.FromString, requests)
        encoded, encode_ms = timed_map(lambda order: order.SerializeToString(), orders)
        if len(decoded) != len(orders) or len(encoded) != len(orders):
            raise RuntimeError("Incomplete Python codec profile")
        if run >= 0:
            rows.append(dict(run=run + 1, request_decode_ms=decode_ms, response_encode_ms=encode_ms))
        del decoded, encoded
    return dict(request_decode_ns_per_call=median([r["request_decode_ms"] for r in rows]) * 1_000_000 / len(orders),
                response_encode_ns_per_call=median([r["response_encode_ms"] for r in rows]) * 1_000_000 / len(orders),
                runs=rows, method="Separate Python generated-message microbenchmarks on dataset fixtures")


def main():
    parser = argparse.ArgumentParser(description="Step 3: sequential C# -> Python gRPC latency")
    parser.add_argument("--dataset", default="data/orders.jsonl")
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    require_runs(args.runs)
    if not 0 < args.timeout <= 3600:
        parser.error("--timeout must be greater than zero and at most 3600 seconds")
    dataset, records = load_dataset(args.dataset)
    dll = ROOT / "client/bin/Release/v1/net10.0/OrderClient.dll"
    if not dll.exists():
        raise RuntimeError("C# v1 client is not built. Run scripts/setup.ps1 first.")
    output = evidence(args.output_dir, "03_rpc", dataset=str(dataset), dataset_sha256=digest(dataset))
    run_id = output.name
    print("STEP 3: C# CLIENT -> PYTHON SERVER", flush=True)
    print(f"Orders per pass: {len(records)}; warmup: 1; measured passes: {args.runs}", flush=True)
    print("A dedicated temporary server is used; RabbitMQ and existing servers are not involved.", flush=True)
    with isolated_server(output) as (address, pid):
        write_json(output / "server_instance.json", dict(address=address, pid=pid, schema="v1", broker_enabled=False))
        result = subprocess.run([
            dotnet_executable(), str(dll), "bench", "--dataset", str(dataset), "--runs", str(args.runs),
            "--timeout", str(args.timeout), "--output-dir", str(output), "--run-id", run_id,
            "--address", f"http://{address}",
        ], cwd=ROOT)
        if result.returncode:
            raise RuntimeError(f"C# benchmark failed with exit code {result.returncode}; partial evidence: {output}")
    client = json.loads((output / "rpc_client.json").read_text(encoding="utf-8"))
    pb, messages = load_protobuf(records)
    server_codec = server_codec_profile(pb, messages, args.runs, run_id)
    write_json(output / "server_codec.json", server_codec)
    client_codec = client["client_codec"]
    codec_ns = (client_codec["request_encode_ns_per_call"] + client_codec["response_decode_ns_per_call"]
                + server_codec["request_decode_ns_per_call"] + server_codec["response_encode_ns_per_call"])
    share = codec_ns / (client["median_mean_ms"] * 1_000_000) * 100
    summary = dict(client=client, server_codec=server_codec, estimated_codec_ns_per_call=codec_ns,
                   estimated_codec_share_percent=share, dataset_sha256=digest(dataset),
                   codec_estimate_limit="An estimate from separate fixture microbenchmarks, not a direct measurement inside the RPC. Fixtures use dataset IDs/timestamps, not the newly generated server responses. Network framing, gRPC scheduling, object conversion and business logic are not codec time.")
    write_json(output / "rpc_summary.json", summary)
    write_csv(output / "rpc_medians.csv", [dict(count=len(records), runs=args.runs,
              p50_ms=client["median_p50_ms"], p95_ms=client["median_p95_ms"], p99_ms=client["median_p99_ms"],
              mean_ms=client["median_mean_ms"], estimated_codec_share_percent=share)])
    print("\nSTEP 3: MEDIANS ACROSS MEASURED PASSES")
    print(f"Calls per pass: {len(records)}; passes: {args.runs}; warmup excluded")
    for key in ("median_p50_ms", "median_p95_ms", "median_p99_ms", "median_mean_ms"):
        print(f"{key:22} {client[key]:.4f} ms")
    print(f"Codec share ESTIMATE: {share:.3f}% (separate microbenchmarks, not direct RPC profiling)")
    print(f"Unique created IDs including warmup: {client['total_created_including_warmup']}")
    print(f"Results: {output}")


if __name__ == "__main__":
    main()
