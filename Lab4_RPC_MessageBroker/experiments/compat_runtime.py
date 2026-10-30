"""Isolated processes and evidence collection, used only by explicit schema scripts."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
from uuid import uuid4

import grpc

from experiments.common import ROOT, write_json
from experiments.rpc import dotnet_executable
from messaging import DEFAULT_URL, connect


def wait_for(function, description, processes=(), timeout=30):
    until = time.monotonic() + timeout
    while True:
        for process in processes:
            if process.poll() is not None:
                raise RuntimeError(f"Process {process.pid} exited while waiting for {description}; inspect case logs")
        value = function()
        if value:
            return value
        if time.monotonic() >= until:
            raise TimeoutError(f"Timed out waiting for {description}; inspect case logs")
        time.sleep(0.1)


def audit_rows(path):
    path = Path(path)
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    rows = []
    for index, line in enumerate(lines):
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            if index == len(lines) - 1 and not text.endswith("\n"):
                break  # The writer may still be completing the last line.
            raise
    return rows


@contextmanager
def child_process(arguments, log):
    log = Path(log)
    log.parent.mkdir(parents=True, exist_ok=True)
    flags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    with log.open("w", encoding="utf-8") as stream:
        process = subprocess.Popen(arguments, cwd=ROOT, stdout=stream, stderr=subprocess.STDOUT,
                                   creationflags=flags)
        try:
            yield process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


@contextmanager
def order_server(version, directory, namespace=None, broker_url=DEFAULT_URL,
                 create_response_delay_ms=0, idempotency=True, capture_observations=True, record_publications=False):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with socket.socket() as available:
        available.bind(("127.0.0.1", 0))
        port = available.getsockname()[1]
    address = f"127.0.0.1:{port}"
    observations = directory / "server_observations.jsonl"
    results = directory / "server_results.jsonl"
    publications = directory / "server_publications.jsonl"
    args = [sys.executable, "-m", "server.main", "--schema", version, "--address", address,
            "--create-response-delay-ms", str(create_response_delay_ms), "--quiet"]
    if capture_observations:
        args += ["--observations", str(observations)]
    if not idempotency:
        args += ["--disable-idempotency"]
    if namespace:
        args += ["--broker-url", broker_url, "--namespace", namespace, "--audit", str(results)]
        if record_publications:
            args += ["--publication-audit", str(publications)]
    with child_process(args, directory / "server.log") as process:
        with grpc.insecure_channel(address) as channel:
            ready = grpc.channel_ready_future(channel)

            def is_ready():
                try:
                    ready.result(timeout=0.1)
                    return True
                except grpc.FutureTimeoutError:
                    return False

            wait_for(is_ready, "gRPC server startup", (process,))
        yield SimpleNamespace(version=version, address=f"http://{address}", process=process,
                              observations=observations, results=results, publications=publications)


@contextmanager
def warehouse_process(version, directory, namespace, broker_url=DEFAULT_URL, name="warehouse"):
    directory = Path(directory)
    audit = directory / f"{name}_observations.jsonl"
    args = [sys.executable, "-m", "warehouse.main", "--schema", version, "--broker-url", broker_url,
            "--namespace", namespace, "--audit", str(audit)]
    with child_process(args, directory / f"{name}.log") as process:
        wait_for(lambda: any(row["kind"] == "ready" for row in audit_rows(audit)),
                 f"warehouse {version} startup", (process,))
        yield SimpleNamespace(version=version, process=process, audit=audit)


def queue_state(namespace, broker_url=DEFAULT_URL):
    connection, channel, names = connect(broker_url, namespace)
    try:
        result = {}
        for role, name in zip(("orders", "results"), names):
            state = channel.queue_declare(queue=name, passive=True).method
            result[role] = dict(name=name, ready=state.message_count, consumers=state.consumer_count)
        return result
    finally:
        connection.close()


@contextmanager
def broker_namespace(directory, broker_url=DEFAULT_URL, prefix="compat"):
    if prefix not in ("compat", "failure"):
        raise ValueError("Unsupported owned namespace prefix")
    namespace = prefix + "-" + uuid4().hex[:16]
    # Only this invocation's unique queues are declared/deleted.
    initial = queue_state(namespace, broker_url)
    write_json(Path(directory) / "queue_namespace.json", dict(namespace=namespace, initial=initial))
    try:
        yield namespace
    finally:
        cleanup = dict(namespace=namespace, deleted=False)
        try:
            connection, channel, names = connect(broker_url, namespace)
            try:
                for name in names:
                    channel.queue_delete(queue=name)
                cleanup["deleted"] = True
            finally:
                connection.close()
        except Exception as error:
            cleanup["error"] = str(error)
            print(f"Cleanup warning for {namespace}: {error}", flush=True)
        write_json(Path(directory) / "queue_cleanup.json", cleanup)


def require_artifacts(versions):
    for version in versions:
        for path in (ROOT / f"client/bin/Release/{version}/net10.0/OrderClient.dll",
                     ROOT / f"server/generated/{version}/orders_pb2.py"):
            if not path.exists():
                raise RuntimeError(f"Missing {path}. Run scripts/setup.ps1 first.")


def client_call(version, arguments, destination, process_timeout=30):
    result = subprocess.run([
        dotnet_executable(), str(ROOT / f"client/bin/Release/{version}/net10.0/OrderClient.dll"), *arguments,
    ], cwd=ROOT, capture_output=True, encoding="utf-8", timeout=process_timeout)
    value = dict(version=version, arguments=arguments, exit_code=result.returncode,
                 stdout=result.stdout, stderr=result.stderr)
    write_json(destination, value)
    if result.returncode == 0:
        value["body"] = json.loads(result.stdout)
    return value


def worker(version, operation, source, destination, output=None, mode="direct"):
    args = [sys.executable, "-m", "experiments.schema_worker", "--schema", version,
            "--operation", operation, "--input", str(source), "--mode", mode]
    if output is not None:
        args += ["--output", str(output)]
    result = subprocess.run(args, cwd=ROOT, capture_output=True, encoding="utf-8", timeout=30)
    if result.returncode:
        write_json(destination, dict(error=result.stderr, exit_code=result.returncode))
        raise RuntimeError(f"Schema worker failed; see {destination}")
    value = json.loads(result.stdout)
    write_json(destination, value)
    return value


def response_snapshot(version, body):
    def state(name, key):
        known = (version == "v2") == (name == "promo_code")
        return dict(known=known, present=(key in body) if known else None,
                    value=body.get(key, "") if known else None)
    return dict(body=body, comment=state("comment", "comment"), promo_code=state("promo_code", "promoCode"),
                prices_minor=[int(item.get("unitPriceMinor", 0)) for item in body.get("items", [])])


def field_label(state):
    if not state["known"]:
        return "unknown"
    if not state["present"]:
        return "absent:default=''"
    return "set:" + state["value"]


def create_case(client_version, server, directory, name, price=16000, optional=True):
    request_id = "schema-" + uuid4().hex
    args = ["create", "--address", server.address, "--request-id", request_id,
            "--buyer", "buyer-schema", "--item", f"sku-001:2:{price}"]
    if optional:
        args += ["--promo-code", "PROMO_V2"] if client_version == "v2" else ["--comment", "NOTE_V1"]
    client = client_call(client_version, args, Path(directory) / f"{name}_client.json")
    rows = audit_rows(server.observations)
    received = next((r for r in rows if r["kind"] == "rpc_request" and r["request_id"] == request_id), None)
    stored = next((r for r in rows if r["kind"] == "rpc_created" and r["request_id"] == request_id), None)
    rejected = next((r for r in rows if r["kind"] == "rpc_rejected" and r["request_id"] == request_id), None)
    if received is None:
        raise RuntimeError(f"No decoded request evidence for {name}; inspect server/client logs")
    if client["exit_code"] != 0:
        if not (client["exit_code"] == 2 and rejected and rejected["code"] == "INVALID_ARGUMENT"
                and "InvalidArgument" in client["stderr"]):
            raise RuntimeError(f"Unexpected RPC failure for {name}: {client['stderr']}")
        status = "INVALID_ARGUMENT"
        response = None
    else:
        if stored is None:
            raise RuntimeError(f"No stored-order evidence for {name}")
        status = "OK"
        response = response_snapshot(client_version, client["body"])
    detail = dict(client_schema=client_version, server_schema=server.version, request_id=request_id,
                  input_price=price, optional_sent=optional, rpc_status=status,
                  server_received=received["received"], server_stored=stored["stored"] if stored else None,
                  client_received=response, rejection=rejected)
    write_json(Path(directory) / f"{name}_observations.json", detail)
    return detail


def finish_order(server, warehouse, client_version, order_id, directory, name):
    def completed():
        processed = next((r for r in audit_rows(warehouse.audit)
                          if r["kind"] == "processed" and r["order_id"] == order_id), None)
        result = next((r for r in audit_rows(server.results)
                       if r["kind"] == "result" and r["order_id"] == order_id), None)
        acked = any(r["kind"] == "acked" and r["order_id"] == order_id for r in audit_rows(warehouse.audit))
        return dict(warehouse=processed, server_result=result) if processed and result and acked else None
    evidence = wait_for(completed, f"processing order {order_id}", (server.process, warehouse.process))
    fetched = client_call(client_version, ["get", order_id, "--address", server.address],
                          Path(directory) / f"{name}_final_client.json")
    if fetched["exit_code"] != 0:
        raise RuntimeError(f"GetOrder failed: {fetched['stderr']}")
    if fetched["body"].get("status") not in ("ORDER_STATUS_CONFIRMED", "ORDER_STATUS_REJECTED"):
        raise RuntimeError("Warehouse result was observed but final order status is not terminal")
    sent_result = evidence["warehouse"]["sent_result"]["body"]
    received_result = evidence["server_result"]["received"]["body"]
    if sent_result != received_result:
        raise RuntimeError("The result changed on the return path from warehouse to order service")
    expected_status = {2: "ORDER_STATUS_CONFIRMED", 3: "ORDER_STATUS_REJECTED"}[evidence["server_result"]["status"]]
    if fetched["body"].get("orderId") != order_id or fetched["body"]["status"] != expected_status:
        raise RuntimeError("The client did not receive the warehouse result for the expected order")
    evidence["client_final"] = response_snapshot(client_version, fetched["body"])
    write_json(Path(directory) / f"{name}_final_observations.json", evidence)
    return evidence
