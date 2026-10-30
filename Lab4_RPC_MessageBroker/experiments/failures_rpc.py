"""Experiment 3: unavailable server and ambiguous timeout/retry outcomes."""
import argparse
import re
import socket
from uuid import uuid4

from experiments.common import evidence, write_csv, write_json
from experiments.compatibility import table
from experiments.compat_runtime import audit_rows, client_call, order_server, require_artifacts


def unavailable(output):
    directory = output / "server_not_running"
    directory.mkdir()
    with socket.socket() as reserved:
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            reserved.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        reserved.bind(("127.0.0.1", 0))
        address = f"http://127.0.0.1:{reserved.getsockname()[1]}"
        # This socket reserves the address but never listens. No order server is started.
        result = client_call("v1", ["create", "--address", address, "--timeout", "5",
                                    "--request-id", "missing-" + uuid4().hex], directory / "client.json")
    match = re.search(r"gRPC (\w+):", result["stderr"])
    status = match.group(1) if match else "unknown"
    if result["exit_code"] != 2 or status not in ("Unavailable", "DeadlineExceeded"):
        raise RuntimeError("Could not reproduce the unavailable-server case; inspect client.json")
    detail = dict(address=address, no_server_started=True, reserved_socket_was_not_listening=True,
                  client_status=status, client_error=result["stderr"], created_orders=0,
                  count_basis="No OrderService process exists at this exclusively reserved non-listening endpoint")
    write_json(directory / "summary.json", detail)
    return dict(case="server_absent", idempotency="-", first_status=status,
                retry_status="not_attempted", created_orders=0), detail


def timeout_retry(output, enabled, delay_ms, timeout_seconds, retry_timeout):
    directory = output / ("retry_idempotency_on" if enabled else "retry_idempotency_off")
    with order_server("v1", directory, create_response_delay_ms=delay_ms, idempotency=enabled) as server:
        request_id = "retry-" + uuid4().hex
        buyer = "buyer-" + uuid4().hex
        result = client_call("v1", ["retry-demo", "--address", server.address, "--buyer", buyer,
            "--request-id", request_id, "--timeout", str(timeout_seconds), "--retry-timeout", str(retry_timeout)],
            directory / "client.json", process_timeout=40 + retry_timeout)
        if result["exit_code"] != 0:
            raise RuntimeError(f"Retry client failed: {result['stderr']}")
        observed = result["body"]
        if observed["first_status"] != "DeadlineExceeded" or observed["retry_status"] != "OK":
            raise RuntimeError("The configured slow-response scenario was not reproduced; inspect client.json")
        audit = audit_rows(server.observations)
        created = [row for row in audit if row["kind"] == "rpc_created" and row["request_id"] == request_id]
        received = [row for row in audit if row["kind"] == "rpc_request" and row["request_id"] == request_id]
        ids = sorted(row["order_id"] for row in created)
        if observed["before_order_ids"] or sorted(observed["after_order_ids"]) != ids or len(received) != 2:
            raise RuntimeError("Server evidence and client ListOrdersByBuyer results do not agree")
        detail = dict(idempotency_enabled=enabled, server_delay_ms=delay_ms, client=observed,
                      actual_created_order_ids=ids, actual_created_count=len(ids),
                      decoded_attempts=len(received), attempts_used_same_request_id=True,
                      server_observations=audit,
                      delay_placement="After storing the order, before returning the response")
        write_json(directory / "summary.json", detail)
    return dict(case="timeout_then_retry", idempotency="on" if enabled else "off",
                first_status=observed["first_status"], retry_status=observed["retry_status"], created_orders=len(ids)), detail


def main():
    parser = argparse.ArgumentParser(description="Experiment 3: RPC failures")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--delay-ms", type=int, default=2000)
    parser.add_argument("--timeout", type=float, default=0.5)
    parser.add_argument("--retry-timeout", type=float, default=10)
    args = parser.parse_args()
    if not 0 < args.timeout < args.delay_ms / 1000 < args.retry_timeout <= 3600:
        parser.error("Need 0 < client timeout < server delay < retry timeout <= 3600 seconds")
    require_artifacts(("v1",))
    output = evidence(args.output_dir, "experiment_3_rpc_failures")
    write_json(output / "run_status.json", dict(complete=False))
    rows, details = [], []
    print("EXPERIMENT 3: RPC FAILURES", flush=True)
    row, detail = unavailable(output)
    rows.append(row)
    details.append(detail)
    for enabled in (True, False):
        print(f"Timeout/retry: idempotency={'on' if enabled else 'off'}", flush=True)
        row, detail = timeout_retry(output, enabled, args.delay_ms, args.timeout, args.retry_timeout)
        rows.append(row)
        details.append(detail)
        write_json(output / "rpc_failures_summary.json", dict(complete=False, rows=rows, observations=details))
    write_json(output / "rpc_failures_summary.json", dict(complete=True, rows=rows, observations=details))
    write_csv(output / "rpc_failures_matrix.csv", rows)
    write_json(output / "run_status.json", dict(complete=True))
    table("ACTUAL CLIENT ERRORS AND CREATED ORDERS", rows, [(key, key) for key in rows[0]])
    print("The first timeout is reproduced after channel warmup; both attempts reuse one request_id.", flush=True)
    print(f"Results: {output}", flush=True)


if __name__ == "__main__":
    main()
