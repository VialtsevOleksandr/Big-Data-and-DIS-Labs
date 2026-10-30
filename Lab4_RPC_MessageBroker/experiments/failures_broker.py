"""Experiment 4: queued backlog and a real Docker SIGKILL/restart, tracked by IDs."""
import argparse
import csv
from pathlib import Path
import time
from uuid import uuid4

from experiments.common import digest, evidence, load_dataset, write_csv, write_json
from experiments.compatibility import table
from experiments.compat_runtime import broker_namespace, client_call, order_server, queue_state, require_artifacts, wait_for
from experiments.failure_runtime import Ledger, docker_worker
from messaging import DEFAULT_URL


def read_csv(path):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def load_orders(server, directory, dataset, count, timeout):
    token = uuid4().hex
    buyer = "buyer-failure-" + token
    manifest = directory / "sent_orders.csv"
    print(f"Creating {count} orders through the C# gRPC client...", flush=True)
    result = client_call("v1", ["load-orders", "--address", server.address,
        "--dataset", str(dataset), "--count", str(count), "--buyer", buyer, "--run-id", token,
        "--manifest", str(manifest), "--timeout", "15"], directory / "load_client.json", process_timeout=timeout)
    if result["stderr"].strip():
        print(result["stderr"].strip(), flush=True)
    if result["exit_code"] != 0:
        raise RuntimeError("Could not create the complete batch; inspect load_client.json and sent_orders.csv")
    rows = read_csv(manifest)
    if len(rows) != count or len({r["order_id"] for r in rows}) != count or len({r["request_id"] for r in rows}) != count:
        raise RuntimeError("Sent-order manifest has an unexpected count or duplicate IDs")
    return buyer, rows


def wait_published(ledger, server, namespace, sent, broker_url, timeout):
    last = [-1]
    def ready():
        ledger.update()
        count = len(set(ledger.published) & sent)
        if count // 1000 != last[0]:
            print(f"Broker-confirmed unique order IDs: {count}/{len(sent)}", flush=True)
            last[0] = count // 1000
        if not sent.issubset(ledger.published):
            return None
        state = queue_state(namespace, broker_url)
        if state["orders"]["consumers"] != 0:
            raise RuntimeError("An unexpected consumer is reading this invocation's queue")
        return state if state["orders"]["ready"] >= len(sent) else None
    return wait_for(ready, "the whole batch to be confirmed and queued with no warehouse", (server.process,), timeout)


def wait_processed(ledger, worker, server, sent, timeout):
    until = time.monotonic() + timeout
    last_count, last_print = -1, 0.0
    while True:
        worker.require_running()
        if server.process.poll() is not None:
            raise RuntimeError("Order service exited while warehouse was processing")
        ledger.update()
        done = len(set(ledger.processed) & sent)
        now = time.monotonic()
        if done // 1000 != last_count or now - last_print > 10:
            print(f"Processed unique IDs: {done}/{len(sent)}; attempts={sum(ledger.processed.values())}; results={len(ledger.results)}", flush=True)
            last_count, last_print = done // 1000, now
        if (sent.issubset(ledger.processed) and sent.issubset(ledger.acked)
                and sent.issubset(ledger.results) and sum(ledger.results.values()) >= sum(ledger.processed.values())):
            return True
        if now >= until:
            return False
        time.sleep(0.2)


def after_worker_stopped(namespace, broker_url):
    def stopped():
        state = queue_state(namespace, broker_url)
        return state if state["orders"]["consumers"] == 0 else None
    return wait_for(stopped, "worker disconnect and return of unacknowledged messages", timeout=20)


def choose_checkpoint(records, count):
    def available(order):
        for item in order["items"]:
            product = item["product_id"]
            if product.startswith("sku-") and product[4:].isdigit() and int(product[4:]) % 11 == 0:
                return False
        return True
    # Prefer a successful reservation near the middle, so a repeated reservation is observable too.
    return next((i for i in range(count // 2, count - 1) if available(records[i])), count // 2)


def final_statuses(server, buyer, directory, timeout):
    filename = directory / "final_order_statuses.csv"
    result = client_call("v1", ["order-statuses", "--address", server.address, "--buyer", buyer,
        "--status-file", str(filename), "--timeout", "30"], directory / "status_client.json", process_timeout=timeout)
    if result["exit_code"] != 0:
        raise RuntimeError("Could not read final server states; inspect status_client.json")
    return {row["order_id"]: int(row["status"]) for row in read_csv(filename)}


def summarize(directory, manifest, ledger, statuses, final_queue, finished, extra):
    ledger.update()
    sent = {row["order_id"] for row in manifest}
    if set(statuses) != sent:
        raise RuntimeError("Final server order IDs do not match the sent-order manifest")
    processed = set(ledger.processed)
    missing = sorted(sent - processed)
    duplicates = sorted(key for key, count in ledger.processed.items() if count > 1)
    unexpected = sorted((processed | set(ledger.results) | set(ledger.published)) - sent)
    ready = final_queue["orders"]["ready"]
    # A timeout while messages are still queued is not evidence of message loss.
    lost = missing if ready == 0 else ([] if not missing else None)
    rows = [dict(index=int(row["index"]), source_order_id=row["source_order_id"], request_id=row["request_id"],
        order_id=row["order_id"], processing_attempts=ledger.processed[row["order_id"]],
        acknowledgement_attempts=ledger.acked[row["order_id"]], reservation_attempts=ledger.reserved[row["order_id"]],
        result_deliveries=ledger.results[row["order_id"]], redelivered_attempts=ledger.redelivered[row["order_id"]],
        worker_instances=len(ledger.instances.get(row["order_id"], set())), final_status=statuses[row["order_id"]])
        for row in manifest]
    summary = dict(complete=finished, sent_count=len(sent), **ledger.counts(),
        missing_order_ids=missing, lost_order_ids=lost, duplicate_order_ids=duplicates,
        unexpected_order_ids=unexpected, queued_after_worker_stopped=ready,
        confirmed=sum(value == 2 for value in statuses.values()), rejected=sum(value == 3 for value in statuses.values()),
        pending=sum(value == 1 for value in statuses.values()), final_queue=final_queue, **extra,
        processing_definition="processed audit entry after inventory handling and confirmed result publication, before input ack",
        evidence_io="Warehouse audit flushes each JSONL line to the host bind mount; the crash checkpoint additionally fsyncs before waiting. Audit is evidence, not order storage or persistent deduplication.",
        restart_limitation="Inventory and deduplication are in process memory; a new worker can process/reserve the same order again. The order service deduplicates result event IDs while it stays running.")
    write_csv(directory / "per_order_results.csv", rows)
    write_csv(directory / "duplicate_orders.csv", [row for row in rows if row["processing_attempts"] > 1])
    write_json(directory / "delivery_summary.json", summary)
    return summary


def run_case(output, mode, dataset, records, count, broker_url, docker_broker_url, timeout):
    directory = output / mode
    directory.mkdir()
    extra = dict(scenario=mode)
    with broker_namespace(directory, broker_url, prefix="failure") as namespace:
        with order_server("v1", directory, namespace, broker_url,
                          capture_observations=False, record_publications=True) as server:
            buyer, manifest = load_orders(server, directory, dataset, count, timeout)
            expected_sources = [record["order_id"] for record in records[:count]]
            if [row["source_order_id"] for row in manifest] != expected_sources:
                raise RuntimeError("The C# client did not use the selected seeded dataset records in order")
            sent = {row["order_id"] for row in manifest}
            ledger = Ledger(directory, server)
            queued = wait_published(ledger, server, namespace, sent, broker_url, timeout)
            write_json(directory / "before_warehouse.json", dict(queue=queued, sent_count=count,
                confirmed_unique_ids=len(ledger.published), confirmed_publications=sum(ledger.published.values())))
            print(f"BACKLOG: ready={queued['orders']['ready']}, warehouse consumers={queued['orders']['consumers']}", flush=True)
            extra["queue_before_warehouse"] = queued
            if mode == "backlog":
                with docker_worker(directory, namespace, broker_url=docker_broker_url) as worker:
                    finished = wait_processed(ledger, worker, server, sent, timeout)
            else:
                index = choose_checkpoint(records, count)
                target = manifest[index]["order_id"]
                print(f"Crash checkpoint: input index={index}, order_id={target}", flush=True)
                with docker_worker(directory, namespace, checkpoint=target, broker_url=docker_broker_url) as worker:
                    def at_checkpoint():
                        worker.require_running()
                        ledger.update()
                        return next((row for row in ledger.checkpoints if row["order_id"] == target
                                     and row["instance_id"] == worker.instance), None)
                    checkpoint = wait_for(at_checkpoint, "processing-before-ack crash checkpoint", (server.process,), timeout)
                    if not 0 < len(ledger.processed) < count or ledger.acked[target] != 0:
                        raise RuntimeError("Crash point was not in the middle of the batch before target acknowledgement")
                    before = dict(checkpoint=checkpoint, counts=ledger.counts(),
                                  processed_order_ids=sorted(ledger.processed), queue=queue_state(namespace, broker_url))
                    write_json(directory / "before_kill.json", before)
                    print(f"BEFORE KILL: unique={len(ledger.processed)}, attempts={sum(ledger.processed.values())}, target acked=0", flush=True)
                    killed = worker.hard_kill()
                    after_kill = after_worker_stopped(namespace, broker_url)
                    write_json(directory / "after_kill.json", dict(kill=killed, queue=after_kill))
                    print(f"SIGKILL confirmed: exit={killed['exit_code']}; messages ready after kill={after_kill['orders']['ready']}", flush=True)
                with docker_worker(directory, namespace, broker_url=docker_broker_url) as restarted:
                    finished = wait_processed(ledger, restarted, server, sent, timeout)
                extra.update(crash_target_order_id=target, crash_target_source_index=index,
                             before_kill=before, after_kill=after_kill, kill=killed)
            final_queue = after_worker_stopped(namespace, broker_url)
            statuses = final_statuses(server, buyer, directory, timeout)
            summary = summarize(directory, manifest, ledger, statuses, final_queue, finished, extra)
    row = dict(scenario=mode, sent=count, attempts=summary["processing_attempts"],
               unique=summary["unique_processed"], duplicate_ids=len(summary["duplicate_order_ids"]),
               lost=len(summary["lost_order_ids"]) if summary["lost_order_ids"] is not None else "unknown:pending",
               queued=summary["queued_after_worker_stopped"], pending=summary["pending"], complete=summary["complete"])
    write_csv(output / "delivery_matrix.csv", [row])
    return row, summary


def main():
    parser = argparse.ArgumentParser(description="Experiment 4: warehouse failures, 10000 messages per scenario")
    parser.add_argument("--scenario", choices=("backlog", "crash"), required=True)
    parser.add_argument("--dataset", default="data/orders.jsonl")
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--broker-url", default=DEFAULT_URL)
    parser.add_argument("--docker-broker-url", default="amqp://dis:dis-local@rabbitmq:5672/%2F")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    if args.count < 10000 or args.timeout <= 0:
        parser.error("Need at least 10000 messages and a positive scenario timeout")
    require_artifacts(("v1",))
    dataset, records = load_dataset(args.dataset)
    if len(records) < args.count:
        parser.error("Dataset has fewer records than --count")
    output = evidence(args.output_dir, "experiment_4_warehouse_" + args.scenario,
                      dataset=str(dataset), dataset_sha256=digest(dataset), requested_count=args.count)
    write_json(output / "run_status.json", dict(complete=False, scenario=args.scenario))
    print(f"EXPERIMENT 4: {args.scenario.upper()}, {args.count} MESSAGES", flush=True)
    row, summary = run_case(output, args.scenario, dataset, records, args.count,
                            args.broker_url, args.docker_broker_url, args.timeout)
    write_json(output / "warehouse_failure_summary.json", summary)
    write_json(output / "run_status.json", dict(complete=summary["complete"], scenario=args.scenario))
    table("DELIVERY COUNTS VERIFIED BY ORDER IDs", [row], [(key, key) for key in row])
    print(f"Results: {output}", flush=True)
    if not summary["complete"]:
        raise RuntimeError("Observation deadline reached; partial results saved. Pending messages are not labelled as lost.")


if __name__ == "__main__":
    main()
