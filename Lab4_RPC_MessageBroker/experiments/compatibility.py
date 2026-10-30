"""Schema checks for the student to launch; importing this module starts nothing."""
import argparse
from itertools import product
import json
from pathlib import Path

from experiments.common import ROOT, digest, evidence, write_csv, write_json
from experiments.compat_runtime import (audit_rows, broker_namespace, client_call, create_case,
    field_label, finish_order, order_server, queue_state, require_artifacts, wait_for,
    warehouse_process, worker)
from messaging import DEFAULT_URL


def table(title, rows, columns):
    print("\n" + title, flush=True)
    if not rows:
        print("No completed cases.", flush=True)
        return
    widths = {key: max(len(label), *(len(str(row.get(key, "-"))) for row in rows)) for key, label in columns}
    print(" | ".join(label.ljust(widths[key]) for key, label in columns), flush=True)
    print("-+-".join("-" * widths[key] for key, _ in columns), flush=True)
    for row in rows:
        print(" | ".join(str(row.get(key, "-")).ljust(widths[key]) for key, _ in columns), flush=True)


def rpc_row(detail, scenario):
    received, stored, reply = detail["server_received"], detail["server_stored"], detail["client_received"]
    sent = detail["input_price"]
    corrupt = received["prices_minor"] != [sent]
    if stored:
        corrupt = corrupt or stored["prices_minor"] != [sent]
    if reply:
        corrupt = corrupt or reply["prices_minor"] != [sent]
    outcome = "application_error" if detail["rpc_status"] != "OK" else "silent_corruption" if corrupt else "OK"
    return dict(client=detail["client_schema"], server=detail["server_schema"], scenario=scenario,
        sent=sent, decoded=received["prices_minor"][0], stored=stored["prices_minor"][0] if stored else "-",
        reply=reply["prices_minor"][0] if reply else "-", rpc=detail["rpc_status"], outcome=outcome,
        server_comment=field_label(received["comment"]), server_promo=field_label(received["promo_code"]),
        reply_comment=field_label(reply["comment"]) if reply else "-",
        reply_promo=field_label(reply["promo_code"]) if reply else "-")


def rpc_matrix(output, breaking=False):
    versions = ("v1", "v2_breaking") if breaking else ("v1", "v2")
    scenarios = (("even", 16000, True), ("odd", 15999, True)) if breaking else (
        ("set", 16000, True), ("absent", 16000, False))
    rows, details = [], []
    for client_version, server_version in product(versions, repeat=2):
        print(f"RPC case: client={client_version} server={server_version}", flush=True)
        directory = output / f"rpc_{client_version}_to_{server_version}"
        with order_server(server_version, directory) as server:
            for name, price, optional in scenarios:
                detail = create_case(client_version, server, directory, name, price, optional)
                rows.append(rpc_row(detail, name))
                details.append(detail)
        write_json(output / "rpc_schema_summary.json", dict(complete=False, rows=rows, observations=details))
    write_json(output / "rpc_schema_summary.json", dict(complete=True, rows=rows, observations=details,
        interpretation="unknown means the generated receiver has no accessor for the field. It does not prove the binary unknown field was discarded. Error rows are application validation errors, not protobuf decoder errors."))
    write_csv(output / "rpc_schema_matrix.csv", rows)
    if breaking:
        table("INCOMPATIBLE PRICE: ACTUAL RPC OBSERVATIONS", rows,
              [(k, k) for k in ("client", "server", "scenario", "sent", "decoded", "stored", "reply", "rpc", "outcome")])
        print("decoded/stored are receiver-side values. A correct-looking reply can hide corrupted server data.", flush=True)
    else:
        for scenario in ("set", "absent"):
            table(f"RPC SCHEMA MATRIX: OPTIONAL FIELD {scenario.upper()}",
                  [row for row in rows if row["scenario"] == scenario],
                  [(k, k) for k in ("client", "server", "rpc", "server_comment", "server_promo", "reply_comment", "reply_promo")])
    return rows


def pipeline_case(output, client_version, server_version, warehouse_version, broker_url, prices=(16000,)):
    directory = output / f"pipeline_c-{client_version}_s-{server_version}_w-{warehouse_version}"
    directory.mkdir(parents=True, exist_ok=True)
    rows, details = [], []
    with broker_namespace(directory, broker_url) as namespace:
        with order_server(server_version, directory, namespace, broker_url) as server:
            with warehouse_process(warehouse_version, directory, namespace, broker_url) as warehouse:
                for index, price in enumerate(prices):
                    name = f"order_{index}"
                    created = create_case(client_version, server, directory, name, price)
                    if created["rpc_status"] != "OK":
                        raise RuntimeError("Pipeline setup failed before the broker; inspect RPC evidence")
                    order_id = created["server_stored"]["body"]["order_id"]
                    completed = finish_order(server, warehouse, client_version, order_id, directory, name)
                    sender = created["server_stored"]
                    received = completed["warehouse"]["received"]
                    corrupted = received["prices_minor"] != sender["prices_minor"]
                    row = dict(client=client_version, sender=server_version, reader=warehouse_version,
                        sent_price=sender["prices_minor"][0], decoded_price=received["prices_minor"][0],
                        outcome="silent_corruption" if corrupted else "OK",
                        sender_comment=field_label(sender["comment"]), sender_promo=field_label(sender["promo_code"]),
                        reader_comment=field_label(received["comment"]), reader_promo=field_label(received["promo_code"]),
                        final_status=completed["client_final"]["body"]["status"],
                        return_writer=completed["warehouse"]["schema"],
                        return_reader=completed["server_result"]["schema"], result_returned=True,
                        negative_price_accepted=received["prices_minor"][0] < 0,
                        order_id=order_id)
                    rows.append(row)
                    details.append(dict(created=created, completed=completed, namespace=namespace))
    write_json(directory / "case_summary.json", dict(rows=rows, observations=details))
    return rows


def backlog_upgrade(output, broker_url):
    directory = output / "backlog_v1_then_reader_v2"
    directory.mkdir(parents=True, exist_ok=True)
    observations = []
    with broker_namespace(directory, broker_url) as namespace:
        with order_server("v1", directory, namespace, broker_url) as server:
            # Register an actual v1 consumer, then stop it before writing the backlog.
            with warehouse_process("v1", directory, namespace, broker_url, name="old_warehouse"):
                pass
            def reader_stopped():
                state = queue_state(namespace, broker_url)
                return state if state["orders"]["consumers"] == 0 else None
            stopped = wait_for(reader_stopped, "old v1 consumer to disconnect", (server.process,))
            created = [create_case("v1", server, directory, f"backlog_{i}") for i in range(3)]
            if any(item["rpc_status"] != "OK" for item in created):
                raise RuntimeError("Could not create the old-schema backlog")
            order_ids = [item["server_stored"]["body"]["order_id"] for item in created]
            def backlog_ready():
                state = queue_state(namespace, broker_url)
                return state if state["orders"]["ready"] == 3 and state["orders"]["consumers"] == 0 else None
            before = wait_for(backlog_ready, "three v1 messages to accumulate", (server.process,))
            write_json(directory / "before_upgrade.json", dict(
                writer_schema="v1", old_reader_schema="v1", new_reader_schema="v2",
                stopped=stopped, queued=before, sent_order_ids=order_ids))
            print("Backlog captured: writer=v1, stopped reader=v1, ready messages=3, consumers=0.", flush=True)
            with warehouse_process("v2", directory, namespace, broker_url, name="new_warehouse") as warehouse:
                for index, order_id in enumerate(order_ids):
                    observations.append(finish_order(server, warehouse, "v1", order_id, directory, f"backlog_{index}"))
                after = queue_state(namespace, broker_url)
    processed_ids = {item["warehouse"]["order_id"] for item in observations}
    rows = [dict(order_id=item["warehouse"]["order_id"], writer="v1", restarted_reader="v2",
                 reader_comment=field_label(item["warehouse"]["received"]["comment"]),
                 reader_promo=field_label(item["warehouse"]["received"]["promo_code"]),
                 final_status=item["client_final"]["body"]["status"]) for item in observations]
    summary = dict(complete=True, sent_order_ids=order_ids, processed_order_ids=sorted(processed_ids),
                   missing_order_ids=sorted(set(order_ids) - processed_ids),
                   queue_before_restart=before, queue_after_processing=after,
                   rows=rows, observations=observations,
                   scope="Three targeted schema-upgrade fixtures; not the separate 10000-message failure experiment")
    write_json(output / "backlog_upgrade_summary.json", summary)
    write_csv(output / "backlog_upgrade_matrix.csv", rows)
    table("OLD MESSAGES v1 -> RESTARTED READER v2", rows,
          [(k, k) for k in ("writer", "restarted_reader", "reader_comment", "reader_promo", "final_status")])
    print(f"Sent IDs: {len(order_ids)}; processed IDs: {len(processed_ids)}; missing: {len(summary['missing_order_ids'])}", flush=True)
    return summary


def broker_matrix(output, broker_url, breaking=False):
    rows = []
    if breaking:
        combinations = [(sender, sender, reader) for sender, reader in product(("v1", "v2_breaking"), repeat=2)]
    else:
        combinations = list(product(("v1", "v2"), repeat=3))
    for client_version, server_version, warehouse_version in combinations:
        print(f"Queue case: client={client_version} sender={server_version} reader={warehouse_version}", flush=True)
        rows.extend(pipeline_case(output, client_version, server_version, warehouse_version, broker_url,
                                  prices=(16000, 15999) if breaking else (16000,)))
        write_json(output / "broker_schema_summary.json", dict(complete=False, end_to_end_rows=rows))
    # Matching client/sender versions isolate broker compatibility from earlier RPC transformations.
    isolated = [row for row in rows if row["client"] == row["sender"]]
    write_json(output / "broker_schema_summary.json", dict(complete=True, end_to_end_rows=rows,
        isolated_broker_rows=isolated,
        interpretation="Sender values come from the order service after RPC. Reader values come from the warehouse after Protobuf decoding. The warehouse reserves by product/quantity, so an invalid price can be silently accepted. Result events are also observed on the return path."))
    write_csv(output / "broker_schema_matrix.csv", isolated)
    write_csv(output / "end_to_end_schema_matrix.csv", rows)
    if breaking:
        table("INCOMPATIBLE PRICE: ACTUAL BROKER OBSERVATIONS", isolated,
              [(k, k) for k in ("sender", "reader", "sent_price", "decoded_price", "outcome", "final_status")])
        print("No price validation is added to the warehouse for this test; its actual behavior is recorded.", flush=True)
    else:
        table("BROKER MATRIX: MATCHED CLIENT/SENDER, FOUR WRITER/READER PAIRS", isolated,
              [(k, k) for k in ("sender", "reader", "sender_comment", "sender_promo", "reader_comment", "reader_promo")])
        table("ALL EIGHT CLIENT / ORDER-SERVICE / WAREHOUSE COMBINATIONS", rows,
              [(k, k) for k in ("client", "sender", "reader", "reader_comment", "reader_promo", "final_status")])
        backlog_upgrade(output, broker_url)
    return rows


def unknown_fields(output):
    original = output / "original_v2.pb"
    fixture = output / "original_v2.json"
    write_json(fixture, dict(order_id="relay-order", buyer_id="relay-buyer", created_at="2026-01-01T00:00:00Z",
        status="ORDER_STATUS_PENDING", shipping_address=dict(country="UA", city="Київ", street="вул. Тестова, 1", postal_code="01001"),
        items=[dict(product_id="sku-001", quantity=2, unit_price_minor=16000)], promo_code="PROMO_V2"))
    encoded = worker("v2", "encode", fixture, output / "original_observation.json", output=original)
    source = encoded["after"]
    rows, details = [], []
    for language, mode in product(("Python", "C#"), ("direct", "json", "fields")):
        name = ("python" if language == "Python" else "csharp") + "_" + mode
        destination = output / f"{name}.pb"
        observation = output / f"{name}_v1.json"
        if language == "Python":
            old = worker("v1", "relay", original, observation, output=destination, mode=mode)
        else:
            called = client_call("v1", ["schema-relay", "--input", str(original), "--output", str(destination), "--mode", mode], observation)
            if called["exit_code"] != 0:
                raise RuntimeError(f"C# relay failed: {called['stderr']}")
            old = called["body"]
            if old["schema_namespace"] != "Dis.Orders.V1":
                raise RuntimeError("The relay client was not built against v1")
        decoded = worker("v2", "inspect", destination, output / f"{name}_v2.json")["after"]
        before_body, after_body = dict(source["body"]), dict(decoded["body"])
        before_body.pop("promo_code", None)
        after_body.pop("promo_code", None)
        if before_body != after_body:
            raise RuntimeError(f"Relay unexpectedly changed a known field: {name}")
        promo = decoded["promo_code"]
        retained = bool(promo["present"] and promo["value"] == source["promo_code"]["value"])
        row = dict(language=language, mode=mode, old_knows_promo=old["before"]["promo_code"]["known"],
                   recovered=field_label(promo), retained=retained,
                   input_sha256=digest(original), output_sha256=digest(destination))
        rows.append(row)
        details.append(dict(row=row, old_receiver=old, new_receiver=decoded))
    write_json(output / "unknown_fields_summary.json", dict(complete=True, source=source, rows=rows, observations=details,
        modes=dict(direct="Decode and serialize the same message object", json="Convert through known-field JSON",
                   fields="Rebuild the top-level Order from known fields; nested messages are copied")))
    write_csv(output / "unknown_fields_matrix.csv", rows)
    table("v2 -> OLD v1 PROCESS -> v2: UNKNOWN PROMO FIELD", rows,
          [(k, k) for k in ("language", "mode", "old_knows_promo", "recovered", "retained")])


def main():
    parser = argparse.ArgumentParser(description="Schema compatibility checks; explicit execution only")
    parser.add_argument("--part", choices=("rpc", "broker", "relay", "breaking"), required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--broker-url", default=DEFAULT_URL)
    args = parser.parse_args()
    versions = ("v1", "v2", "v2_breaking") if args.part == "breaking" else ("v1", "v2")
    require_artifacts(versions)
    output = evidence(args.output_dir, "04_schema_" + args.part,
        schema_sha256={version: digest(ROOT / f"schemas/{version}/orders.proto") for version in versions})
    write_json(output / "run_status.json", dict(complete=False, part=args.part))
    print(f"SCHEMA CHECK: {args.part}", flush=True)
    print("Observed values are collected from actual generated-code receivers; outcomes are not prefilled.", flush=True)
    if args.part == "rpc":
        rpc_matrix(output)
    elif args.part == "broker":
        broker_matrix(output, args.broker_url)
    elif args.part == "relay":
        unknown_fields(output)
    else:
        rpc_output, broker_output = output / "rpc", output / "broker"
        rpc_output.mkdir()
        broker_output.mkdir()
        rpc_matrix(rpc_output, breaking=True)
        broker_matrix(broker_output, args.broker_url, breaking=True)
    write_json(output / "run_status.json", dict(complete=True, part=args.part))
    print("\nunknown = field is not in the receiver schema; absent:default='' = known optional field was not set.", flush=True)
    print("These are targeted compatibility cases, not timing or 10000-message failure experiments.", flush=True)
    print(f"Results: {output}", flush=True)


if __name__ == "__main__":
    main()
