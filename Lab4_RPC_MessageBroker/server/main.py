"""Run from the lab directory: python -m server.main."""

import argparse
from concurrent import futures
import logging

import grpc

from schema import load_schema, VERSIONS
from server.service import OrderRepository, OrderService


def create_server(*, address="127.0.0.1:50051", idempotency=True, create_response_delay_ms=0,
                  schema="v1", events=False, observations=None):
    _, orders_pb2_grpc = load_schema(schema)
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=8),
        options=[("grpc.max_receive_message_length", 32 * 1024 * 1024),
                 ("grpc.max_send_message_length", 32 * 1024 * 1024)],
    )
    repository = OrderRepository(idempotency=idempotency, schema=schema, events=events)
    orders_pb2_grpc.add_OrderServiceServicer_to_server(
        OrderService(repository, create_response_delay_ms=create_response_delay_ms, observations=observations), server
    )
    port = server.add_insecure_port(address)
    if port == 0:
        raise RuntimeError(f"Cannot bind to {address}")
    return server, repository, port


def main():
    parser = argparse.ArgumentParser(description="DIS order service: gRPC, Protobuf v1, in-memory storage")
    parser.add_argument("--address", default="127.0.0.1:50051")
    parser.add_argument("--create-response-delay-ms", type=int, default=0)
    parser.add_argument("--disable-idempotency", action="store_true")
    parser.add_argument("--schema", choices=VERSIONS, default="v1")
    parser.add_argument("--broker-url", help="Enable asynchronous warehouse integration")
    parser.add_argument("--namespace", default="lab")
    parser.add_argument("--audit", help="JSONL audit file for results")
    parser.add_argument("--observations", help="Optional decoded request/order snapshots for schema checks")
    parser.add_argument("--publication-audit", help="Optional confirmed-publish IDs for delivery experiments")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    if args.create_response_delay_ms < 0:
        parser.error("--create-response-delay-ms must not be negative")
    logging.basicConfig(level=logging.WARNING if args.quiet else logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    observations = None
    if args.observations:
        from messaging import Audit
        observations = Audit(args.observations)
    server, repository, _ = create_server(
        address=args.address,
        idempotency=not args.disable_idempotency,
        create_response_delay_ms=args.create_response_delay_ms,
        schema=args.schema, events=bool(args.broker_url),
        observations=observations,
    )
    bridge = None
    if args.broker_url:
        from server.broker import BrokerBridge
        bridge = BrokerBridge(repository, args.broker_url, args.namespace, args.audit, args.publication_audit)
        bridge.start()
    server.start()
    logging.info("Listening on %s; schema=%s; storage=memory; idempotency=%s; broker=%s",
                 args.address, args.schema, not args.disable_idempotency, bool(bridge))
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=2).wait()
    finally:
        if bridge:
            bridge.stop()
        if observations:
            observations.close()


if __name__ == "__main__":
    main()
