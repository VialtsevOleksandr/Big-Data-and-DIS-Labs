import argparse
from collections import Counter
import logging
import time
from threading import Event
from uuid import uuid4

from messaging import Audit, DEFAULT_URL, connect, publish, queue_names
from schema import load_schema, VERSIONS
from observations import snapshot


class Inventory:
    def __init__(self, stock_per_product=1000000):
        self.stock_per_product = stock_per_product
        self.stock = {}
        self.results = {}

    def reserve(self, order):
        if order.order_id in self.results:
            return (*self.results[order.order_id], True)
        needed = Counter()
        for item in order.items:
            needed[item.product_id] += item.quantity
        reason = ""
        for product, quantity in needed.items():
            # Deterministic catalogue: every 11th SKU is unavailable.
            unavailable = product.startswith("sku-") and product[4:].isdigit() and int(product[4:]) % 11 == 0
            available = self.stock.setdefault(product, 0 if unavailable else self.stock_per_product)
            if available < quantity:
                reason = f"Insufficient stock: {product}"
                break
        if not reason:
            for product, quantity in needed.items():
                self.stock[product] -= quantity
        self.results[order.order_id] = (not reason, reason)
        return not reason, reason, False


def main():
    parser = argparse.ArgumentParser(description="Warehouse: RabbitMQ only, no direct RPC")
    parser.add_argument("--schema", choices=VERSIONS, default="v1")
    parser.add_argument("--broker-url", default=DEFAULT_URL)
    parser.add_argument("--namespace", default="lab")
    parser.add_argument("--audit")
    parser.add_argument("--delay-before-ack-ms", type=int, default=0)
    parser.add_argument("--delay-order-id", help="Delay only this order; deterministic crash experiment")
    parser.add_argument("--stock-per-product", type=int, default=1000000)
    parser.add_argument("--instance-id", default=None)
    parser.add_argument("--compact-audit", action="store_true")
    parser.add_argument("--audit-buffered", action="store_true", help="Flush each line; fsync at explicit crash checkpoints")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()
    if args.delay_before_ack_ms < 0 or args.stock_per_product < 0:
        parser.error("Delay and stock must not be negative")
    queue_names(args.namespace)
    logging.basicConfig(level=logging.WARNING if args.quiet else logging.INFO, format="%(asctime)s %(message)s")
    logging.getLogger("pika").setLevel(logging.WARNING)
    pb, _ = load_schema(args.schema)
    inventory = Inventory(args.stock_per_product)
    audit = Audit(args.audit, durable=not args.audit_buffered)
    instance = args.instance_id or uuid4().hex
    stopping = Event()
    try:
        while not stopping.is_set():
            connection = None
            try:
                connection, channel, (orders_queue, results_queue) = connect(args.broker_url, args.namespace)
                channel.basic_qos(prefetch_count=1)
                logging.info("Warehouse ready: schema=%s namespace=%s", args.schema, args.namespace)

                def callback(ch, method, properties, body):
                    event = pb.OrderCreated.FromString(body)
                    order = event.order
                    accepted, reason, duplicate = inventory.reserve(order)
                    result = pb.OrderProcessed(event_id=event.event_id, order_id=order.order_id,
                                               status=pb.ORDER_STATUS_CONFIRMED if accepted else pb.ORDER_STATUS_REJECTED,
                                               reason=reason)
                    publish(ch, results_queue, result, result.event_id)
                    logging.info("Processed order_id=%s status=%s duplicate=%s reason=%s",
                                 order.order_id, result.status, duplicate, reason)
                    audit.write("processed", order_id=order.order_id, event_id=event.event_id,
                                status=result.status, reason=reason, duplicate_in_memory=duplicate,
                                reserved=accepted and not duplicate, redelivered=method.redelivered,
                                schema=args.schema, instance_id=instance,
                                received=snapshot(order) if args.audit and not args.compact_audit else None,
                                sent_result=snapshot(result) if args.audit and not args.compact_audit else None)
                    if args.delay_before_ack_ms and (not args.delay_order_id or order.order_id == args.delay_order_id):
                        audit.flush_durable()
                        audit.write("before_ack", order_id=order.order_id, event_id=event.event_id,
                                    instance_id=instance, delay_ms=args.delay_before_ack_ms)
                        audit.flush_durable()
                        # Heartbeats continue even while this delivery remains unacknowledged.
                        connection.sleep(args.delay_before_ack_ms / 1000)
                    ch.basic_ack(method.delivery_tag)
                    audit.write("acked", order_id=order.order_id, instance_id=instance)

                channel.basic_consume(orders_queue, callback, auto_ack=False)
                audit.write("ready", schema=args.schema, namespace=args.namespace, instance_id=instance)
                channel.start_consuming()
            except KeyboardInterrupt:
                stopping.set()
            except Exception:
                logging.exception("Warehouse reconnecting; unacked deliveries will be redelivered")
                time.sleep(1)
            finally:
                if connection and connection.is_open:
                    connection.close()
    finally:
        audit.close()


if __name__ == "__main__":
    main()
