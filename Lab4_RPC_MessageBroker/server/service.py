"""RPC handlers and a thread-safe in-memory repository."""

from collections import defaultdict
import logging
from threading import Lock
import time
from uuid import uuid4
from queue import Queue

import grpc

from schema import load_schema
from observations import snapshot


class InvalidOrder(ValueError):
    pass


class RequestConflict(ValueError):
    pass


def validate_create(request) -> None:
    for field in ("request_id", "buyer_id"):
        if not getattr(request, field).strip():
            raise InvalidOrder(f"{field} must not be empty")
    if not request.HasField("shipping_address"):
        raise InvalidOrder("shipping_address is required")
    for field in ("country", "city", "street", "postal_code"):
        if not getattr(request.shipping_address, field).strip():
            raise InvalidOrder(f"shipping_address.{field} must not be empty")
    if not 1 <= len(request.items) <= 20:
        raise InvalidOrder("items must contain between 1 and 20 entries")
    for index, item in enumerate(request.items):
        if not item.product_id.strip():
            raise InvalidOrder(f"items[{index}].product_id must not be empty")
        if item.quantity == 0:
            raise InvalidOrder(f"items[{index}].quantity must be positive")
        if item.unit_price_minor <= 0:
            raise InvalidOrder(f"items[{index}].unit_price_minor must be positive")


class OrderRepository:
    def __init__(self, *, idempotency: bool = True, schema="v1", events=False):
        self.pb, _ = load_schema(schema)
        self.schema = schema
        self._lock = Lock()
        self._orders = {}
        self._by_buyer: dict[str, list[str]] = defaultdict(list)
        self._requests: dict[str, tuple[bytes, str]] = {}
        self._idempotency = idempotency
        self.outbox = Queue() if events else None
        self.result_deliveries = 0
        self.duplicate_results = 0
        self._results = set()

    def _copy(self, order):
        result = self.pb.Order()
        result.CopyFrom(order)
        return result

    def create(self, request):
        validate_create(request)
        fingerprint = request.SerializeToString(deterministic=True)
        with self._lock:
            if self._idempotency and request.request_id in self._requests:
                previous, order_id = self._requests[request.request_id]
                if fingerprint != previous:
                    raise RequestConflict("request_id was already used with different data")
                return self._copy(self._orders[order_id]), False

            order = self.pb.Order(
                order_id=str(uuid4()),
                buyer_id=request.buyer_id,
                status=self.pb.ORDER_STATUS_PENDING,
                shipping_address=request.shipping_address,
                items=request.items,
            )
            order.created_at.GetCurrentTime()
            for name in ("comment", "promo_code"):
                if name in request.DESCRIPTOR.fields_by_name and request.HasField(name):
                    setattr(order, name, getattr(request, name))
            self._orders[order.order_id] = order
            self._by_buyer[order.buyer_id].append(order.order_id)
            if self._idempotency:
                self._requests[request.request_id] = (fingerprint, order.order_id)
            if self.outbox is not None:
                self.outbox.put(self.pb.OrderCreated(event_id=str(uuid4()), order=order))
            return self._copy(order), True

    def get(self, order_id: str):
        with self._lock:
            order = self._orders.get(order_id)
            return None if order is None else self._copy(order)

    def list_by_buyer(self, buyer_id: str):
        with self._lock:
            return [self._copy(self._orders[key]) for key in self._by_buyer.get(buyer_id, ())]

    def apply_result(self, result):
        if result.status not in (self.pb.ORDER_STATUS_CONFIRMED, self.pb.ORDER_STATUS_REJECTED):
            raise ValueError("Invalid warehouse status")
        with self._lock:
            self.result_deliveries += 1
            if result.event_id in self._results:
                self.duplicate_results += 1
                return False
            order = self._orders.get(result.order_id)
            if order is None:
                raise ValueError("Result references an unknown order (server memory was reset)")
            order.status = result.status
            self._results.add(result.event_id)
            return True


class OrderService:
    def __init__(self, repository: OrderRepository, *, create_response_delay_ms: int = 0, observations=None):
        self.repository = repository
        self.pb = repository.pb
        self.delay_seconds = create_response_delay_ms / 1000
        self.observations = observations

    def CreateOrder(self, request, context):
        if self.observations:
            self.observations.write("rpc_request", schema=self.repository.schema,
                                    request_id=request.request_id, received=snapshot(request))
        try:
            order, created = self.repository.create(request)
        except InvalidOrder as error:
            if self.observations:
                self.observations.write("rpc_rejected", request_id=request.request_id,
                                        code="INVALID_ARGUMENT", reason=str(error))
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, str(error))
        except RequestConflict as error:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, str(error))
        logging.info("request_id=%s order_id=%s created=%s", request.request_id, order.order_id, created)
        if self.observations:
            self.observations.write("rpc_created" if created else "rpc_reused", request_id=request.request_id,
                                    order_id=order.order_id, stored=snapshot(order))
        # Delay AFTER committing: a client deadline does not roll back the order.
        if self.delay_seconds:
            time.sleep(self.delay_seconds)
        return order

    def GetOrder(self, request, context):
        if not request.order_id.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "order_id must not be empty")
        order = self.repository.get(request.order_id)
        if order is None:
            context.abort(grpc.StatusCode.NOT_FOUND, "order was not found")
        return order

    def ListOrdersByBuyer(self, request, context):
        if not request.buyer_id.strip():
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "buyer_id must not be empty")
        return self.pb.ListOrdersByBuyerResponse(orders=self.repository.list_by_buyer(request.buyer_id))
