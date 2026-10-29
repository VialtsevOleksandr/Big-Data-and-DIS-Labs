"""Volatile outbox publisher and independent result consumer."""
import logging
from queue import Empty
from threading import Event, Thread

from messaging import Audit, connect, publish, queue_names


class BrokerBridge:
    def __init__(self, repository, url, namespace, audit=None):
        queue_names(namespace)
        self.repository = repository
        self.url, self.namespace, self.audit = url, namespace, audit
        self.stopping = Event()
        self.threads = []

    def start(self):
        for target in (self._publish, self._consume):
            thread = Thread(target=target, daemon=True)
            thread.start()
            self.threads.append(thread)

    def stop(self):
        self.stopping.set()
        for thread in self.threads:
            thread.join(timeout=12)

    def _publish(self):
        pending = None
        while not self.stopping.is_set():
            connection = None
            try:
                connection, channel, (orders_queue, _) = connect(self.url, self.namespace)
                while not self.stopping.is_set():
                    if pending is None:
                        try:
                            pending = self.repository.outbox.get(timeout=0.1)
                        except Empty:
                            connection.process_data_events(time_limit=0)
                            continue
                    publish(channel, orders_queue, pending, pending.event_id)
                    logging.info("Published order_id=%s to %s", pending.order.order_id, orders_queue)
                    self.repository.outbox.task_done()
                    pending = None
            except Exception:
                logging.exception("Publisher reconnecting; an uncertain publish may be repeated")
                self.stopping.wait(1)
            finally:
                if connection and connection.is_open:
                    connection.close()

    def _consume(self):
        audit = Audit(self.audit)
        try:
            while not self.stopping.is_set():
                connection = None
                try:
                    connection, channel, (_, results_queue) = connect(self.url, self.namespace)
                    channel.basic_qos(prefetch_count=1)

                    def callback(ch, method, properties, body):
                        result = self.repository.pb.OrderProcessed.FromString(body)
                        try:
                            updated = self.repository.apply_result(result)
                        except ValueError as error:
                            audit.write("result_rejected", order_id=result.order_id, reason=str(error))
                            ch.basic_reject(method.delivery_tag, requeue=False)
                            return
                        audit.write("result", order_id=result.order_id, event_id=result.event_id,
                                    status=result.status, updated=updated, redelivered=method.redelivered)
                        logging.info("Warehouse result order_id=%s status=%s updated=%s",
                                     result.order_id, result.status, updated)
                        ch.basic_ack(method.delivery_tag)

                    channel.basic_consume(results_queue, callback, auto_ack=False)
                    while not self.stopping.is_set():
                        connection.process_data_events(time_limit=0.2)
                except Exception:
                    logging.exception("Result consumer reconnecting")
                    self.stopping.wait(1)
                finally:
                    if connection and connection.is_open:
                        connection.close()
        finally:
            audit.close()
