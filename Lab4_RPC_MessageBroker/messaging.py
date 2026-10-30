"""RabbitMQ primitives; every thread owns its own connection/channel."""
import json
import os
from pathlib import Path
import re
import time
from threading import Lock

import pika

DEFAULT_URL = "amqp://dis:dis-local@127.0.0.1:5673/%2F"


def queue_names(namespace):
    if not re.fullmatch(r"[a-zA-Z0-9_-]{1,80}", namespace):
        raise ValueError("Namespace must contain only letters, digits, hyphen or underscore")
    return f"dis.{namespace}.orders", f"dis.{namespace}.results"


def connect(url, namespace):
    names = queue_names(namespace)
    parameters = pika.URLParameters(url)
    parameters.heartbeat = 30
    parameters.blocked_connection_timeout = 30
    parameters.socket_timeout = 5
    parameters.stack_timeout = 10
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    for name in names:
        channel.queue_declare(queue=name, durable=True)
    channel.confirm_delivery()
    return connection, channel, names


def publish(channel, queue, message, message_id):
    channel.basic_publish(
        exchange="", routing_key=queue, mandatory=True,
        body=message.SerializeToString(),
        properties=pika.BasicProperties(content_type="application/x-protobuf", delivery_mode=2,
                                        message_id=message_id),
    )


class Audit:
    """Evidence only, not order storage or a persistent deduplication database."""
    def __init__(self, filename=None, durable=True):
        self.file = None
        self.durable = durable
        self.lock = Lock()
        if filename:
            path = Path(filename)
            path.parent.mkdir(parents=True, exist_ok=True)
            self.file = path.open("a", encoding="utf-8", buffering=1)

    def write(self, kind, **fields):
        with self.lock:
            if self.file:
                self.file.write(json.dumps(dict(kind=kind, time_ns=time.time_ns(), **fields), ensure_ascii=False) + "\n")
                self.file.flush()
                if self.durable:
                    os.fsync(self.file.fileno())

    def flush_durable(self):
        with self.lock:
            if self.file:
                self.file.flush()
                os.fsync(self.file.fileno())

    def close(self):
        with self.lock:
            if self.file:
                self.file.close()
                self.file = None
