"""З'єднання з вузлами, пошук лідера, дрібні помічники для SQL."""
from __future__ import annotations

import json
import socket
import threading
import time
from pathlib import Path

import psycopg
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row

from . import config

# Щоб клієнт не висів хвилинами на з'єднанні з вузлом, який зник із мережі:
# перевірка кожну секунду, три невдачі або 3 с без підтвердження даних -> помилка.
TCP_OPTIONS = {
    "keepalives": 1,
    "keepalives_idle": 1,
    "keepalives_interval": 1,
    "keepalives_count": 3,
    "tcp_user_timeout": 3000,
}


def split_host(host: str) -> tuple[str, int]:
    """'repa-pg1' -> ('repa-pg1', 5432); 'localhost:55431' -> ('localhost', 55431)."""
    if ":" in host:
        name, port = host.rsplit(":", 1)
        return name, int(port)
    return host, config.PORT


def conninfo(host: str, *, admin: bool = False, dbname: str | None = None,
             connect_timeout: int = 5, **extra) -> str:
    name, port = split_host(host)
    params = {
        "host": name,
        "port": port,
        "dbname": dbname or config.DB,
        "user": config.ADMIN_USER if admin else config.APP_USER,
        "password": config.ADMIN_PASSWORD if admin else config.APP_PASSWORD,
        "connect_timeout": connect_timeout,
        "application_name": "notes-admin" if admin else "notes-app",
    }
    params.update(TCP_OPTIONS)
    params.update(extra)
    return make_conninfo(**params)


def connect(host: str, *, admin: bool = False, autocommit: bool = True, **kwargs) -> psycopg.Connection:
    """З'єднання з одним вузлом. autocommit: кожна інструкція — окрема транзакція."""
    return psycopg.connect(conninfo(host, admin=admin, **kwargs), autocommit=autocommit, row_factory=dict_row)


def one(conn: psycopg.Connection, query, params=None) -> dict | None:
    return conn.execute(query, params).fetchone()


def scalar(conn: psycopg.Connection, query, params=None):
    row = conn.execute(query, params).fetchone()
    return None if row is None else next(iter(row.values()))


def rows(conn: psycopg.Connection, query, params=None) -> list[dict]:
    return conn.execute(query, params).fetchall()


def lsn_to_int(lsn: str) -> int:
    """'0/3000148' -> номер байта в журналі WAL."""
    high, low = lsn.split("/")
    return (int(high, 16) << 32) | int(low, 16)


def mib(n_bytes) -> float:
    return round(float(n_bytes) / 1048576, 3)


def dump(obj, path: str | Path | None = None) -> None:
    """Друкує результат у консоль як JSON і, якщо задано шлях, зберігає у файл."""
    text = json.dumps(obj, indent=2, default=str, ensure_ascii=True)
    print(text, flush=True)
    if path:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text + "\n", encoding="utf-8")


class LeaderFinder:
    """Знаходить вузол, що приймає записи, серед відомого списку вузлів.

    Застосунок не знає, хто лідер: він по черзі підключається до кожного вузла з параметром
    libpq target_session_attrs=read-write. Фоловер таке з'єднання відхиляє («session is
    read-only»), лідер приймає. Так застосунок сам знаходить нового лідера після failover.

    Адреси вузлів кешуються й оновлюються у фоновому потоці: розв'язання імені зупиненого
    контейнера може тривати секунди і не має блокувати цикл запису.
    """

    def __init__(self, hosts: list[str]):
        self.hosts = list(hosts)
        self.addresses: dict[str, str] = {}
        self.skip_until: dict[str, float] = {}
        self.last_errors: dict[str, str] = {}
        self._resolve_all()
        threading.Thread(target=self._resolver_loop, daemon=True).start()

    def _resolve_all(self) -> None:
        for host in self.hosts:
            name, _ = split_host(host)
            try:
                self.addresses[host] = socket.gethostbyname(name)
            except OSError:
                pass

    def _resolver_loop(self) -> None:
        while True:
            time.sleep(2.0)
            self._resolve_all()

    def try_connect(self) -> tuple[psycopg.Connection | None, str | None]:
        """Один обхід вузлів. Повертає (з'єднання, хост) або (None, None), якщо лідера зараз немає."""
        now = time.monotonic()
        for host in self.hosts:
            address = self.addresses.get(host)
            if address is None or now < self.skip_until.get(host, 0.0):
                continue
            _, port = split_host(host)
            # Швидка перевірка, чи вузол узагалі відповідає: мертвий вузол не затримує обхід.
            try:
                socket.create_connection((address, port), timeout=0.3).close()
            except OSError as exc:
                self.skip_until[host] = time.monotonic() + 1.0
                self.last_errors[host] = f"tcp: {exc}"
                continue
            try:
                conn = psycopg.connect(
                    conninfo(f"{address}:{port}", connect_timeout=2, target_session_attrs="read-write"),
                    autocommit=True, row_factory=dict_row,
                )
            except psycopg.OperationalError as exc:
                self.last_errors[host] = str(exc).strip().splitlines()[-1] if str(exc).strip() else "error"
                continue
            return conn, host
        return None, None
