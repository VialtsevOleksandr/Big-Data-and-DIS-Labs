"""Налаштування застосунку зі змінних середовища (їх задає docker-compose)."""
from __future__ import annotations

import os

DB = os.environ.get("NOTES_DB", "notes")
PORT = int(os.environ.get("NOTES_PORT", "5432"))

APP_USER = os.environ.get("NOTES_APP_USER", "notes_app")
APP_PASSWORD = os.environ.get("NOTES_APP_PASSWORD", "notes_app_pw")
ADMIN_USER = os.environ.get("NOTES_ADMIN_USER", "postgres")
ADMIN_PASSWORD = os.environ.get("NOTES_ADMIN_PASSWORD", "postgres_pw")
REPL_USER = os.environ.get("NOTES_REPL_USER", "replicator")
REPL_PASSWORD = os.environ.get("NOTES_REPL_PASSWORD", "repl_pw")

# Усі вузли конфігурації; перший — лідер за замовчуванням.
NODES = [h.strip() for h in os.environ.get("NOTES_NODES", "").split(",") if h.strip()]


def node_name(host: str) -> str:
    """repa-pg2 -> pg2: коротке ім'я вузла (cluster_name, application_name, префікс слота)."""
    return host.split(":")[0].rsplit("-", 1)[-1]


def parse_hosts(value: str | None) -> list[str]:
    """Список вузлів з аргументу командного рядка або всі вузли конфігурації."""
    if not value:
        return list(NODES)
    return [h.strip() for h in value.split(",") if h.strip()]


def default_leader() -> str:
    if not NODES:
        raise SystemExit("NOTES_NODES is empty: pass --host/--leader explicitly")
    return NODES[0]
