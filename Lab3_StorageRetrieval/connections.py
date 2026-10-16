"""Прості підключення до PostgreSQL і DuckDB з параметрами з .env."""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv
import psycopg


BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=False)


def connect_postgres():
    return psycopg.connect(
        host=os.getenv("PG_HOST", "127.0.0.1"),
        port=int(os.getenv("PG_PORT", "5432")),
        dbname=os.getenv("PG_DB", "storage_retrieval_lab"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASSWORD", ""),
        connect_timeout=int(os.getenv("PG_CONNECT_TIMEOUT", "5")),
        options="-c timezone=UTC",
        autocommit=True,
    )


def connect_duckdb(*, read_only=True):
    path = Path(os.getenv("DUCKDB_PATH", "data/sales.duckdb"))
    if not path.is_absolute():
        path = BASE_DIR / path
    if not read_only:
        path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(
        str(path),
        read_only=read_only,
        config={
            "TimeZone": "UTC",
            "threads": int(os.getenv("DUCKDB_THREADS", "4")),
            "memory_limit": os.getenv("DUCKDB_MEMORY_LIMIT", "2GB"),
        },
    )
