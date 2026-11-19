"""Операції сервісу нотаток і сесія з гарантією read-after-write."""
from __future__ import annotations

import time
from collections import Counter

import psycopg

from .db import one, rows, scalar

INSERT_SQL = "INSERT INTO notes (author_id, title, body) VALUES (%s, %s, %s) RETURNING id"
SELECT_SQL = "SELECT id, author_id, title, body, updated_at FROM notes WHERE id = %s"


class NotesService:
    """Операції над нотатками через одне з'єднання з одним вузлом."""

    def __init__(self, conn: psycopg.Connection):
        self.conn = conn

    def create(self, author_id: int, title: str, body: str) -> int:
        return scalar(self.conn, INSERT_SQL, (author_id, title, body))

    def get(self, note_id: int) -> dict | None:
        return one(self.conn, SELECT_SQL, (note_id,))

    def update_title(self, note_id: int, title: str) -> bool:
        cur = self.conn.execute(
            "UPDATE notes SET title = %s, updated_at = clock_timestamp() WHERE id = %s", (title, note_id))
        return cur.rowcount == 1

    def update_body(self, note_id: int, body: str) -> bool:
        cur = self.conn.execute(
            "UPDATE notes SET body = %s, updated_at = clock_timestamp() WHERE id = %s", (body, note_id))
        return cur.rowcount == 1

    def delete(self, note_id: int) -> bool:
        cur = self.conn.execute("DELETE FROM notes WHERE id = %s", (note_id,))
        return cur.rowcount == 1

    def list_by_author(self, author_id: int, limit: int = 50) -> list[dict]:
        return rows(
            self.conn,
            "SELECT id, author_id, title, body, updated_at FROM notes "
            "WHERE author_id = %s ORDER BY updated_at DESC LIMIT %s",
            (author_id, limit),
        )


class ReadYourWritesSession:
    """Сесія одного користувача: пише на лідера, читає з фоловера і бачить власні записи.

    Спосіб: після кожного запису сесія запам'ятовує позицію WAL лідера (LSN), до якої цей запис
    уже скинуто на диск. Перед читанням вона питає фоловера, чи він відтворив журнал до цієї
    позиції. Якщо так — читає з фоловера. Якщо ні — чекає не довше wait_ms, а потім читає з лідера.

    Ціна: зайвий запит до лідера після кожного запису (взяти LSN), зайвий запит до фоловера
    перед кожним читанням (перевірити LSN), а при відставанні — очікування або читання з лідера.
    """

    def __init__(self, leader: psycopg.Connection, follower: psycopg.Connection,
                 wait_ms: float = 10.0, poll_ms: float = 1.0):
        self.leader = NotesService(leader)
        self.follower = NotesService(follower)
        self.wait_s = wait_ms / 1000.0
        self.poll_s = poll_ms / 1000.0
        self.last_write_lsn: str | None = None
        self.sources: Counter = Counter()
        self.wait_ms_samples: list[float] = []

    def _remember_write_position(self) -> None:
        # Після COMMIT із synchronous_commit=on запис уже на диску лідера,
        # тому позиція скинутого WAL гарантовано не менша за кінець нашої транзакції.
        self.last_write_lsn = scalar(self.leader.conn, "SELECT pg_current_wal_flush_lsn()")

    def _follower_caught_up(self) -> bool:
        if self.last_write_lsn is None:
            return True
        return bool(scalar(self.follower.conn,
                           "SELECT pg_last_wal_replay_lsn() >= %s::pg_lsn", (self.last_write_lsn,)))

    def create(self, author_id: int, title: str, body: str) -> int:
        note_id = self.leader.create(author_id, title, body)
        self._remember_write_position()
        return note_id

    def update_title(self, note_id: int, title: str) -> bool:
        changed = self.leader.update_title(note_id, title)
        self._remember_write_position()
        return changed

    def get(self, note_id: int) -> tuple[dict | None, str]:
        """Повертає (нотатка, джерело): follower, follower_waited або leader."""
        if self._follower_caught_up():
            self.sources["follower"] += 1
            return self.follower.get(note_id), "follower"

        started = time.perf_counter()
        deadline = started + self.wait_s
        while time.perf_counter() < deadline:
            time.sleep(self.poll_s)
            if self._follower_caught_up():
                self.wait_ms_samples.append((time.perf_counter() - started) * 1000.0)
                self.sources["follower_waited"] += 1
                return self.follower.get(note_id), "follower_waited"

        self.wait_ms_samples.append((time.perf_counter() - started) * 1000.0)
        self.sources["leader"] += 1
        return self.leader.get(note_id), "leader"
