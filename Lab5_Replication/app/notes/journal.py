"""Власний журнал застосунку: кожна спроба запису, її результат і час підтвердження.

Формат — CSV, один рядок на спробу:
  seq         порядковий номер спроби в межах запуску
  op          операція (insert)
  status      ok        — база підтвердила запис;
              error     — запит надіслано, підтвердження немає (доля запису невідома);
              no_leader — надсилати було нікуди, жоден вузол не приймав записи
  note_id     ідентифікатор, який повернула база (лише для ok)
  title       унікальний заголовок "<run_id>/<seq>": за ним запис звіряється з вузлами
  node        cluster_name вузла, що підтвердив запис
  sent_at     час надсилання, секунди Unix
  acked_at    час підтвердження, секунди Unix
  latency_ms  час від надсилання до підтвердження
  detail      текст помилки або попередження сервера
"""
from __future__ import annotations

import csv
import time
from pathlib import Path

FIELDS = ["seq", "op", "status", "note_id", "title", "node", "sent_at", "acked_at", "latency_ms", "detail"]


class Journal:
    def __init__(self, path: str | Path, flush_every: int = 100, flush_interval: float = 1.0):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = self.path.open("w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._file)
        self._writer.writerow(FIELDS)
        self._file.flush()
        self._pending = 0
        self._flush_every = flush_every
        self._flush_interval = flush_interval
        self._last_flush = time.monotonic()

    def write(self, *, seq: int, status: str, title: str = "", note_id=None, node: str = "",
              sent_at: float | None = None, acked_at: float | None = None,
              latency_ms: float | None = None, detail: str = "", op: str = "insert") -> None:
        self._writer.writerow([
            seq, op, status,
            "" if note_id is None else note_id,
            title, node,
            "" if sent_at is None else f"{sent_at:.6f}",
            "" if acked_at is None else f"{acked_at:.6f}",
            "" if latency_ms is None else f"{latency_ms:.3f}",
            detail.replace("\n", " ")[:300],
        ])
        self._pending += 1
        now = time.monotonic()
        if self._pending >= self._flush_every or now - self._last_flush >= self._flush_interval:
            self._file.flush()
            self._pending = 0
            self._last_flush = now

    def close(self) -> None:
        self._file.flush()
        self._file.close()


def read_journal(path: str | Path) -> list[dict]:
    """Читає журнал і приводить числові поля до чисел."""
    result = []
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            row["seq"] = int(row["seq"])
            row["note_id"] = int(row["note_id"]) if row["note_id"] else None
            row["sent_at"] = float(row["sent_at"]) if row["sent_at"] else None
            row["acked_at"] = float(row["acked_at"]) if row["acked_at"] else None
            row["latency_ms"] = float(row["latency_ms"]) if row["latency_ms"] else None
            result.append(row)
    return result
