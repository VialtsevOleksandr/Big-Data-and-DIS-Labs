"""Генератор даних на своїй машині, без Docker.

  python generate_data.py --notes 100000 --seed 42 --out data
  python generate_data.py --help

Це той самий генератор, що й `python -m notes.generate` у контейнері застосунку.
Для генерації у файл потрібен лише Python 3.10+, сторонні пакети не потрібні.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "app"))

from notes.generate import main  # noqa: E402

if __name__ == "__main__":
    sys.exit(main())
