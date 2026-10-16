"""CLI: python -m salesgen {generate|validate|stats} ..."""
from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path

from .config import PROFILES, Config


def _config_from_args(a) -> Config:
    cfg = Config.from_json(a.config) if a.config else PROFILES[a.profile]
    over = {k: v for k, v in {
        "seed": a.seed, "n_receipts": a.receipts, "n_stores": a.stores, "n_products": a.products,
        "start_date": a.start, "end_date": a.end, "batch_size": a.batch_size}.items() if v is not None}
    return dataclasses.replace(cfg, **over)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m salesgen", description="Генератор синтетичних продажів (DDIA, розд. 3)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    g = sub.add_parser("generate", help="згенерувати набір CSV + manifest.json")
    g.add_argument("--profile", choices=sorted(PROFILES), default="small",
                   help="готовий профіль (small=10k, pilot=100k, full=3M чеків); за замовчуванням small")
    g.add_argument("--config", help="JSON-конфігурація (замість --profile)")
    g.add_argument("--seed", type=int)
    g.add_argument("--receipts", type=int, help="кількість чеків")
    g.add_argument("--stores", type=int, help="кількість магазинів")
    g.add_argument("--products", type=int, help="кількість товарів")
    g.add_argument("--start", help="перший день, YYYY-MM-DD (UTC, включно)")
    g.add_argument("--end", help="день після останнього, YYYY-MM-DD (UTC, виключно)")
    g.add_argument("--batch-size", type=int, help="чеків на один буфер запису (на дані не впливає)")
    g.add_argument("--out", help="каталог результату (обов'язковий, крім --dry-run)")
    g.add_argument("--overwrite", action="store_true", help="дозволити перезапис непорожнього каталогу")
    g.add_argument("--dry-run", action="store_true", help="лише показати конфігурацію")

    v = sub.add_parser("validate", help="перевірити інваріанти набору")
    v.add_argument("out")
    v.add_argument("--no-hash", action="store_true", help="не перераховувати SHA-256")

    s = sub.add_parser("stats", help="статистика якості → stats.md і stats.json")
    s.add_argument("out")
    s.add_argument("--itemset-sample", type=int, default=20_000,
                   help="розмір випадкової вибірки чеків без повторів для трійок/четвірок")

    a = ap.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if a.cmd == "generate":
        cfg = _config_from_args(a)
        from .catalog import n_spec_types
        cfg.validate(n_spec_types())
        print(json.dumps(dataclasses.asdict(cfg), ensure_ascii=False))
        if a.dry_run:
            return 0
        if not a.out:
            ap.error("generate: потрібен --out")
        from .writer import run
        run(cfg, Path(a.out), overwrite=a.overwrite)
        return 0
    if a.cmd == "validate":
        from .validate import validate
        err, summary = validate(a.out, check_hashes=not a.no_hash)
        print(json.dumps(summary, ensure_ascii=False))
        if err.ok:
            print("OK: усі інваріанти виконані")
            return 0
        print("ПОМИЛКИ:")
        for k, c in err.counts.most_common():
            print(f"  {k}: {c}; приклади: {err.examples[k]}")
        return 1
    if a.cmd == "stats":
        from .stats import run as stats_run
        stats_run(a.out, a.itemset_sample)
        print(f"Записано {Path(a.out) / 'stats.md'} і stats.json")
        return 0
    return 2
