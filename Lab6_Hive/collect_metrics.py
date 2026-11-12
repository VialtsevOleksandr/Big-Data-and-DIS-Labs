"""Зведення часу й RECORDS_IN_Map_1 з логів beeline однієї частини.

    python collect_metrics.py evidence/part1/<run_id>

Для кожного логу виписує запити з часом виконання (з рядка beeline
"... (X seconds)") і лічильником RECORDS_IN_Map_1. Однакові запити в
одному лозі вважаються повторами одного заміру: для них рахується медіана.
Результат друкується й зберігається в metrics.md у тій самій папці.
"""

import re
import statistics
import sys
from pathlib import Path

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
# Текст запиту беремо з журналу HiveServer2: beeline зі stdin сам його не друкує.
EXECUTING = re.compile(r"Executing command\(queryId=[^)]*\):\s?(.*)$")
LOG_LINE = re.compile(r"^(INFO|WARN|ERROR|DEBUG)\s*:")
TIME = re.compile(r"(?:([\d,]+) rows? selected|No rows affected|([\d,]+) rows? affected) \(([\d.]+) seconds\)")
RECORDS = re.compile(r"RECORDS_IN_Map_1:\s*(\d+)")


def fmt_time(value):
    return f"{value:.3f}".replace(".", ",")


def fmt_int(value):
    return f"{value:,}".replace(",", " ")


def parse(log):
    """Список запитів: текст, рядки результату, час, RECORDS_IN_Map_1."""
    statements, current, collecting = [], None, False
    for raw in log.read_text(encoding="utf-8", errors="replace").splitlines():
        line = ANSI.sub("", raw)
        executing = EXECUTING.search(line)
        if executing:
            current = {"sql": [executing.group(1)], "rows": None, "time": None, "records": None}
            statements.append(current)
            collecting = True
            continue
        if current is None:
            continue
        if collecting:
            if LOG_LINE.match(line) or not line.strip():
                collecting = False
            else:
                current["sql"].append(line)
                continue
        time = TIME.search(line)
        if time and current["time"] is None:
            rows = time.group(1) or time.group(2) or "0"
            current["rows"] = int(rows.replace(",", ""))
            current["time"] = float(time.group(3))
        records = RECORDS.search(line)
        if records and current["records"] is None:
            current["records"] = int(records.group(1))
    for st in statements:
        st["sql"] = " ".join(" ".join(st["sql"]).split())
    return [st for st in statements if st["time"] is not None]


def report(run_dir):
    out = [f"# Метрики: {run_dir.as_posix()}", ""]
    for log in sorted(run_dir.glob("*.log")):
        statements = parse(log)
        if not statements:
            continue
        out += [f"## {log.name}", "",
                "| Запит | Рядків результату | RECORDS_IN_Map_1 | Час, с |",
                "| --- | --- | --- | --- |"]
        groups = {}
        for st in statements:
            groups.setdefault(st["sql"], []).append(st)
            short = st["sql"] if len(st["sql"]) <= 90 else st["sql"][:87] + "..."
            rec = fmt_int(st["records"]) if st["records"] is not None else "—"
            out.append(f"| `{short}` | {st['rows']} | {rec} | {fmt_time(st['time'])} |")
        repeated = {sql: sts for sql, sts in groups.items() if len(sts) >= 3}
        for sql, sts in repeated.items():
            times = [st["time"] for st in sts]
            short = sql if len(sql) <= 90 else sql[:87] + "..."
            out += ["", f"Повтори `{short}`: " + "; ".join(fmt_time(t) for t in times)
                    + f" → медіана **{fmt_time(statistics.median(times))}** с"]
        out.append("")
    return "\n".join(out)


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    run_dir = Path(sys.argv[1])
    text = report(run_dir)
    (run_dir / "metrics.md").write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
