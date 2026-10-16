"""Погоджена схема (7 таблиць) і опис CSV-формату — спільні для генератора,
маніфесту й валідатора. SQL DDL тут не генерується (поза межами завдання)."""
from __future__ import annotations

# (column, sql_type, nullable)
TABLES: dict[str, list[tuple[str, str, bool]]] = {
    "units": [("unit_id", "SMALLINT", False), ("code", "VARCHAR(8)", False),
              ("name", "VARCHAR(50)", False), ("quantity_scale", "SMALLINT", False)],
    "categories": [("category_id", "SMALLINT", False), ("name", "VARCHAR(100)", False),
                   ("description", "TEXT", True)],
    "product_types": [("product_type_id", "SMALLINT", False), ("category_id", "SMALLINT", False),
                      ("name", "VARCHAR(100)", False), ("description", "TEXT", True)],
    "stores": [("store_id", "SMALLINT", False), ("name", "VARCHAR(120)", False),
               ("latitude", "NUMERIC(9,6)", True), ("longitude", "NUMERIC(9,6)", True),
               ("description", "TEXT", True)],
    "products": [("product_id", "SMALLINT", False), ("product_type_id", "SMALLINT", False),
                 ("unit_id", "SMALLINT", False), ("name", "VARCHAR(200)", False),
                 ("description", "TEXT", True), ("current_price", "NUMERIC(10,2)", False)],
    "receipts": [("receipt_id", "INTEGER", False), ("store_id", "SMALLINT", False),
                 ("sold_at", "TIMESTAMPTZ", False), ("total_amount", "NUMERIC(14,2)", False)],
    "receipt_items": [("receipt_id", "INTEGER", False), ("line_no", "SMALLINT", False),
                      ("product_id", "SMALLINT", False), ("quantity", "NUMERIC(10,3)", False),
                      ("unit_price", "NUMERIC(10,2)", False), ("line_amount", "NUMERIC(14,2)", False)],
}

# Порядок завантаження (батьківські таблиці раніше за дочірні)
LOAD_ORDER = ("units", "categories", "product_types", "stores", "products", "receipts", "receipt_items")

CSV_FORMAT = {
    "encoding": "UTF-8 (без BOM)",
    "delimiter": ",",
    "quote": '"',
    "escape": 'подвоєння лапок ("")',
    "quoting": "лише поля з комою, лапками або переносом рядка",
    "line_terminator": "\\n",
    "header": True,
    "header_in_every_part": True,
    "null": "порожнє нецитоване поле; порожніх фактичних рядків немає",
    "decimal_separator": ".",
    "numeric": "фіксована кількість знаків: quantity — 3, ціни/суми — 2, координати — 6",
    "timestamp": "ISO 8601, UTC, секундна точність, суфікс +00:00 (YYYY-MM-DDTHH:MM:SS+00:00)",
    "row_order": {"receipts": "(sold_at, receipt_id)", "receipt_items": "(receipt_id, line_no)"},
}


def header(table: str) -> str:
    return ",".join(c for c, _, _ in TABLES[table])


def schema_for_manifest() -> dict:
    return {t: [{"column": c, "type": ty, "nullable": n} for c, ty, n in cols] for t, cols in TABLES.items()}
