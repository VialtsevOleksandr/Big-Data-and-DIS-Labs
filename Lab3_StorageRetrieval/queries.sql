-- Однаковий SQL для PostgreSQL і DuckDB.
-- A = product_id 568; B = store_id 90.
-- C для 3–6: [2025-07-01, 2025-08-01) UTC.
-- C для 7–9: [2025-07-15, 2025-07-16) UTC.
-- 7–9: різні product_type_id, кожна комбінація враховується один раз на чек.

-- query: 1 | Кількість проданого товару за одиницями
SELECT u.code AS unit, SUM(ri.quantity) AS total_quantity, COUNT(*) AS lines
FROM receipt_items ri
JOIN products p ON p.product_id = ri.product_id
JOIN units u ON u.unit_id = p.unit_id
GROUP BY u.code
ORDER BY u.code;

-- query: 2 | Загальна виручка
SELECT SUM(total_amount) AS revenue
FROM receipts;

-- query: 3 | Виручка за період C
SELECT SUM(total_amount) AS revenue
FROM receipts
WHERE sold_at >= TIMESTAMPTZ '2025-07-01 00:00:00+00'
  AND sold_at <  TIMESTAMPTZ '2025-08-01 00:00:00+00';

-- query: 4 | Кількість товару A в магазині B за період C
-- COUNT(*) рахує чеки, оскільки генератор не повторює product_id у чеку.
SELECT COALESCE(SUM(ri.quantity), 0) AS total_quantity, COUNT(*) AS receipts
FROM receipts r
JOIN receipt_items ri ON ri.receipt_id = r.receipt_id
WHERE ri.product_id = 568
  AND r.store_id = 90
  AND r.sold_at >= TIMESTAMPTZ '2025-07-01 00:00:00+00'
  AND r.sold_at <  TIMESTAMPTZ '2025-08-01 00:00:00+00';

-- query: 5 | Кількість товару A в усіх магазинах за період C
SELECT COALESCE(SUM(ri.quantity), 0) AS total_quantity, COUNT(*) AS receipts
FROM receipts r
JOIN receipt_items ri ON ri.receipt_id = r.receipt_id
WHERE ri.product_id = 568
  AND r.sold_at >= TIMESTAMPTZ '2025-07-01 00:00:00+00'
  AND r.sold_at <  TIMESTAMPTZ '2025-08-01 00:00:00+00';

-- query: 6 | Виручка кожного магазину за період C
-- LEFT JOIN залишає також магазини без продажів за період.
SELECT s.store_id, s.name AS store_name, COALESCE(SUM(r.total_amount), 0) AS revenue
FROM stores s
LEFT JOIN receipts r ON r.store_id = s.store_id
  AND r.sold_at >= TIMESTAMPTZ '2025-07-01 00:00:00+00'
  AND r.sold_at <  TIMESTAMPTZ '2025-08-01 00:00:00+00'
GROUP BY s.store_id, s.name
ORDER BY s.store_id;

-- query: 7 | Топ-10 пар типів товарів за день
WITH basket_types AS MATERIALIZED (
    SELECT DISTINCT ri.receipt_id, p.product_type_id
    FROM receipts r
    JOIN receipt_items ri ON ri.receipt_id = r.receipt_id
    JOIN products p ON p.product_id = ri.product_id
    WHERE r.sold_at >= TIMESTAMPTZ '2025-07-15 00:00:00+00'
      AND r.sold_at <  TIMESTAMPTZ '2025-07-16 00:00:00+00'
), top_combinations AS (
    SELECT b1.product_type_id AS type_1, b2.product_type_id AS type_2, COUNT(*) AS receipt_count
    FROM basket_types b1
    JOIN basket_types b2 ON b2.receipt_id = b1.receipt_id
      AND b2.product_type_id > b1.product_type_id
    GROUP BY b1.product_type_id, b2.product_type_id
    ORDER BY receipt_count DESC, type_1, type_2
    LIMIT 10
)
SELECT t.type_1, p1.name AS type_1_name,
       t.type_2, p2.name AS type_2_name,
       t.receipt_count
FROM top_combinations t
JOIN product_types p1 ON p1.product_type_id = t.type_1
JOIN product_types p2 ON p2.product_type_id = t.type_2
ORDER BY t.receipt_count DESC, t.type_1, t.type_2;

-- query: 8 | Топ-10 трійок типів товарів за день
WITH basket_types AS MATERIALIZED (
    SELECT DISTINCT ri.receipt_id, p.product_type_id
    FROM receipts r
    JOIN receipt_items ri ON ri.receipt_id = r.receipt_id
    JOIN products p ON p.product_id = ri.product_id
    WHERE r.sold_at >= TIMESTAMPTZ '2025-07-15 00:00:00+00'
      AND r.sold_at <  TIMESTAMPTZ '2025-07-16 00:00:00+00'
), top_combinations AS (
    SELECT b1.product_type_id AS type_1, b2.product_type_id AS type_2, b3.product_type_id AS type_3, COUNT(*) AS receipt_count
    FROM basket_types b1
    JOIN basket_types b2 ON b2.receipt_id = b1.receipt_id
      AND b2.product_type_id > b1.product_type_id
    JOIN basket_types b3 ON b3.receipt_id = b1.receipt_id
      AND b3.product_type_id > b2.product_type_id
    GROUP BY b1.product_type_id, b2.product_type_id, b3.product_type_id
    ORDER BY receipt_count DESC, type_1, type_2, type_3
    LIMIT 10
)
SELECT t.type_1, p1.name AS type_1_name,
       t.type_2, p2.name AS type_2_name,
       t.type_3, p3.name AS type_3_name,
       t.receipt_count
FROM top_combinations t
JOIN product_types p1 ON p1.product_type_id = t.type_1
JOIN product_types p2 ON p2.product_type_id = t.type_2
JOIN product_types p3 ON p3.product_type_id = t.type_3
ORDER BY t.receipt_count DESC, t.type_1, t.type_2, t.type_3;

-- query: 9 | Топ-10 четвірок типів товарів за день
WITH basket_types AS MATERIALIZED (
    SELECT DISTINCT ri.receipt_id, p.product_type_id
    FROM receipts r
    JOIN receipt_items ri ON ri.receipt_id = r.receipt_id
    JOIN products p ON p.product_id = ri.product_id
    WHERE r.sold_at >= TIMESTAMPTZ '2025-07-15 00:00:00+00'
      AND r.sold_at <  TIMESTAMPTZ '2025-07-16 00:00:00+00'
), top_combinations AS (
    SELECT b1.product_type_id AS type_1, b2.product_type_id AS type_2, b3.product_type_id AS type_3, b4.product_type_id AS type_4, COUNT(*) AS receipt_count
    FROM basket_types b1
    JOIN basket_types b2 ON b2.receipt_id = b1.receipt_id
      AND b2.product_type_id > b1.product_type_id
    JOIN basket_types b3 ON b3.receipt_id = b1.receipt_id
      AND b3.product_type_id > b2.product_type_id
    JOIN basket_types b4 ON b4.receipt_id = b1.receipt_id
      AND b4.product_type_id > b3.product_type_id
    GROUP BY b1.product_type_id, b2.product_type_id, b3.product_type_id, b4.product_type_id
    ORDER BY receipt_count DESC, type_1, type_2, type_3, type_4
    LIMIT 10
)
SELECT t.type_1, p1.name AS type_1_name,
       t.type_2, p2.name AS type_2_name,
       t.type_3, p3.name AS type_3_name,
       t.type_4, p4.name AS type_4_name,
       t.receipt_count
FROM top_combinations t
JOIN product_types p1 ON p1.product_type_id = t.type_1
JOIN product_types p2 ON p2.product_type_id = t.type_2
JOIN product_types p3 ON p3.product_type_id = t.type_3
JOIN product_types p4 ON p4.product_type_id = t.type_4
ORDER BY t.receipt_count DESC, t.type_1, t.type_2, t.type_3, t.type_4;
