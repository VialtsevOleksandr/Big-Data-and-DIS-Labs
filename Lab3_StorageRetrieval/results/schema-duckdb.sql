-- Фактичні DDL-команди завантажувача load_data.py; дані не змінювалися.

CREATE TABLE "units" (
    "unit_id" SMALLINT NOT NULL,
    "code" VARCHAR(8) NOT NULL,
    "name" VARCHAR(50) NOT NULL,
    "quantity_scale" SMALLINT NOT NULL,
    CHECK (quantity_scale IN (0, 3)),
    PRIMARY KEY (unit_id),
    UNIQUE (code)
);

CREATE TABLE "categories" (
    "category_id" SMALLINT NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    "description" TEXT,
    PRIMARY KEY (category_id),
    UNIQUE (name)
);

CREATE TABLE "product_types" (
    "product_type_id" SMALLINT NOT NULL,
    "category_id" SMALLINT NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    "description" TEXT,
    PRIMARY KEY (product_type_id),
    UNIQUE (category_id, name),
    FOREIGN KEY (category_id) REFERENCES categories (category_id)
);

CREATE TABLE "stores" (
    "store_id" SMALLINT NOT NULL,
    "name" VARCHAR(120) NOT NULL,
    "latitude" NUMERIC(9,6),
    "longitude" NUMERIC(9,6),
    "description" TEXT,
    CHECK ((latitude IS NULL) = (longitude IS NULL)),
    CHECK (latitude BETWEEN -90 AND 90),
    CHECK (longitude BETWEEN -180 AND 180),
    PRIMARY KEY (store_id)
);

CREATE TABLE "products" (
    "product_id" SMALLINT NOT NULL,
    "product_type_id" SMALLINT NOT NULL,
    "unit_id" SMALLINT NOT NULL,
    "name" VARCHAR(200) NOT NULL,
    "description" TEXT,
    "current_price" NUMERIC(10,2) NOT NULL,
    CHECK (current_price BETWEEN 0 AND 99999999.99),
    PRIMARY KEY (product_id),
    FOREIGN KEY (product_type_id) REFERENCES product_types (product_type_id),
    FOREIGN KEY (unit_id) REFERENCES units (unit_id)
);

CREATE TABLE "receipts" (
    "receipt_id" INTEGER NOT NULL,
    "store_id" SMALLINT NOT NULL,
    "sold_at" TIMESTAMPTZ NOT NULL,
    "total_amount" NUMERIC(14,2) NOT NULL,
    CHECK (total_amount BETWEEN 0 AND 999999999999.99)
);

CREATE TABLE "receipt_items" (
    "receipt_id" INTEGER NOT NULL,
    "line_no" SMALLINT NOT NULL,
    "product_id" SMALLINT NOT NULL,
    "quantity" NUMERIC(10,3) NOT NULL,
    "unit_price" NUMERIC(10,2) NOT NULL,
    "line_amount" NUMERIC(14,2) NOT NULL,
    CHECK (line_no > 0),
    CHECK (quantity > 0 AND quantity <= 9999999.999),
    CHECK (unit_price BETWEEN 0 AND 99999999.99),
    CHECK (line_amount BETWEEN 0 AND 999999999999.99),
    CHECK (line_amount = ROUND(quantity * unit_price, 2))
);
