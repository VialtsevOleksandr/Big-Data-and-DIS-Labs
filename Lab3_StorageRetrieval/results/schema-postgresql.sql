-- Фактичні DDL-команди завантажувача load_data.py; дані не змінювалися.

CREATE TABLE public."units" (
    "unit_id" SMALLINT NOT NULL,
    "code" VARCHAR(8) NOT NULL,
    "name" VARCHAR(50) NOT NULL,
    "quantity_scale" SMALLINT NOT NULL,
    CHECK (quantity_scale IN (0, 3))
);

CREATE TABLE public."categories" (
    "category_id" SMALLINT NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    "description" TEXT
);

CREATE TABLE public."product_types" (
    "product_type_id" SMALLINT NOT NULL,
    "category_id" SMALLINT NOT NULL,
    "name" VARCHAR(100) NOT NULL,
    "description" TEXT
);

CREATE TABLE public."stores" (
    "store_id" SMALLINT NOT NULL,
    "name" VARCHAR(120) NOT NULL,
    "latitude" NUMERIC(9,6),
    "longitude" NUMERIC(9,6),
    "description" TEXT,
    CHECK ((latitude IS NULL) = (longitude IS NULL)),
    CHECK (latitude BETWEEN -90 AND 90),
    CHECK (longitude BETWEEN -180 AND 180)
);

CREATE TABLE public."products" (
    "product_id" SMALLINT NOT NULL,
    "product_type_id" SMALLINT NOT NULL,
    "unit_id" SMALLINT NOT NULL,
    "name" VARCHAR(200) NOT NULL,
    "description" TEXT,
    "current_price" NUMERIC(10,2) NOT NULL,
    CHECK (current_price BETWEEN 0 AND 99999999.99)
);

CREATE TABLE public."receipts" (
    "receipt_id" INTEGER NOT NULL,
    "store_id" SMALLINT NOT NULL,
    "sold_at" TIMESTAMPTZ NOT NULL,
    "total_amount" NUMERIC(14,2) NOT NULL,
    CHECK (total_amount BETWEEN 0 AND 999999999999.99)
);

CREATE TABLE public."receipt_items" (
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

ALTER TABLE public."units" ADD PRIMARY KEY (unit_id);

ALTER TABLE public."units" ADD UNIQUE (code);

ALTER TABLE public."categories" ADD PRIMARY KEY (category_id);

ALTER TABLE public."categories" ADD UNIQUE (name);

ALTER TABLE public."product_types" ADD PRIMARY KEY (product_type_id);

ALTER TABLE public."product_types" ADD UNIQUE (category_id, name);

ALTER TABLE public."stores" ADD PRIMARY KEY (store_id);

ALTER TABLE public."products" ADD PRIMARY KEY (product_id);

ALTER TABLE public."receipts" ADD PRIMARY KEY (receipt_id);

ALTER TABLE public."receipt_items" ADD PRIMARY KEY (receipt_id, line_no) INCLUDE (product_id);

ALTER TABLE public."product_types" ADD FOREIGN KEY (category_id) REFERENCES public."categories" (category_id);

ALTER TABLE public."products" ADD FOREIGN KEY (product_type_id) REFERENCES public."product_types" (product_type_id);

ALTER TABLE public."products" ADD FOREIGN KEY (unit_id) REFERENCES public."units" (unit_id);

ALTER TABLE public."receipts" ADD FOREIGN KEY (store_id) REFERENCES public."stores" (store_id);

ALTER TABLE public."receipt_items" ADD FOREIGN KEY (receipt_id) REFERENCES public."receipts" (receipt_id);

ALTER TABLE public."receipt_items" ADD FOREIGN KEY (product_id) REFERENCES public."products" (product_id);

CREATE INDEX idx_receipts_time_cover ON public.receipts (sold_at) INCLUDE (receipt_id, store_id, total_amount);

CREATE INDEX idx_receipt_items_product_receipt_cover ON public.receipt_items (product_id, receipt_id) INCLUDE (quantity);
