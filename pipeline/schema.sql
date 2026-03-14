-- ============================================================
--  BRAZIL E-COMMERCE (OLIST) -- DATABASE SCHEMA
--  Database : olist_db
-- ============================================================

-- DROP ALL TABLES (reverse FK order)
DROP TABLE IF EXISTS order_reviews   CASCADE;
DROP TABLE IF EXISTS order_payments  CASCADE;
DROP TABLE IF EXISTS order_items     CASCADE;
DROP TABLE IF EXISTS orders          CASCADE;
DROP TABLE IF EXISTS products        CASCADE;
DROP TABLE IF EXISTS sellers         CASCADE;
DROP TABLE IF EXISTS customers       CASCADE;
DROP TABLE IF EXISTS geolocation     CASCADE;

-- 1. CUSTOMERS
CREATE TABLE customers (
    customer_id              VARCHAR(50)  PRIMARY KEY,
    customer_unique_id       VARCHAR(50)  NOT NULL,
    customer_zip_code_prefix INT,
    customer_city            VARCHAR(100),
    customer_state           CHAR(2)
);

-- 2. SELLERS
CREATE TABLE sellers (
    seller_id                VARCHAR(50)  PRIMARY KEY,
    seller_zip_code_prefix   INT,
    seller_city              VARCHAR(100),
    seller_state             CHAR(2)
);

-- 3. PRODUCTS
CREATE TABLE products (
    product_id                      VARCHAR(50)  PRIMARY KEY,
    product_category_name           TEXT,
    product_name_lenght             FLOAT,
    product_description_lenght      FLOAT,
    product_photos_qty              FLOAT,
    product_weight_g                FLOAT,
    product_length_cm               FLOAT,
    product_height_cm               FLOAT,
    product_width_cm                FLOAT,
    product_category_name_english   TEXT
);

-- 4. ORDERS
CREATE TABLE orders (
    order_id                        VARCHAR(50)  PRIMARY KEY,
    customer_id                     VARCHAR(50)  REFERENCES customers(customer_id),
    order_status                    VARCHAR(20),
    order_purchase_timestamp        TIMESTAMP,
    order_approved_at               TIMESTAMP,
    order_delivered_carrier_date    TIMESTAMP,
    order_delivered_customer_date   TIMESTAMP,
    order_estimated_delivery_date   TIMESTAMP
);

-- 5. ORDER ITEMS
CREATE TABLE order_items (
    order_id             VARCHAR(50)  REFERENCES orders(order_id),
    order_item_id        INT,
    product_id           VARCHAR(50)  REFERENCES products(product_id),
    seller_id            VARCHAR(50)  REFERENCES sellers(seller_id),
    shipping_limit_date  TIMESTAMP,
    price                DECIMAL(10,2),
    freight_value        DECIMAL(10,2),
    price_outlier_flag   SMALLINT     DEFAULT 0,
    PRIMARY KEY (order_id, order_item_id)
);

-- 6. ORDER PAYMENTS
CREATE TABLE order_payments (
    order_id              VARCHAR(50)  REFERENCES orders(order_id),
    payment_sequential    INT,
    payment_type          VARCHAR(30),
    payment_installments  INT,
    payment_value         DECIMAL(10,2),
    PRIMARY KEY (order_id, payment_sequential)
);

-- 7. ORDER REVIEWS
CREATE TABLE order_reviews (
    review_id               VARCHAR(50),
    order_id                VARCHAR(50)  REFERENCES orders(order_id),
    review_score            SMALLINT     CHECK (review_score BETWEEN 1 AND 5),
    review_comment_title    TEXT,
    review_comment_message  TEXT,
    review_creation_date    TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

-- 8. GEOLOCATION
CREATE TABLE geolocation (
    geolocation_zip_code_prefix  INT,
    geolocation_lat              FLOAT,
    geolocation_lng              FLOAT,
    geolocation_city             VARCHAR(100),
    geolocation_state            CHAR(2)
);

-- INDEXES
CREATE INDEX idx_orders_customer ON orders(customer_id);
CREATE INDEX idx_orders_status   ON orders(order_status);
CREATE INDEX idx_orders_purchase ON orders(order_purchase_timestamp);
CREATE INDEX idx_items_order     ON order_items(order_id);
CREATE INDEX idx_items_product   ON order_items(product_id);
CREATE INDEX idx_items_seller    ON order_items(seller_id);
CREATE INDEX idx_payments_order  ON order_payments(order_id);
CREATE INDEX idx_reviews_order   ON order_reviews(order_id);
CREATE INDEX idx_geo_zip         ON geolocation(geolocation_zip_code_prefix);
