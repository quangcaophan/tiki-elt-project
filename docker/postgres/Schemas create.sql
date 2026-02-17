-- ============================================================
-- TIKI ETL PIPELINE - DATABASE SCHEMA
-- ============================================================
-- Schemas: raw (landing) → cleaned (dbt transformed) → dim (future)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS cleaned;
CREATE SCHEMA IF NOT EXISTS dim;


-- ============================================================
-- RAW LAYER  (landing zone, lưu JSON gốc từ API)
-- ============================================================

-- raw.raw_categories
CREATE TABLE IF NOT EXISTS raw.raw_categories (
    categories_id   TEXT        NOT NULL,
    extract_time    TIMESTAMP   NOT NULL,
    raw_response    JSONB,
    PRIMARY KEY (categories_id)
);

-- raw.raw_product_listings
CREATE TABLE IF NOT EXISTS raw.raw_product_listings (
    category_id     TEXT        NOT NULL,
    page            INTEGER     NOT NULL,
    extract_time    TIMESTAMP   NOT NULL,
    raw_response    JSONB,
    PRIMARY KEY (category_id, page)
);

-- raw.raw_sellers
CREATE TABLE IF NOT EXISTS raw.raw_sellers (
    seller_id       TEXT        NOT NULL,
    extract_time    TIMESTAMP   NOT NULL,
    raw_response    JSONB,
    PRIMARY KEY (seller_id)
);

-- raw.raw_reviews
CREATE TABLE IF NOT EXISTS raw.raw_reviews (
    spid            BIGINT      NOT NULL,
    product_id      BIGINT      NOT NULL,
    seller_id       INTEGER     NOT NULL,
    page            INTEGER     NOT NULL,
    extract_time    TIMESTAMP   NOT NULL,
    raw_response    JSONB,
    PRIMARY KEY (spid, page)
);


-- ============================================================
-- CLEANED LAYER  (output của dbt, đã transform từ JSON)
-- ============================================================

-- cleaned.categories
CREATE TABLE IF NOT EXISTS cleaned.categories (
    category_id         INTEGER         NOT NULL,
    parent_id           INTEGER,
    name                VARCHAR(255),
    url_key             VARCHAR(500),
    url_path            TEXT,
    level               INTEGER,
    status              VARCHAR(50),
    is_leaf             BOOLEAN,
    product_count       INTEGER,
    thumbnail_url       TEXT,
    meta_title          TEXT,
    meta_description    TEXT,
    PRIMARY KEY (category_id),
    CONSTRAINT fk_category_parent
        FOREIGN KEY (parent_id) REFERENCES cleaned.categories (category_id)
        ON DELETE SET NULL
);

-- cleaned.sellers
CREATE TABLE IF NOT EXISTS cleaned.sellers (
    seller_id           INTEGER         NOT NULL,
    store_id            INTEGER,
    name                VARCHAR(255),
    url                 TEXT,
    logo                TEXT,
    avg_rating_point    NUMERIC(3, 2),
    review_count        INTEGER,
    total_follower      INTEGER,
    is_official         BOOLEAN,
    days_since_joined   INTEGER,
    extract_time        TIMESTAMP,
    PRIMARY KEY (seller_id)
);

-- cleaned.products
CREATE TABLE IF NOT EXISTS cleaned.products (
    spid                    BIGINT          NOT NULL,
    product_id              BIGINT          NOT NULL,
    seller_id               INTEGER         NOT NULL,
    name                    VARCHAR(500),
    sku                     BIGINT,
    url_key                 VARCHAR(500),
    url_path                TEXT,
    short_description       TEXT,
    price                   NUMERIC(15, 2),
    list_price              NUMERIC(15, 2),
    original_price          NUMERIC(15, 2),
    discount                NUMERIC(15, 2),
    discount_rate           NUMERIC(5, 2),
    rating_average          NUMERIC(3, 2),
    review_count            INTEGER,
    order_count             INTEGER,
    favourite_count         INTEGER,
    thumbnail_url           TEXT,
    has_ebook               BOOLEAN,
    inventory_status        VARCHAR(50),
    productset_group_name   VARCHAR(255),
    all_time_quantity_sold  INTEGER,
    meta_title              TEXT,
    meta_description        TEXT,
    last_updated            TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (spid),
    CONSTRAINT fk_product_seller
        FOREIGN KEY (seller_id) REFERENCES cleaned.sellers (seller_id)
        ON DELETE RESTRICT
);

-- cleaned.reviews
CREATE TABLE IF NOT EXISTS cleaned.reviews (
    review_id       BIGINT          NOT NULL,
    product_id      BIGINT          NOT NULL,
    spid            BIGINT          NOT NULL,
    seller_id       INTEGER         NOT NULL,
    user_id         BIGINT,
    user_name       VARCHAR(255),
    rating          INTEGER,
    title           TEXT,
    content         TEXT,
    purchased_at    TIMESTAMP,
    variant         TEXT,
    thank_count     INTEGER,
    PRIMARY KEY (review_id),
    CONSTRAINT fk_review_product
        FOREIGN KEY (spid) REFERENCES cleaned.products (spid)
        ON DELETE RESTRICT,
    CONSTRAINT fk_review_seller
        FOREIGN KEY (seller_id) REFERENCES cleaned.sellers (seller_id)
        ON DELETE RESTRICT
);

-- cleaned.product_categories  (bảng junction nhiều-nhiều)
CREATE TABLE IF NOT EXISTS cleaned.product_categories (
    spid            BIGINT      NOT NULL,
    category_id     INTEGER     NOT NULL,
    PRIMARY KEY (spid, category_id),
    CONSTRAINT fk_pc_product
        FOREIGN KEY (spid) REFERENCES cleaned.products (spid)
        ON DELETE CASCADE,
    CONSTRAINT fk_pc_category
        FOREIGN KEY (category_id) REFERENCES cleaned.categories (category_id)
        ON DELETE CASCADE
);


-- ============================================================
-- DIM LAYER  (future — star schema cho analytics)
-- ============================================================

CREATE TABLE IF NOT EXISTS dim.dim_categories (
    category_key        SERIAL          PRIMARY KEY,
    category_id         INTEGER         UNIQUE NOT NULL,
    category_name       VARCHAR(255),
    parent_category_id  INTEGER,
    category_level      INTEGER,
    url_path            TEXT,
    is_leaf             BOOLEAN,
    last_updated        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_sellers (
    seller_key          SERIAL          PRIMARY KEY,
    seller_id           INTEGER         UNIQUE NOT NULL,
    seller_name         VARCHAR(255),
    is_official         BOOLEAN,
    avg_rating_point    NUMERIC(3, 2),
    total_follower      INTEGER,
    days_since_joined   INTEGER,
    last_updated        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_products (
    product_key         SERIAL          PRIMARY KEY,
    product_id          BIGINT          UNIQUE NOT NULL,    -- master product
    product_name        VARCHAR(500),
    url_key             VARCHAR(500),
    has_ebook           BOOLEAN,
    last_updated        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim.dim_customers (
    customer_key        SERIAL          PRIMARY KEY,
    user_id             BIGINT          UNIQUE NOT NULL,
    user_name           VARCHAR(255),
    last_updated        TIMESTAMP       DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- INDEXES  (tăng tốc query thường dùng)
-- ============================================================

-- categories: lookup theo parent (cho recursive query)
CREATE INDEX IF NOT EXISTS idx_categories_parent_id
    ON cleaned.categories (parent_id);

-- products: lookup theo product_id (master) và seller_id
CREATE INDEX IF NOT EXISTS idx_products_product_id
    ON cleaned.products (product_id);
CREATE INDEX IF NOT EXISTS idx_products_seller_id
    ON cleaned.products (seller_id);

-- reviews: lookup theo product và seller
CREATE INDEX IF NOT EXISTS idx_reviews_product_id
    ON cleaned.reviews (product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_spid
    ON cleaned.reviews (spid);
CREATE INDEX IF NOT EXISTS idx_reviews_seller_id
    ON cleaned.reviews (seller_id);