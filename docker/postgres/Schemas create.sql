-- create Schemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS cleaned;
CREATE SCHEMA IF NOT EXISTS dim;

-- RAW: raw_categories
CREATE TABLE IF NOT EXISTS raw.raw_categories (
    categories_id TEXT PRIMARY KEY,
    extract_time TIMESTAMP,
    raw_response JSONB
);

-- RAW: raw_product_listings
CREATE TABLE IF NOT EXISTS raw.raw_product_listings (
    category_id TEXT,
    extract_time TIMESTAMP,
    product_id TEXT,
    raw_response JSONB,
    PRIMARY KEY (product_id, extract_time)
);

-- RAW: raw_product_details
CREATE TABLE IF NOT EXISTS raw.raw_product_details (
    product_id TEXT PRIMARY KEY,
    extract_time TIMESTAMP,
    raw_response JSONB
);

-- RAW: raw_reviews
CREATE TABLE IF NOT EXISTS raw.raw_reviews (
    product_id TEXT,
    review_id TEXT,
    extract_time TIMESTAMP,
    raw_response JSONB,
    PRIMARY KEY (review_id, extract_time)
);

-- RAW: raw_sellers
CREATE TABLE IF NOT EXISTS raw.raw_sellers (
    seller_id TEXT PRIMARY KEY, -- ID của seller
    extract_time TIMESTAMP,
    raw_response JSONB
);



-- 1. Bảng Categories
CREATE TABLE IF NOT EXISTS cleaned.categories (
    category_id BIGINT PRIMARY KEY,
    parent_id BIGINT,
    name VARCHAR(255),
    level INTEGER,
    is_leaf BOOLEAN,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Bảng Sellers
CREATE TABLE IF NOT EXISTS cleaned.sellers (
    seller_id BIGINT PRIMARY KEY,
    store_id BIGINT,
    name VARCHAR(255),
    url TEXT,
    logo TEXT,
    avg_rating_point NUMERIC(3, 2),
    review_count INTEGER,
    total_follower INTEGER,
    is_official BOOLEAN,
    days_since_joined INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. Bảng Products (Sử dụng spid làm PK để phục vụ Market Insight)
CREATE TABLE IF NOT EXISTS cleaned.products (
    spid BIGINT PRIMARY KEY, -- Seller Product ID
    product_id BIGINT NOT NULL, -- Master Product ID
    seller_id BIGINT NOT NULL, -- Liên kết với bảng sellers
    name VARCHAR(500),
    sku BIGINT,
    url_key VARCHAR(500),
    url_path TEXT,
    short_description TEXT,
    price NUMERIC(15, 2),
    list_price NUMERIC(15, 2),
    original_price NUMERIC(15, 2),
    discount NUMERIC(15, 2),
    discount_rate NUMERIC(5, 2),
    rating_average NUMERIC(3, 2),
    review_count INTEGER,
    all_time_quantity_sold INTEGER,
    inventory_status VARCHAR(50),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_seller FOREIGN KEY (seller_id) REFERENCES cleaned.sellers(seller_id)
);

-- 4. Bảng trung gian Product - Categories
CREATE TABLE IF NOT EXISTS cleaned.product_categories (
    spid BIGINT NOT NULL,
    category_id BIGINT NOT NULL,
    PRIMARY KEY (spid, category_id),
    CONSTRAINT fk_product FOREIGN KEY (spid) REFERENCES cleaned.products(spid),
    CONSTRAINT fk_category FOREIGN KEY (category_id) REFERENCES cleaned.categories(category_id)
);

-- 5. Bảng Reviews
CREATE TABLE IF NOT EXISTS cleaned.reviews (
    review_id BIGINT PRIMARY KEY,
    spid BIGINT NOT NULL, -- Liên kết trực tiếp với sản phẩm của nhà bán
    customer_id BIGINT,
    title TEXT,
    content TEXT,
    rating INTEGER,
    thank_count INTEGER,
    created_at TIMESTAMP,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_product_review FOREIGN KEY (spid) REFERENCES cleaned.products(spid)
);

-- 6. Bảng Review Images
CREATE TABLE IF NOT EXISTS cleaned.review_images (
    image_id BIGINT PRIMARY KEY,
    review_id BIGINT NOT NULL,
    full_path TEXT,
    CONSTRAINT fk_review_image FOREIGN KEY (review_id) REFERENCES cleaned.reviews(review_id)
);



-- DIM: dim_categories
CREATE TABLE IF NOT EXISTS dim.dim_categories (
    category_key SERIAL PRIMARY KEY, -- Surrogate key
    category_id TEXT UNIQUE NOT NULL,
    category_name VARCHAR(255),
    parent_category_id TEXT,
    category_level INTEGER,
    -- ... các thuộc tính khác từ categories
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM: dim_products
CREATE TABLE IF NOT EXISTS dim.dim_products (
    product_key SERIAL PRIMARY KEY, -- Surrogate key
    product_id TEXT UNIQUE NOT NULL,
    product_name VARCHAR(500),
    brand_name VARCHAR(255),
    author_name VARCHAR(255), -- Nếu là sách
    -- ... các thuộc tính khác từ products
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM: dim_sellers
CREATE TABLE IF NOT EXISTS dim.dim_sellers (
    seller_key SERIAL PRIMARY KEY, -- Surrogate key
    seller_id TEXT UNIQUE NOT NULL,
    seller_name VARCHAR(255),
    is_official BOOLEAN,
    -- ... các thuộc tính khác từ sellers
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- DIM: dim_customers (từ reviews)
CREATE TABLE IF NOT EXISTS dim.dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id TEXT UNIQUE NOT NULL,
    customer_name VARCHAR(255),
    region VARCHAR(255),
    joined_time TIMESTAMP,
    total_review INTEGER,
    total_thank INTEGER,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
