-- Redshift star schema for the sales analytics warehouse.
-- Loaded from s3://<bucket>/processed/sales/ (Parquet) by Glue or COPY.

CREATE SCHEMA IF NOT EXISTS sales;

-- ---------- Dimensions ----------

CREATE TABLE IF NOT EXISTS sales.dim_date (
    date_key        INTEGER      NOT NULL PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE         NOT NULL,
    day_of_week     SMALLINT     NOT NULL,
    day_name        VARCHAR(10)  NOT NULL,
    day_of_month    SMALLINT     NOT NULL,
    week_of_year    SMALLINT     NOT NULL,
    month           SMALLINT     NOT NULL,
    month_name      VARCHAR(10)  NOT NULL,
    quarter         SMALLINT     NOT NULL,
    year            SMALLINT     NOT NULL,
    is_weekend      BOOLEAN      NOT NULL
)
DISTSTYLE ALL
SORTKEY (full_date);

CREATE TABLE IF NOT EXISTS sales.dim_product (
    product_key     INTEGER IDENTITY(1, 1) PRIMARY KEY,
    product_name    VARCHAR(100) NOT NULL UNIQUE,
    category        VARCHAR(50),
    list_price      DECIMAL(12, 2)
)
DISTSTYLE ALL;

CREATE TABLE IF NOT EXISTS sales.dim_city (
    city_key        INTEGER IDENTITY(1, 1) PRIMARY KEY,
    city_name       VARCHAR(100) NOT NULL UNIQUE,
    state           VARCHAR(100),
    region          VARCHAR(50)
)
DISTSTYLE ALL;

-- ---------- Fact ----------

CREATE TABLE IF NOT EXISTS sales.fact_sales (
    order_id        VARCHAR(20)    NOT NULL PRIMARY KEY,
    customer_id     VARCHAR(20),
    date_key        INTEGER        NOT NULL REFERENCES sales.dim_date (date_key),
    product_key     INTEGER        NOT NULL REFERENCES sales.dim_product (product_key),
    city_key        INTEGER        NOT NULL REFERENCES sales.dim_city (city_key),
    event_timestamp TIMESTAMP,
    quantity        INTEGER        NOT NULL,
    unit_price      DECIMAL(12, 2) NOT NULL,
    revenue         DECIMAL(14, 2) NOT NULL,
    payment_method  VARCHAR(20),
    order_status    VARCHAR(20),
    source          VARCHAR(10)    NOT NULL DEFAULT 'batch',  -- 'batch' or 'stream'
    ingestion_date  DATE           NOT NULL,
    loaded_at       TIMESTAMP      NOT NULL DEFAULT GETDATE()
)
DISTKEY (order_id)
SORTKEY (date_key);

-- Staging table for idempotent loads (MERGE into fact_sales on order_id).
CREATE TABLE IF NOT EXISTS sales.stg_sales (LIKE sales.fact_sales);
