-- --------------------------------------
-- 1. Fact Table: Inventory Reconciliation
-- --------------------------------------
CREATE TABLE fact_inventory_reconciliation (
    date_key DATE,
    sku VARCHAR(50),
    product_name VARCHAR(255),
    opening_stock INT,
    quantity_sold BIGINT,           -- Changed from INT
    expected_closing_stock BIGINT,  -- Changed from INT
    actual_closing_stock INT,
    discrepancy_amount BIGINT        -- Changed from INT
)
DISTKEY(sku)
SORTKEY(date_key);

-- --------------------------------------
-- 2. Dimension Table: Products
-- --------------------------------------
CREATE TABLE IF NOT EXISTS dim_products (
    sku VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100)
)
DISTSTYLE ALL                     -- Small table, broadcast to all nodes for joins
SORTKEY(sku);

-- --------------------------------------
-- 3. Fact Table: Daily Sales
-- --------------------------------------
CREATE TABLE IF NOT EXISTS fact_daily_sales (
    date_key DATE,
    sku VARCHAR(50),
    product_name VARCHAR(255),
    total_quantity_sold BIGINT       -- Using BIGINT to match the Spark sum() output
)
DISTKEY(sku)
SORTKEY(date_key);
