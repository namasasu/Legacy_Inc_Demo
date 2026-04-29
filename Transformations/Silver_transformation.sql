-- ============================================================================
-- SILVER LAYER: NORMALIZED DIMENSIONAL MODEL (STAR SCHEMA)
-- ============================================================================
-- Transforms raw bronze data into a star schema with 3 dimension tables and 
-- 1 fact table. All tables include data quality constraints.
-- Source: LEGACY_INC.bronze.sales_catalogue
-- ============================================================================

-- ============================================================================
-- dim_product_categories: Product taxonomy lookup (37 rows)
-- Purpose: Category/subcategory hierarchy with maintenance flag
-- Key: category_id
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.silver.dim_product_categories
(
  CONSTRAINT valid_category_id EXPECT (category_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_category EXPECT (category_name IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_subcategory EXPECT (subcategory_name IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT 'Product category dimension - normalized product taxonomy'
CLUSTER BY (category_id, category_name)
AS
SELECT DISTINCT
  TRIM(ID) AS category_id,
  TRIM(CAT) AS category_name,
  TRIM(SUBCAT) AS subcategory_name,
  CASE 
    WHEN UPPER(TRIM(MAINTENANCE)) = 'YES' THEN true
    WHEN UPPER(TRIM(MAINTENANCE)) = 'NO' THEN false
    ELSE NULL
  END AS requires_maintenance,
  source_file,
  ingestion_timestamp
FROM LEGACY_INC.bronze.sales_catalogue
WHERE ID IS NOT NULL;

-- ============================================================================
-- dim_products: Master product catalog (397 rows)
-- Purpose: Product details with lifecycle dates
-- Key: product_id
-- Relationships: Can be linked to categories via product_key prefix
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.silver.dim_products
(
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_product_key EXPECT (product_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_name EXPECT (
    product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_start_date EXPECT (start_date IS NOT NULL)
)
COMMENT 'Product dimension - deduplicated product catalog with category reference'
CLUSTER BY (product_id, product_line)
AS
SELECT DISTINCT
  prd_id AS product_id,
  prd_key AS product_key,
  TRIM(prd_nm) AS product_name,
  prd_cost AS product_cost,
  TRIM(prd_line) AS product_line,
  prd_start_dt AS start_date,
  prd_end_dt AS end_date,
  CASE 
    WHEN prd_end_dt IS NULL THEN true 
    ELSE false 
  END AS is_active,
  source_file,
  ingestion_timestamp
FROM LEGACY_INC.bronze.sales_catalogue
WHERE prd_id IS NOT NULL;

-- ============================================================================
-- dim_customers: Master customer list with demographics (18,485 rows)
-- Purpose: Customer account and demographic data merged
-- Key: customer_id
-- Note: Merges cst_* columns with CID, BDATE, CNTRY demographics
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.silver.dim_customers
(
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_customer_key EXPECT (customer_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_name EXPECT (
    first_name IS NOT NULL AND last_name IS NOT NULL
  ) ON VIOLATION DROP ROW,
  CONSTRAINT valid_gender EXPECT (gender IN ('Male', 'Female', 'Refuse to identify', NULL))
)
COMMENT 'Customer dimension - deduplicated customers with demographics'
CLUSTER BY (customer_id, country)
AS
SELECT DISTINCT
  cst_id AS customer_id,
  cst_key AS customer_key,
  COALESCE(TRIM(CID), cst_key) AS external_customer_id,
  TRIM(cst_firstname) AS first_name,
  TRIM(cst_lastname) AS last_name,
  TRIM(cst_marital_status) AS marital_status,
  CASE 
    WHEN TRIM(cst_gndr) = 'M' THEN 'Male'
    WHEN TRIM(cst_gndr) = 'F' THEN 'Female'
    WHEN TRIM(cst_gndr) = 'Unknown' THEN 'Refuse to identify'
    ELSE NULL
  END AS gender,
  BDATE AS birth_date,
  CASE 
    WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
    WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
    WHEN TRIM(CNTRY) IS NULL OR LENGTH(TRIM(CNTRY)) = 0 THEN NULL
    ELSE TRIM(CNTRY)
  END AS country,
  cst_create_date AS create_date,
  CASE 
    WHEN BDATE IS NOT NULL THEN YEAR(CURRENT_DATE()) - YEAR(BDATE)
    ELSE NULL
  END AS age,
  source_file,
  ingestion_timestamp
FROM LEGACY_INC.bronze.sales_catalogue
WHERE cst_id IS NOT NULL;

-- ============================================================================
-- fact_sales: Sales transactions fact table (60,375 rows)
-- Purpose: Central fact table with sales transactions
-- Grain: One row per order line item (order_number + product_key)
-- Foreign Keys: customer_id -> dim_customers, product_key -> dim_products
-- Measures: sales_amount, quantity, unit_price, calculated_unit_price, total_line_amount
-- Flags: is_shipped, is_on_time
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.silver.fact_sales
(
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION FAIL UPDATE,
  CONSTRAINT valid_product_key EXPECT (product_key IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_sales EXPECT (sales_amount >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT non_negative_price EXPECT (unit_price >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT reasonable_dates EXPECT (
    order_date <= due_date OR due_date IS NULL
  )
)
COMMENT 'Sales fact table - normalized transaction data with foreign keys to dimensions'
CLUSTER BY (order_date, customer_id)
AS
SELECT 
  TRIM(sls_ord_num) AS order_number,
  TRIM(sls_prd_key) AS product_key,
  sls_cust_id AS customer_id,
  sls_order_dt AS order_date,
  sls_ship_dt AS ship_date,
  sls_due_dt AS due_date,
  sls_sales AS sales_amount,
  sls_quantity AS quantity,
  sls_price AS unit_price,
  sls_sales / NULLIF(sls_quantity, 0) AS calculated_unit_price,
  sls_sales * sls_quantity AS total_line_amount,
  CASE 
    WHEN sls_ship_dt IS NOT NULL THEN true 
    ELSE false 
  END AS is_shipped,
  CASE
    WHEN sls_ship_dt <= sls_due_dt OR sls_due_dt IS NULL THEN true
    WHEN sls_ship_dt IS NULL THEN NULL
    ELSE false
  END AS is_on_time,
  source_file,
  ingestion_timestamp
FROM LEGACY_INC.bronze.sales_catalogue
WHERE sls_ord_num IS NOT NULL;
