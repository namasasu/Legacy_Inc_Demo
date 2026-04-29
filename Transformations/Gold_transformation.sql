-- ============================================================================
-- GOLD LAYER: BUSINESS AGGREGATIONS AND ANALYTICS
-- ============================================================================
-- Business-ready aggregated tables for reporting and analytics
-- Source: LEGACY_INC.silver dimension and fact tables
-- All aggregations use materialized views for optimal query performance
-- ============================================================================

-- ============================================================================
-- daily_sales_summary: Daily sales metrics
-- Purpose: Time-series aggregation of sales performance by day
-- Grain: One row per order_date
-- Measures: Orders, revenue, units, customers, avg metrics
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.gold.daily_sales_summary
COMMENT 'Daily sales performance metrics'
CLUSTER BY (order_date)
AS
SELECT 
  DATE_FORMAT(TO_DATE(CAST(order_date AS STRING), 'yyyyMMdd'), 'dd/MMM/yyyy') AS order_date,
  COUNT(DISTINCT order_number) AS total_orders,
  COUNT(*) AS total_line_items,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(sales_amount) AS total_revenue,
  SUM(quantity) AS total_units_sold,
  ROUND(AVG(sales_amount), 2) AS avg_line_amount,
  ROUND(AVG(unit_price), 2) AS avg_unit_price,
  SUM(CASE WHEN is_shipped THEN 1 ELSE 0 END) AS shipped_orders,
  SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) AS on_time_orders,
  ROUND(SUM(CASE WHEN is_on_time THEN 1 ELSE 0 END) * 1.0 / COUNT(*), 2) AS on_time_percentage
FROM LEGACY_INC.silver.fact_sales
GROUP BY order_date;

-- ============================================================================
-- customer_lifetime_value: Customer value metrics
-- Purpose: Aggregate customer purchase behavior and lifetime value
-- Grain: One row per customer_id
-- Measures: Total orders, revenue, avg order value, first/last purchase
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.gold.customer_lifetime_value
COMMENT 'Customer lifetime value and purchase behavior metrics'
CLUSTER BY (customer_id, customer_segment)
AS
SELECT 
  c.customer_id,
  c.first_name,
  c.last_name,
  c.gender,
  c.country,
  c.age,
  COUNT(DISTINCT f.order_number) AS total_orders,
  COUNT(*) AS total_line_items,
  SUM(f.sales_amount) AS lifetime_value,
  SUM(f.quantity) AS total_units_purchased,
  ROUND(AVG(f.sales_amount), 2) AS avg_order_value,
  ROUND(AVG(f.unit_price), 2) AS avg_unit_price,
  DATE_FORMAT(TO_DATE(CAST(MIN(f.order_date) AS STRING), 'yyyyMMdd'), 'dd/MMM/yyyy') AS first_purchase_date,
  DATE_FORMAT(TO_DATE(CAST(MAX(f.order_date) AS STRING), 'yyyyMMdd'), 'dd/MMM/yyyy') AS last_purchase_date,
  CASE 
    WHEN SUM(f.sales_amount) >= 10000 THEN 'High Value'
    WHEN SUM(f.sales_amount) >= 5000 THEN 'Medium Value'
    ELSE 'Low Value'
  END AS customer_segment
FROM LEGACY_INC.silver.dim_customers c
INNER JOIN LEGACY_INC.silver.fact_sales f ON c.customer_id = f.customer_id
GROUP BY ALL;

-- ============================================================================
-- product_performance: Product sales performance
-- Purpose: Aggregate product-level sales metrics
-- Grain: One row per product_key (note: links to fact_sales only, not dim_products due to key mismatch)
-- Measures: Orders, revenue, units sold, customers
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.gold.product_performance
COMMENT 'Product-level sales performance metrics'
CLUSTER BY (product_key, total_revenue)
AS
SELECT 
  product_key,
  COUNT(DISTINCT order_number) AS total_orders,
  COUNT(*) AS total_line_items,
  COUNT(DISTINCT customer_id) AS unique_customers,
  SUM(sales_amount) AS total_revenue,
  SUM(quantity) AS total_units_sold,
  ROUND(AVG(sales_amount), 2) AS avg_line_amount,
  ROUND(AVG(unit_price), 2) AS avg_unit_price,
  DATE_FORMAT(TO_DATE(CAST(MIN(order_date) AS STRING), 'yyyyMMdd'), 'dd/MMM/yyyy') AS first_sale_date,
  DATE_FORMAT(TO_DATE(CAST(MAX(order_date) AS STRING), 'yyyyMMdd'), 'dd/MMM/yyyy') AS last_sale_date,
  CASE 
    WHEN SUM(sales_amount) >= 50000 THEN 'Top Seller'
    WHEN SUM(sales_amount) >= 20000 THEN 'Good Seller'
    ELSE 'Low Seller'
  END AS product_tier
FROM LEGACY_INC.silver.fact_sales
GROUP BY product_key;

-- ============================================================================
-- monthly_sales_summary: Monthly sales trends
-- Purpose: Time-series aggregation by month for trend analysis
-- Grain: One row per year-month
-- Measures: Orders, revenue, customers, growth metrics
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.gold.monthly_sales_summary
COMMENT 'Monthly sales performance and trend metrics'
CLUSTER BY (year, month)
AS
WITH monthly_base AS (
  SELECT 
    CAST(SUBSTRING(CAST(order_date AS STRING), 1, 4) AS INT) AS year,
    CAST(SUBSTRING(CAST(order_date AS STRING), 5, 2) AS INT) AS month,
    COUNT(DISTINCT order_number) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(sales_amount) AS total_revenue,
    SUM(quantity) AS total_units_sold
  FROM LEGACY_INC.silver.fact_sales
  GROUP BY ALL
)
SELECT 
  year,
  month,
  total_orders,
  unique_customers,
  total_revenue,
  total_units_sold,
  ROUND(total_revenue / NULLIF(total_orders, 0), 2) AS avg_order_value,
  ROUND(total_revenue / NULLIF(unique_customers, 0), 2) AS revenue_per_customer
FROM monthly_base
ORDER BY year, month;

-- ============================================================================
-- customer_segments: Customer segmentation analysis
-- Purpose: Group customers by demographics and purchase behavior
-- Grain: One row per segment combination
-- Measures: Customer count, revenue, avg metrics per segment
-- ============================================================================
CREATE OR REFRESH MATERIALIZED VIEW LEGACY_INC.gold.customer_segments
COMMENT 'Customer segmentation by demographics and purchase behavior'
CLUSTER BY (gender, customer_tier)
AS
WITH customer_tiers AS (
  SELECT 
    c.customer_id,
    COALESCE(c.gender, 'Unknown') AS gender,
    COALESCE(c.country, 'Unknown') AS country,
    SUM(f.sales_amount) AS lifetime_value,
    COUNT(DISTINCT f.order_number) AS total_orders,
    AVG(f.sales_amount) AS avg_transaction_value,
    CASE 
      WHEN SUM(f.sales_amount) >= 2000 THEN 'High Value'
      WHEN SUM(f.sales_amount) >= 1000 THEN 'Medium Value'
      ELSE 'Low Value'
    END AS customer_tier
  FROM LEGACY_INC.silver.dim_customers c
  INNER JOIN LEGACY_INC.silver.fact_sales f ON c.customer_id = f.customer_id
  GROUP BY c.customer_id, c.gender, c.country
)
SELECT 
  gender,
  country,
  customer_tier,
  COUNT(DISTINCT customer_id) AS customer_count,
  SUM(lifetime_value) AS total_revenue,
  ROUND(AVG(avg_transaction_value), 2) AS avg_transaction_value,
  SUM(total_orders) AS total_orders,
  ROUND(SUM(total_orders) * 0.1 / COUNT(DISTINCT customer_id), 2) AS avg_orders_per_customer
FROM customer_tiers
GROUP BY gender, country, customer_tier
ORDER BY total_revenue DESC;
