/*
=============================================================================
DATA QUALITY CHECKS – GOLD LAYER
=============================================================================
Purpose:
    Validate data quality for Gold Layer (Star Schema)

Checks Included:
    1. Row Count Validation
    2. Duplicate Detection
    3. NULL Key Validation
    4. Referential Integrity (Fact → Dimension)
    5. Business Rule Validation
    6. Negative / Invalid Values
    7. Date Validation
    8. Data Standardization
    9. Coverage Check
=============================================================================
*/


-- ============================================================
-- 1. ROW COUNT VALIDATION
-- ============================================================
SELECT 'dim_customers' AS table_name, COUNT(*) FROM gold.dim_customers
UNION ALL
SELECT 'dim_products', COUNT(*) FROM gold.dim_products
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM gold.fact_sales;


-- ============================================================
-- 2. DUPLICATE CHECK
-- ============================================================
-- Customers
SELECT customer_id, COUNT(*)
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Products
SELECT product_number, COUNT(*)
FROM gold.dim_products
GROUP BY product_number
HAVING COUNT(*) > 1;


-- ============================================================
-- 3. NULL KEY CHECK
-- ============================================================
SELECT * FROM gold.dim_customers WHERE customer_key IS NULL;
SELECT * FROM gold.dim_products WHERE product_key IS NULL;


-- ============================================================
-- 4. REFERENTIAL INTEGRITY CHECK (FACT → DIM)
-- ============================================================
-- Missing Products
SELECT COUNT(*) AS missing_products
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products dp
ON fs.product_key = dp.product_key
WHERE dp.product_key IS NULL;

-- Missing Customers
SELECT COUNT(*) AS missing_customers
FROM gold.fact_sales fs
LEFT JOIN gold.dim_customers dc
ON fs.customer_key = dc.customer_key
WHERE dc.customer_key IS NULL;


-- ============================================================
-- 5. BUSINESS RULE VALIDATION
-- ============================================================
-- sales = quantity * price
SELECT *
FROM gold.fact_sales
WHERE sales_amount != quantity * price;


-- ============================================================
-- 6. NEGATIVE / INVALID VALUES
-- ============================================================
SELECT *
FROM gold.fact_sales
WHERE quantity <= 0
   OR price <= 0
   OR sales_amount <= 0;


-- ============================================================
-- 7. DATE VALIDATION
-- ============================================================
SELECT *
FROM gold.fact_sales
WHERE order_date > shipping_date
   OR shipping_date > due_date;


-- ============================================================
-- 8. DATA STANDARDIZATION CHECK
-- ============================================================
SELECT DISTINCT gender FROM gold.dim_customers;
SELECT DISTINCT country FROM gold.dim_customers;
SELECT DISTINCT product_line FROM gold.dim_products;


-- ============================================================
-- 9. COVERAGE CHECK
-- ============================================================
SELECT 
    COUNT(*) AS total_sales,
    COUNT(product_key) AS matched_products,
    COUNT(customer_key) AS matched_customers
FROM gold.fact_sales;


-- ============================================================
-- END OF QA SCRIPT
-- ============================================================
 
