/*
================================================================================
Quality Checks
================================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency,
    and accuracy of the Gold Layer. These checks ensure:
    
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.

================================================================================
*/

-- =============================================================================
-- Checking 'gold.dim_customers'
-- =============================================================================

-- Check for Uniqueness of Customer Key in gold.dim_customers
-- Expectation: No results
SELECT 
    customer_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_key
HAVING COUNT(*) > 1;


-- Check for Uniqueness of Customer ID in gold.dim_customers
-- Expectation: No results (each customer should appear only once)
SELECT 
    customer_id,
    COUNT(*) AS duplicate_count
FROM gold.dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;


-- Check for NULL values in critical Customer Dimension fields
-- Expectation: No results
SELECT 
    'customer_key' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_customers
WHERE customer_key IS NULL
UNION ALL
SELECT 
    'customer_id' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_customers
WHERE customer_id IS NULL
UNION ALL
SELECT 
    'first_name' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_customers
WHERE first_name IS NULL
UNION ALL
SELECT 
    'last_name' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_customers
WHERE last_name IS NULL;


-- =============================================================================
-- Checking 'gold.dim_products'
-- =============================================================================

-- Check for Uniqueness of Product Key in gold.dim_products
-- Expectation: No results
SELECT 
    product_key,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_key
HAVING COUNT(*) > 1;


-- Check for Uniqueness of Product ID in gold.dim_products
-- Expectation: No results (each active product should appear only once)
SELECT 
    product_id,
    COUNT(*) AS duplicate_count
FROM gold.dim_products
GROUP BY product_id
HAVING COUNT(*) > 1;


-- Check for NULL values in critical Product Dimension fields
-- Expectation: No results
SELECT 
    'product_key' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_products
WHERE product_key IS NULL
UNION ALL
SELECT 
    'product_id' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_products
WHERE product_id IS NULL
UNION ALL
SELECT 
    'product_name' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_products
WHERE product_name IS NULL
UNION ALL
SELECT 
    'category' AS field_name,
    COUNT(*) AS null_count
FROM gold.dim_products
WHERE category IS NULL;


-- Verify only active products are included (prd_end_dt IS NULL in source)
-- Expectation: 0 inactive products
SELECT 
    COUNT(*) AS inactive_product_count
FROM silver.crm_product_info
WHERE prd_end_dt IS NOT NULL
  AND prd_id IN (SELECT product_id FROM gold.dim_products);


-- =============================================================================
-- Checking 'gold.fact_sales'
-- =============================================================================

-- Check for NULL values in fact table foreign keys
-- Expectation: No results (all sales should link to valid dimensions)
SELECT 
    'product_key' AS field_name,
    COUNT(*) AS null_count
FROM gold.fact_sales
WHERE product_key IS NULL
UNION ALL
SELECT 
    'customer_key' AS field_name,
    COUNT(*) AS null_count
FROM gold.fact_sales
WHERE customer_key IS NULL;


-- Check for NULL values in critical fact measures
-- Expectation: No results
SELECT 
    'sales_amount' AS field_name,
    COUNT(*) AS null_count
FROM gold.fact_sales
WHERE sales_amount IS NULL
UNION ALL
SELECT 
    'quantity' AS field_name,
    COUNT(*) AS null_count
FROM gold.fact_sales
WHERE quantity IS NULL
UNION ALL
SELECT 
    'order_date' AS field_name,
    COUNT(*) AS null_count
FROM gold.fact_sales
WHERE order_date IS NULL;


-- =============================================================================
-- Referential Integrity Checks
-- =============================================================================

-- Check for orphaned records in fact_sales (product_key not in dim_products)
-- Expectation: No results
SELECT 
    f.order_number,
    f.product_key,
    'Product key not found in dimension' AS issue
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p ON f.product_key = p.product_key
WHERE f.product_key IS NOT NULL
  AND p.product_key IS NULL;


-- Check for orphaned records in fact_sales (customer_key not in dim_customers)
-- Expectation: No results
SELECT 
    f.order_number,
    f.customer_key,
    'Customer key not found in dimension' AS issue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c ON f.customer_key = c.customer_key
WHERE f.customer_key IS NOT NULL
  AND c.customer_key IS NULL;


-- =============================================================================
-- Business Logic Validation
-- =============================================================================

-- Check for negative sales amounts
-- Expectation: No results (unless returns are expected)
SELECT 
    order_number,
    sales_amount
FROM gold.fact_sales
WHERE sales_amount < 0;


-- Check for negative quantities
-- Expectation: No results (unless returns are expected)
SELECT 
    order_number,
    quantity
FROM gold.fact_sales
WHERE quantity < 0;


-- Check for zero or negative prices
-- Expectation: No results (free products should be handled differently)
SELECT 
    order_number,
    price
FROM gold.fact_sales
WHERE price <= 0;


-- Check for mismatched sales calculation (sales_amount should equal quantity * price)
-- Expectation: No results or acceptable rounding differences
SELECT 
    order_number,
    sales_amount,
    quantity,
    price,
    quantity * price AS calculated_sales,
    ABS(sales_amount - (quantity * price)) AS difference
FROM gold.fact_sales
WHERE ABS(sales_amount - (quantity * price)) > 0.01;  -- Allow 1 cent rounding


-- Check for future order dates
-- Expectation: No results
SELECT 
    order_number,
    order_date
FROM gold.fact_sales
WHERE order_date > GETDATE();


-- Check for shipping dates before order dates
-- Expectation: No results
SELECT 
    order_number,
    order_date,
    shipping_date
FROM gold.fact_sales
WHERE shipping_date < order_date;


-- =============================================================================
-- Data Completeness Checks
-- =============================================================================

-- Check customer dimension record count vs source
-- Expectation: Counts should match
SELECT 
    'Source (CRM)' AS source,
    COUNT(*) AS record_count
FROM silver.crm_customer_info
UNION ALL
SELECT 
    'Gold Dimension' AS source,
    COUNT(*) AS record_count
FROM gold.dim_customers;


-- Check product dimension record count vs active source products
-- Expectation: Counts should match
SELECT 
    'Active Source Products' AS source,
    COUNT(*) AS record_count
FROM silver.crm_product_info
WHERE prd_end_dt IS NULL
UNION ALL
SELECT 
    'Gold Dimension' AS source,
    COUNT(*) AS record_count
FROM gold.dim_products;


-- Check fact table record count vs source
-- Expectation: Counts should match
SELECT 
    'Source (Sales Details)' AS source,
    COUNT(*) AS record_count
FROM silver.crm_sales_details
UNION ALL
SELECT 
    'Gold Fact Table' AS source,
    COUNT(*) AS record_count
FROM gold.fact_sales;


-- =============================================================================
-- Summary Report
-- =============================================================================

-- Generate overall data quality summary
SELECT 
    'Gold Layer Quality Check Summary' AS report_title,
    (SELECT COUNT(*) FROM gold.dim_customers) AS customer_count,
    (SELECT COUNT(*) FROM gold.dim_products) AS product_count,
    (SELECT COUNT(*) FROM gold.fact_sales) AS sales_transaction_count,
    (SELECT COUNT(DISTINCT customer_key) FROM gold.fact_sales) AS customers_with_sales,
    (SELECT COUNT(DISTINCT product_key) FROM gold.fact_sales) AS products_with_sales,
    (SELECT SUM(sales_amount) FROM gold.fact_sales) AS total_sales_amount,
    (SELECT MIN(order_date) FROM gold.fact_sales) AS earliest_order_date,
    (SELECT MAX(order_date) FROM gold.fact_sales) AS latest_order_date;


/*
================================================================================
End of Quality Checks
================================================================================
*/
