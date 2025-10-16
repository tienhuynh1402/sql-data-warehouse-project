/*
================================================================================
Script: Create Gold Layer Dimensional Model Views
================================================================================
Purpose:
    This script creates the gold layer views that implement a dimensional model
    (star schema) for analytical queries and reporting. The gold layer transforms
    silver layer data into business-friendly structures optimized for BI tools.
    
    Views Created:
    - gold.dim_customers: Customer dimension with integrated CRM and ERP data
    - gold.dim_products: Product dimension with current/active products only
    - gold.fact_sales: Sales fact table linking to customer and product dimensions
    
    Key Features:
    - Surrogate keys (customer_key, product_key) for dimensional modeling
    - Integrated data from multiple source systems (CRM + ERP)
    - Business-friendly column names
    - Type 1 SCD for customers (current state only)
    - Active products only (filters out historical versions)
    
    The dimensional model enables efficient analytical queries with simplified
    joins and intuitive business terminology.

================================================================================
*/

-- ═══════════════════════════════════════════════════════════════════════════
-- CUSTOMER DIMENSION
-- ═══════════════════════════════════════════════════════════════════════════

/*
View: gold.dim_customers
Purpose: Customer dimension integrating CRM customer master data with ERP 
         demographics and location information. Provides a single, complete
         view of customer attributes for analytical queries.
*/

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id)                                          AS customer_key,
    ci.cst_id                                                                    AS customer_id,
    ci.cst_key                                                                   AS customer_number,
    ci.cst_firstname                                                             AS first_name,
    ci.cst_lastname                                                              AS last_name,
    la.cntry                                                                     AS country,
    ci.cst_marital_status                                                        AS marital_status,
    -- Prioritize CRM gender, fallback to ERP gender if CRM is N/A
    CASE WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr 
         ELSE COALESCE(ca.gen, 'N/A')
    END                                                                          AS gender,
    ca.bdate                                                                     AS birthdate,
    ci.cst_create_date                                                           AS create_date
FROM silver.crm_customer_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;
GO


-- ═══════════════════════════════════════════════════════════════════════════
-- PRODUCT DIMENSION
-- ═══════════════════════════════════════════════════════════════════════════

/*
View: gold.dim_products
Purpose: Product dimension with current/active products only (SCD Type 2 filtered
         to current versions). Integrates CRM product data with ERP category
         hierarchy for complete product classification.
         
Note: Only includes products where prd_end_dt IS NULL (current/active versions).
      Historical product versions are excluded from the dimensional model.
*/

CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key)                    AS product_key,
    pn.prd_id                                                                    AS product_id,
    pn.prd_key                                                                   AS product_number,
    pn.prd_nm                                                                    AS product_name,
    pn.cat_id                                                                    AS category_id,
    pc.cat                                                                       AS category,
    pc.subcat                                                                    AS subcategory,
    pc.maintenance                                                               AS maintenance,
    pn.prd_cost                                                                  AS cost,
    pn.prd_line                                                                  AS product_line,
    pn.prd_start_dt                                                              AS start_date
FROM silver.crm_product_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL;                                                     -- Current/active products only
GO


-- ═══════════════════════════════════════════════════════════════════════════
-- SALES FACT TABLE
-- ═══════════════════════════════════════════════════════════════════════════

/*
View: gold.fact_sales
Purpose: Sales fact table containing transaction-level sales data with foreign
         keys to customer and product dimensions. Implements a star schema design
         for efficient analytical queries and aggregations.
         
Structure: Fact table with measures (sales_amount, quantity, price) and 
           dimension keys (product_key, customer_key) plus date attributes.
*/

CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num                                                               AS order_number,
    pr.product_key,
    cu.customer_key,
    sd.sls_ord_dt                                                                AS order_date,
    sd.sls_ship_dt                                                               AS shipping_date,
    sd.sls_due_dt                                                                AS due_date,
    sd.sls_sales                                                                 AS sales_amount,
    sd.sls_quantity                                                              AS quantity,
    sd.sls_price                                                                 AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO


/*
================================================================================
Usage Examples:
================================================================================

-- Query total sales by customer
SELECT 
    c.customer_number,
    c.first_name,
    c.last_name,
    COUNT(*) AS order_count,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_number, c.first_name, c.last_name
ORDER BY total_sales DESC;

-- Query sales by product category
SELECT 
    p.category,
    p.subcategory,
    COUNT(DISTINCT f.order_number) AS order_count,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_products p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY total_sales DESC;

-- Query sales by country
SELECT 
    c.country,
    COUNT(DISTINCT f.order_number) AS order_count,
    SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
JOIN gold.dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_sales DESC;

================================================================================
*/
