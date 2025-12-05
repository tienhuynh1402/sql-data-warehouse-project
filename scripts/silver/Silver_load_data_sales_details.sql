/*
================================================================================
Query: Load Silver CRM Sales Details (Bronze -> Silver)
================================================================================
Purpose:
    This query transforms sales transaction data from the bronze layer, performing
    the following data quality and validation operations:
    
    - Converts integer date fields (YYYYMMDD format) to proper DATE types
    - Validates and corrects sales amounts using quantity and price calculations
    - Handles missing or invalid price values by deriving from sales/quantity
    - Ensures data consistency through validation rules
    - Handles invalid date formats by setting them to NULL
    
    The transformation creates clean, validated sales records suitable for
    analytical queries and reporting.

Business Context:
    Sales data may contain data quality issues from the source system including
    invalid dates, missing prices, or incorrect sales calculations. This query
    applies business rules to correct these issues and ensure analytical accuracy.

================================================================================
*/
PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLE silver.crm_sales_details;
PRINT '>> Inserting Data Into: silver.crm_sales_details';
INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_ord_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)

SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    -- Convert integer date (YYYYMMDD) to DATE, handle invalid formats
    CASE WHEN sls_ord_dt <= 0 OR LEN(sls_ord_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ord_dt AS VARCHAR) AS DATE)
    END                                                                           AS sls_ord_dt,
    CASE WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
    END                                                                           AS sls_ship_dt,
    CASE WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 THEN NULL
         ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
    END                                                                           AS sls_due_dt,
    -- Validate sales amount: recalculate if null, negative, or doesn't match quantity * price
    CASE WHEN sls_sales IS NULL 
              OR sls_sales <= 0 
              OR sls_sales != sls_quantity * ABS(sls_price)
         THEN sls_quantity * ABS(sls_price)
         ELSE sls_sales
    END                                                                           AS sls_sales,
    sls_quantity,
    -- Derive price from sales/quantity if price is null or invalid
    CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity, 0)
         ELSE sls_price
    END                                                                           AS sls_price
FROM bronze.crm_sales_details;




