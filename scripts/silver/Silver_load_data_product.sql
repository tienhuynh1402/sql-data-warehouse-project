/*
================================================================================
Query: Load Silver CRM Product Information (Bronze -> Silver)
================================================================================
Purpose:
    This query loads and transforms product data from the bronze layer into the
    silver layer, performing the following operations:
    
    - Extracts and standardizes product category identifiers from product keys
    - Splits product keys into category and product components
    - Standardizes product line codes into descriptive names
    - Handles missing cost values by defaulting to zero
    - Constructs temporal validity periods (start/end dates) for product versions
    - Ensures date fields are properly typed
    
    The transformation creates a clean, business-ready product dimension suitable
    for analytical queries and reporting.

Business Context:
    Products may have multiple versions over time (price changes, attribute updates).
    This query creates a slowly changing dimension (SCD Type 2) by calculating
    end dates based on the next version's start date.

================================================================================
*/
PRINT '>> Truncating Table: silver.crm_product_info';
TRUNCATE TABLE silver.crm_product_info;
PRINT '>> Inserting Data Into: silver.crm_product_info';
INSERT INTO silver.crm_product_info (
    prd_id,
    cat_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT 
    prd_id,
    -- Extract category from first 5 chars of product key, replace hyphens with underscores
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_')                                  AS cat_id,
    -- Extract actual product identifier (after position 6)
    SUBSTRING(prd_key, 7, LEN(prd_key))                                           AS prd_key,
    prd_nm,
    -- Default null costs to zero for consistent calculations
    ISNULL(prd_cost, 0)                                                           AS prd_cost,
    -- Standardize product line codes to full descriptive names
    CASE UPPER(TRIM(prd_line))
         WHEN 'M' THEN 'Mountain'
         WHEN 'R' THEN 'Road'
         WHEN 'T' THEN 'Touring'
         WHEN 'S' THEN 'Other Sales'
         ELSE 'N/A'
    END                                                                           AS prd_line,
    CAST(prd_start_dt AS DATE)                                                    AS prd_start_dt,
    -- Calculate end date as one day before the next version's start date
    -- NULL end date indicates current/active version
    CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key 
                                   ORDER BY prd_start_dt) - 1 AS DATE)           AS prd_end_dt
FROM bronze.crm_product_info;