/*
================================================================================
Query: Load Silver CRM Customer Information (Bronze -> Silver)
================================================================================
Purpose:
    This query loads and transforms customer data from the bronze layer into the
    silver layer, performing the following operations:
    
    - Deduplicates customer records by selecting the most recent version
    - Standardizes gender codes into full descriptive values
    - Standardizes marital status codes into full descriptive values
    - Cleans customer names by removing leading/trailing whitespace
    - Filters out invalid records (null customer IDs)
    
    The transformation creates a clean, deduplicated customer dimension with
    standardized attribute values suitable for analytics and reporting.

Business Context:
    Customer information may exist in multiple versions due to updates from the
    source CRM system. This query implements a "current state" approach, keeping
    only the most recent record for each customer based on creation date.

================================================================================
*/
PRINT '>> Truncating Table: silver.crm_customer_info';
TRUNCATE TABLE silver.crm_customer_info;
PRINT '>> Inserting Data Into: silver.crm_customer_info';
INSERT INTO silver.crm_customer_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname)                                                           AS cst_firstname,
    TRIM(cst_lastname)                                                            AS cst_lastname,
    -- Standardize marital status codes to full descriptive values
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
         WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'N/A'
    END                                                                           AS cst_marital_status,
    -- Standardize gender codes to full descriptive values
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
         ELSE 'N/A'
    END                                                                           AS cst_gndr,
    cst_create_date
FROM (
    -- Deduplicate: Keep only the most recent record for each customer
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)    AS flag_last
    FROM bronze.crm_customer_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;
) t
WHERE flag_last = 1;