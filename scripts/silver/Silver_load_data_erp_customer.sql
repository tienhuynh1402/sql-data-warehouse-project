/*
================================================================================
Query: Load Silver ERP Customer Data (Bronze -> Silver)
================================================================================
Purpose:
    This query transforms customer data from the ERP system's bronze layer,
    performing the following data quality and standardization operations:
    
    - Cleans customer IDs by removing legacy system prefixes
    - Validates birth dates to ensure logical data (no future dates)
    - Standardizes gender values to consistent descriptive formats
    - Handles various gender input formats (codes and full text)
    
    The transformation creates clean, standardized ERP customer records suitable
    for integration with CRM customer data and analytical reporting.

Business Context:
    ERP customer data uses a different customer identifier format than the CRM
    system. Legacy records may contain a "NAS" prefix that needs to be stripped
    for proper integration with other systems.

================================================================================
*/
PRINT '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLE silver.erp_cust_az12;
PRINT '>> Inserting Data Into: silver.erp_cust_az12';
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT 
    -- Remove legacy "NAS" prefix from customer IDs for standardization
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
         ELSE cid
    END                                                                           AS cid,
    -- Validate birth date: set to NULL if in the future (data quality issue)
    CASE WHEN bdate > GETDATE() THEN NULL
         ELSE bdate
    END                                                                           AS bdate,
    -- Standardize gender values to consistent format (handles both codes and full text)
    CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female' 
         WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male' 
         ELSE 'N/A'
    END                                                                           AS gen
FROM bronze.erp_cust_az12;




