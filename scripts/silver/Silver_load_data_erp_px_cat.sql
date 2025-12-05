/*
================================================================================
Query: Load Silver ERP Product Category Data (Bronze -> Silver)
================================================================================
Purpose:
    This query loads product category data from the ERP system's bronze layer
    into the silver layer without transformation.
    
    - Performs a direct pass-through of product category information
    - No data quality rules or transformations applied at this stage
    - Maintains original category hierarchy structure (category and subcategory)
    - Preserves maintenance flags for category management
    
    The data is loaded as-is because the source ERP system maintains high data
    quality standards for reference/master data tables.

Business Context:
    Product category data is reference/master data that is well-maintained in
    the source ERP system. This table provides the category hierarchy used for
    product classification and is referenced by product dimension tables.

================================================================================
*/
PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
TRUNCATE TABLE silver.erp_px_cat_g1v2;
PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';
INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;
