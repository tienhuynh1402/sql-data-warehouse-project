/*
================================================================================
Script: Create Silver Layer Tables (Schema Definition)
================================================================================
Purpose:
    This script creates the silver layer tables in the data warehouse, which
    serve as the intermediate/cleansed data layer between bronze (raw) and
    gold (business/analytical) layers.
    
    The script performs the following operations:
    - Drops existing silver tables if they exist (idempotent script design)
    - Creates silver layer tables for CRM and ERP source systems
    - Defines schema structure for cleansed, standardized data
    - Adds DWH audit columns for tracking record creation timestamps
    
    The silver layer stores business-ready data with standardized formats,
    validated values, and proper data types ready for consumption by the
    gold layer or analytical queries.

Table Organization:
    CRM System Tables:
        - silver.crm_customer_info: Customer master data
        - silver.crm_product_info: Product dimension with temporal validity
        - silver.crm_sales_details: Sales transactions
    
    ERP System Tables:
        - silver.erp_cust_az12: Customer demographics
        - silver.erp_loc_a101: Customer location/geography
        - silver.erp_px_cat_g1v2: Product category hierarchy

================================================================================
*/

-- ═══════════════════════════════════════════════════════════════════════════
-- CRM SYSTEM TABLES
-- ═══════════════════════════════════════════════════════════════════════════

-- Customer Master Data
IF OBJECT_ID(N'silver.crm_customer_info', N'U') IS NOT NULL
    DROP TABLE silver.crm_customer_info;

CREATE TABLE silver.crm_customer_info (
    cst_id              INT,                       -- Source system customer ID (natural key)
    cst_key             NVARCHAR(50),              -- Business key from CRM system
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_marital_status  NVARCHAR(50),              -- Standardized: 'Single', 'Married', 'N/A'
    cst_gndr            NVARCHAR(50),              -- Standardized: 'Female', 'Male', 'N/A'
    cst_create_date     DATE,                      -- Original record creation date from source
    dwh_create_date     DATETIME2 DEFAULT GETDATE() -- DWH load timestamp
);

-- Product Dimension with Temporal Validity (SCD Type 2)
IF OBJECT_ID(N'silver.crm_product_info', N'U') IS NOT NULL
    DROP TABLE silver.crm_product_info;

CREATE TABLE silver.crm_product_info (
    prd_id       INT,
    cat_id       NVARCHAR(50),                      -- Derived from product key
    prd_key      NVARCHAR(50),                      -- Business/product key
    prd_nm       NVARCHAR(50),
    prd_cost     INT,                               -- Kept as INT per source system
    prd_line     NVARCHAR(50),                      -- Standardized: 'Mountain', 'Road', 'Touring', etc.
    prd_start_dt DATE,                              -- Version validity start date
    prd_end_dt   DATE,                              -- Version validity end date (NULL = current)
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Sales Transaction Facts
IF OBJECT_ID(N'silver.crm_sales_details', N'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_ord_dt    DATE,                             -- Converted from YYYYMMDD integer format
    sls_ship_dt   DATE,
    sls_due_dt    DATE,
    sls_sales     INT,                              -- Validated/corrected sales amount
    sls_quantity  INT,
    sls_price     INT,                              -- Validated/derived unit price
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- ═══════════════════════════════════════════════════════════════════════════
-- ERP SYSTEM TABLES
-- ═══════════════════════════════════════════════════════════════════════════

-- Customer Location/Geography
IF OBJECT_ID(N'silver.erp_loc_a101', N'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid    NVARCHAR(50),                            -- Normalized customer ID (hyphens removed)
    cntry  NVARCHAR(50),                            -- Standardized country names
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Customer Demographics
IF OBJECT_ID(N'silver.erp_cust_az12', N'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),                            -- Cleaned customer ID (legacy prefix removed)
    bdate  DATE,                                    -- Birth date (validated, future dates set to NULL)
    gen    NVARCHAR(50),                            -- Standardized: 'Female', 'Male', 'N/A'
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

-- Product Category Hierarchy (Reference Data)
IF OBJECT_ID(N'silver.erp_px_cat_g1v2', N'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),                       -- Category level
    subcat      NVARCHAR(50),                       -- Subcategory level
    maintenance NVARCHAR(50),                       -- Maintenance flag/status
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);