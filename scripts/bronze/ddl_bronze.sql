/*
==============================================
DDL Script: Create Bronze Tables
==============================================

Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables
==============================================
*/


-- ───────────────────────────── CRM (source: crm) ─────────────────────────────

IF OBJECT_ID(N'bronze.crm_customer_info', N'U') IS NOT NULL
    DROP TABLE bronze.crm_customer_info;           -- reset to a clean state
CREATE TABLE bronze.crm_customer_info (
    cst_id              INT,                       -- source natural id
    cst_key             NVARCHAR(50),              -- business key from CRM
    cst_firstname       NVARCHAR(50),
    cst_lastname        NVARCHAR(50),
    cst_material_status NVARCHAR(50),              -- as provided by source
    cst_gndr            NVARCHAR(50),
    cst_create_date     DATE
);

IF OBJECT_ID(N'bronze.crm_product_info', N'U') IS NOT NULL
    DROP TABLE bronze.crm_product_info;
CREATE TABLE bronze.crm_product_info (
    prd_id      INT,
    prd_key     NVARCHAR(50),                      -- business/product key
    prd_nm      NVARCHAR(50),
    prd_cost    INT,                               -- kept as INT per source
    prd_line    NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);

IF OBJECT_ID(N'bronze.crm_sales_details', N'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details (
    sls_ord_num   NVARCHAR(50),
    sls_prd_key   NVARCHAR(50),
    sls_cust_id   INT,
    sls_ord_dt    INT,                             -- dates stored as INT (e.g., yyyymmdd) to mirror source; parsed later in silver
    sls_ship_dt   INT,
    sls_due_dt    INT,
    sls_sales     INT,
    sls_quantity  INT,
    sls_price     INT
);

-- ───────────────────────────── ERP (source: erp) ─────────────────────────────

IF OBJECT_ID(N'bronze.erp_loc_a101', N'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101 (
    cid    NVARCHAR(50),
    cntry  NVARCHAR(50)
);

IF OBJECT_ID(N'bronze.erp_cust_az12', N'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
);

IF OBJECT_ID(N'bronze.erp_px_cat_g1v2', N'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          NVARCHAR(50),
    cat         NVARCHAR(50),
    subcat      NVARCHAR(50),
    maintenance NVARCHAR(50)
);
