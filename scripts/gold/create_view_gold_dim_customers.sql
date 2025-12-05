create view gold.dim_customers as 
    SELECT 
		row_number() over(order by cst_id) as customer_key,
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
		la.cntry as country,
        ci.cst_marital_status as marital_status,
		case when ci.cst_gndr != 'N/A' then ci.cst_gndr
			else coalesce (ca.gen,'N/A') end as gender,
		ca.bdate as birthdate,
        ci.cst_create_date as create_date           
    FROM silver.crm_customer_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ci.cst_key = ca.cid
    LEFT JOIN silver.erp_loc_a101 la
        ON ci.cst_key = la.cid

		select *
		from gold.dim_customers

