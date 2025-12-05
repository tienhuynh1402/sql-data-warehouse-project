create view gold.dim_products as
select 
row_number () over (order by pn.prd_start_dt, pn.prd_key) as product_key,
 prd_id as product_id, prd_key as product_number, prd_nm as product_name, cat_id as category_id, pc.cat as category, pc.subcat as subcategory, pc.maintenance , prd_cost as cost, prd_line as product_line, prd_start_dt as start_date
from  silver.crm_product_info pn
left join  silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where prd_end_dt is null 

