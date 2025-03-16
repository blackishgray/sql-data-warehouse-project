-------------------------------------------------------
------------ CREATE VIEW FOR GOLD LAYER ---------------
-------------------------------------------------------
CREATE VIEW gold.dim_customers AS 
select 
ROW_NUMBER() over (order by cst_id) AS customer_key,
ci.cst_id as customer_id, 
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
case 
	when ci.cst_gndr != 'NA' then ci.cst_gndr 
	else coalesce(ca.gen, 'NA')
	END as gender, 
ci.cst_marital_status as marital_status,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci 
left join silver.erp_cust_az12 ca 
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la 
on ci.cst_key = la.cid
;

create view gold.dim_products as 
select 
ROW_NUMBER () over(order by pn.prd_start_dt, pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as sub_category,
pc.maintainance as maintainance,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prd_info pn 
left join silver.erp_px_cat_g1v2 pc 
on pn.cat_id = pc.id
where prd_end_dt is Null

;

CREATE VIEW gold.fact_sales as 
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt as order_date,
sd.sls_ship_dt as shiping_date,
sd.sls_due_dt as due_date,
sd.sls_sales as sales,
sd.sls_quantity as quantity,
sd.sls_price as sales_price
from silver.crm_sales_details sd 
left join gold.dim_products pr 
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu 
on sd.sls_cust_id = cu.customer_id
;
