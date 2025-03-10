
CREATE OR ALTER PROCEDURE insert_date_into_silver AS 
BEGIN 
	print ' -- Truncating table silver.crm_cust_info; '
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT ' -- Inserting date into table silver.crm_cust_info; '
	INSERT INTO silver.crm_cust_info
	(
	cst_id ,
	cst_key ,
	cst_firstname ,
	cst_lastname ,
	cst_marital_status ,
	cst_gndr ,
	cst_create_date 
	)
	select
		cst_id,
		cst_key,
		TRIM(cst_firstname) as cst_firstname,
		TRIM(cst_lastname) as cst_lastname,
		--cst_marital_status,
		case 
			when upper(cst_marital_status) = 'M' then 'Married'
			when upper(cst_marital_status) = 'S' then 'Single'
			Else 'NA'
		End as cst_marital_status,
		case 
			when upper(cst_gndr) = 'F' then 'Female'
			when upper(cst_gndr) = 'M' then 'Male'
			Else 'NA'
		End as cst_gndr,
		cst_create_date
	from 
	(
		select *, ROW_NUMBER() OVER(partition by cst_id order by cst_create_date desc) as rnk
		from bronze.crm_cust_info 
		where cst_id is not null
	) a 
	where a.rnk=1;
	
	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
	prd_id
	,cat_id
	,prd_key      
	,prd_nm       
	,prd_cost     
	,prd_line     
	,prd_start_dt 
	,prd_end_dt   
	)
	select 
		prd_id,
		--prd_key,
		replace(SUBSTRING(prd_key, 1, 5), '', '_') as cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) as prd_key, 
		prd_nm,
		coalesce(prd_cost, 0) as prd_cost,
		case 
			when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
			when UPPER(TRIM(prd_line)) = 'R' then 'Road'
			when UPPER(TRIM(prd_line)) = 'S' then 'Other_Sales'
			when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
			else 'NA'
			END as prd_line,
		cast(prd_start_dt AS date),
		--prd_end_dt,
		dateadd(DD, -1, LEAD(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt
	from bronze.crm_prd_info;


	TRUNCATE TABLE silver.crm_sales_details;
	insert into silver.crm_sales_details (
	sls_ord_num		
	,sls_prd_key	
	,sls_cust_id	
	,sls_order_dt	
	,sls_ship_dt	
	,sls_due_dt		
	,sls_sales		
	,sls_quantity	
	,sls_price		
	)
	select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		--sls_order_dt,
		case
			when sls_order_dt = 0 or LEN(sls_order_dt) != 8 then null 
			else cast(CAST(sls_order_dt AS varchar) as date)
		END as sls_order_dt,
		case
			when sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 then null 
			else cast(CAST(sls_ship_dt AS varchar) as date)
		END as sls_ship_dt,
		--sls_ship_dt,
		case
			when sls_due_dt = 0 or LEN(sls_due_dt) != 8 then null 
			else cast(CAST(sls_due_dt AS varchar) as date)
		END as sls_due_dt,
		--sls_due_dt,
		case 
			when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * abs(sls_price)
				then sls_quantity * abs(sls_price)
			else sls_sales 
		end as sls_sales,
		sls_quantity,
		case 
			when sls_price is null or sls_price <= 0
				then sls_sales / nullif(sls_quantity, 0)
			else sls_price 
		end as sls_price
	from bronze.crm_sales_details
	;

	TRUNCATE TABLE silver.erp_cust_az12;
	insert into silver.erp_cust_az12 (
	cid,
	bdate,
	gen
	)
	select 
		case 
			when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
			else cid 
		end as cid,
		case 
			when bdate > GETDATE() then null
			else bdate
		end as bdate,
		case 
			when UPPER(trim(gen)) in ('M', 'Male') then 'Male'
			when UPPER(TRIM(gen)) In ('F', 'Female') then 'Female'
			else 'NA'
		end as gen 
	from bronze.erp_cust_az12;

	TRUNCATE TABLE silver.erp_loc_a101;
	insert into silver.erp_loc_a101 
	(
	cid,
	cntry
	)
	select 
		REPLACE(cid, '-', '') as cid,
		case 
			when TRIM(cntry) = 'DE' then 'Germany'
			when TRIM(cntry) IN ('USA', 'US') then 'United States'
			when TRIM(cntry) = '' OR cntry IS null then 'NA'
		else TRIM(cntry)
		end as cntry
	from bronze.erp_loc_a101;

	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	insert into silver.erp_px_cat_g1v2 
	(
	 id, cat, subcat, maintainance
	)
	select id, cat, subcat, maintainance
	from bronze.erp_px_cat_g1v2
	;
END
;

exec insert_date_into_silver;
