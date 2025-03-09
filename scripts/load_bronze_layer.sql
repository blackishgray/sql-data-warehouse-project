

CREATE OR ALTER PROCEDURE bronze.load_broze AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time datetime, @batch_end_time datetime
	BEGIN TRY
		set @batch_start_time = GETDATE()
		PRINT '=============================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '=============================================================';

		PRINT '-------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------------';

		set @start_time=GETDATE();
		PRINT '>> Truncating Table bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> Inserting Data into bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'D:\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		;
		set @end_time=GETDATE();
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar)+ ' seconds.'
		PRINT '-------------'

		set @start_time=GETDATE();
		PRINT '>> Truncating Table bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> Inserting Data into bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			---ROWTERMINATOR='\r\n',
			--FIELDQUOTE = '\',
			--FORMAT='CSV',
			--FORMATFILE='D:\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv',
			TABLOCK
		)
		;
		set @end_time=GETDATE();
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds.'
		PRINT '-------------'

		set @start_time=GETDATE();
		PRINT '>> Truncating Table bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> Inserting Data into bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		;
		set @end_time=GETDATE();
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds.'
		PRINT '-------------'

		PRINT '-------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------------';

		set @start_time=GETDATE();
		PRINT '>> Truncating Table bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> Inserting Data into bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		;
		set @end_time=GETDATE();
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds.'
		PRINT '-------------'

		set @start_time=GETDATE();
		PRINT '>> Truncating Table bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> Inserting Data into bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		;
		set @end_time=GETDATE();
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds.'
		PRINT '-------------'

		set @start_time=GETDATE();
		PRINT '>> Truncating Table bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> Truncating Table bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\datasets\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		)
		;
		set @end_time=GETDATE();
		PRINT '>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds.'
		PRINT '-------------'
		set @batch_end_time = GETDATE()
		PRINT '>> Load Duration for the entire bronze layer:' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds.'
	END TRY
	BEGIN CATCH 
		PRINT '=============================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=============================================================';
	END CATCH
END 
;

