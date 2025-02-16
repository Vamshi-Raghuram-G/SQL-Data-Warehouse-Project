use DataWarehouse;

exec bronze.load_bronze

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime; 
	begin try
		print '========================';
		print 'Loading Bronze Layer';
		print '========================';

		print '------------------------';
		print 'Loading CRM Tables';
		print '------------------------';

		set @start_time = GETDATE()
		print '>> Truncating Table : bronze.crm_cust_info';
		Truncate table bronze.crm_cust_info;
		print '>> Inserting Data Into : bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\VAMSHI RAGHURAM G\Desktop\Data Science\SQL\Data Warehouse Project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';

		set @start_time = GETDATE()
		print '>> Truncating Table : bronze.crm_prd_info';
		Truncate table bronze.crm_prd_info;
		print '>> Inserting Data Into : bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\VAMSHI RAGHURAM G\Desktop\Data Science\SQL\Data Warehouse Project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';

		set @start_time = GETDATE()
		print '>> Truncating Table : bronze.crm_sales_details';
		Truncate table bronze.crm_sales_details;
		print '>> Inserting Data Into : bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\VAMSHI RAGHURAM G\Desktop\Data Science\SQL\Data Warehouse Project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';

		print '========================';
		print 'Loading ERP Tables';
		print '========================';

		set @start_time = GETDATE()
		print '>> Truncating Table : bronze.erp_cust_az12';
		Truncate table bronze.erp_cust_az12;
		print '>> Inserting Data Into : bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\VAMSHI RAGHURAM G\Desktop\Data Science\SQL\Data Warehouse Project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';

		set @start_time = GETDATE()
		print '>> Truncating Table : bronze.erp_loc_a101';
		Truncate table bronze.erp_loc_a101;
		print '>> Inserting Data Into : bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\VAMSHI RAGHURAM G\Desktop\Data Science\SQL\Data Warehouse Project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';

		set @start_time = GETDATE()
		print '>> Truncating Table : bronze.erp_px_cat_g1v2';
		Truncate table bronze.erp_px_cat_g1v2;
		print '>> Inserting Data Into : bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\VAMSHI RAGHURAM G\Desktop\Data Science\SQL\Data Warehouse Project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';

	end try
	begin catch
		print '========================';
		print 'ERROR LOADING BRONZE LAYER';
		print '========================';
	end catch
end

