use DataWarehouse;

exec silver.load_silver;

create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime; 
	begin try
		print '========================';
		print 'Loading Silver Layer';
		print '========================';

		print '------------------------';
		print 'Loading CRM Tables';
		print '------------------------';

	set @start_time = GETDATE()
	print '>> Truncating Table : silver.crm_cust_info';
	Truncate table silver.crm_cust_info;
	print '>> Inserting Data Into : silver.crm_cust_info';
	insert into silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

		select 
		cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,

		case when upper(trim(cst_marital_status)) = 'S' then 'Singe'
			 when upper(trim(cst_marital_status)) = 'M' then 'Married'
			 else 'n/a'
		end cst_marital_status,

		case when upper(trim(cst_gndr)) = 'M' then 'Male'
			 when upper(trim(cst_gndr)) = 'F' then 'Female'
			 else 'n/a'
		end cst_gndr,

		cst_create_date
		from
			(select 
			*,ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
			from bronze.crm_cust_info)t
		where flag_last = 1;
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';


	IF OBJECT_ID('silver.crm_prd_info','U') is not null
		drop table silver.crm_prd_info;
	create table silver.crm_prd_info (
		prd_id int,
		cat_id nvarchar(40),
		prd_key nvarchar(40),
		prd_nm nvarchar(40),
		prd_cost int,
		prd_line nvarchar(40),
		prd_start_dt date,
		prd_end_dt date,
		dwh_create_date datetime2 default getdate()
	);

	set @start_time = GETDATE()
	print '>> Truncating Table : silver.crm_prd_info';
	Truncate table silver.crm_prd_info;
	print '>> Inserting Data Into : silver.crm_prd_info';
	insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)

		select 
		prd_id,
		replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,

		case when upper(trim(prd_line)) = 'M' then 'Mountain'
			 when upper(trim(prd_line)) = 'R' then 'Road'
			 when upper(trim(prd_line)) = 'S' then 'Other Sales'
			 when upper(trim(prd_line)) = 'T' then 'Touring'
			 else 'n/a'
		end prd_line,

		cast (prd_start_dt as date) as prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		from bronze.crm_prd_info;
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';


	IF OBJECT_ID('silver.crm_sales_details','U') is not null
		drop table silver.crm_sales_details;
	create table silver.crm_sales_details (
		sls_ord_num nvarchar(40),
		sls_prd_key nvarchar(40),
		sls_cust_id int,
		sls_order_dt date,
		sls_ship_dt date,
		sls_due_dt date,
		sls_sales int,
		sls_quantity int,
		sls_price int,
		dwh_create_date datetime2 default getdate()
	);

	set @start_time = GETDATE()
	print '>> Truncating Table : silver.crm_sales_details';
	Truncate table silver.crm_sales_details;
	print '>> Inserting Data Into : silver.crm_sales_details';
	insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price)

		select
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			 else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,

		case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
			 else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt, 

		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
			 else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt, 

		case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price) then sls_quantity * abs(sls_price)
			 else sls_sales
		end as sls_sales,

		sls_quantity,

		case when sls_price is null or sls_price <= 0 then sls_sales / nullif(sls_quantity,0)
			 else sls_price
		end as sls_price

		from bronze.crm_sales_details;
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';


		print '========================';
		print 'Loading ERP Tables';
		print '========================';

	set @start_time = GETDATE()
	print '>> Truncating Table : silver.erp_cust_az12';
	Truncate table silver.erp_cust_az12;
	print '>> Inserting Data Into : silver.erp_cust_az12';
	insert into silver.erp_cust_az12 (
	cid,
	bdate,
	gen)
		select 
		case when cid like 'NAS%' then SUBSTRING(cid,4,len(cid))
			 else cid
		end as cid,

		case when bdate > GETDATE() then null
			 else bdate
		end as bdate,

		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			 when upper(trim(gen)) in ('M','MALE') then 'Male'
			 else 'n/a'
		end as gen

		from bronze.erp_cust_az12;
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';

	set @start_time = GETDATE()
	print '>> Truncating Table : silver.erp_loc_a101';
	Truncate table silver.erp_loc_a101;
	print '>> Inserting Data Into : silver.erp_loc_a101';
	insert into silver.erp_loc_a101(
	cid,
	cntry)

		select
		replace(cid,'-','') as cid,

		case when trim(cntry) = 'DE' then 'Germany'
			 when trim(cntry) in ('US','USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'n/a'
			 else trim(cntry)
		end as cntry

		from bronze.erp_loc_a101;
		set @end_time = GETDATE()
		print '>> LOAD DURATION : ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' SECONDS';
		print '------------------------';

	set @start_time = GETDATE()
	print '>> Truncating Table : silver.erp_px_cat_g1v2';
	Truncate table silver.erp_px_cat_g1v2;
	print '>> Inserting Data Into : silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2 (
	id,
	cat,
	subcat,
	maintenance)
		select 
		id,
		cat,
		subcat,
		maintenance
		from bronze.erp_px_cat_g1v2;
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