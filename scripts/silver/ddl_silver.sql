use DataWarehouse;

IF OBJECT_ID('silver.crm_cust_info','U') is not null
	drop table silver.crm_cust_info;
create table silver.crm_cust_info (
	cst_id int,
	cst_key nvarchar(40),
	cst_firstname nvarchar(40),
	cst_lastname nvarchar(40),
	cst_marital_status nvarchar(40),
	cst_gndr nvarchar(40),
	cst_create_date date,
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.crm_prd_info','U') is not null
	drop table silver.crm_prd_info;
create table silver.crm_prd_info (
	prd_id int,
	prd_key nvarchar(40),
	prd_nm nvarchar(40),
	prd_cost int,
	prd_line nvarchar(40),
	prd_start_dt datetime,
	prd_end_dt datetime,
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.crm_sales_details','U') is not null
	drop table silver.crm_sales_details;
create table silver.crm_sales_details (
	sls_ord_num nvarchar(40),
	sls_prd_key nvarchar(40),
	sls_cust_id int,
	sls_order_dt int,
	sls_ship_dt int,
	sls_due_dt int,
	sls_sales int,
	sls_quantity int,
	sls_price int,
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.erp_cust_az12','U') is not null
	drop table silver.erp_cust_az12;
create table silver.erp_cust_az12 (
	cid	nvarchar(40),
	bdate date,
	gen nvarchar(40),
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.erp_loc_a101','U') is not null
	drop table silver.erp_loc_a101;
create table silver.erp_loc_a101 (
	cid nvarchar(40),	
	cntry nvarchar(40),
	dwh_create_date datetime2 default getdate()
);

IF OBJECT_ID('silver.erp_px_cat_g1v2','U') is not null
	drop table silver.erp_px_cat_g1v2;
create table silver.erp_px_cat_g1v2 (
	id nvarchar(40),
	cat	nvarchar(40),
	subcat nvarchar(40),	
	maintenance nvarchar(40),
	dwh_create_date datetime2 default getdate()
);