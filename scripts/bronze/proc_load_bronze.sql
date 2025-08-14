exec bronze.load_bronze

create or alter procedure bronze.load_bronze as
BEGIN
	BEGIN TRY
		print '=======================================================================';
		print 'Loading Bronze Layer';
		print '=======================================================================';


		print '-----------------------------------------------------------------------';
		print 'Loading CRM Table';
		print '-----------------------------------------------------------------------';

		print '>>Truncating table : bronze.crm_cust_info'
		truncate table bronze.crm_cust_info;

		print '>>Inserting Data into : bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info 
		from 'D:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		-----------------------------------------------------
		print '>>Truncating table : bronze.crm_prd_info'
		truncate table bronze.crm_prd_info;

			print '>>Inserting Data into : bronze.crm_prd_info'
		bulk insert bronze.crm_prd_info 
		from 'D:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		---------------------------------------------------------------
		print '>>Truncating table : bronze.crm_sales_details'
		truncate table bronze.crm_sales_details;

		print '>>Inserting Data into : bronze.crm_sales_details'
		bulk insert bronze.crm_sales_details 
		from 'D:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);

		print '-----------------------------------------------------------------------';
		print 'Loading ERP Table';
		print '-----------------------------------------------------------------------';

		print '>>Truncating table : bronze.erp_loc_a101'
		truncate table bronze.erp_loc_a101;

		print '>>Inserting Data into : bronze.erp_loc_a101'
		bulk insert bronze.erp_loc_a101
		from 'D:\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		---------------------------------------------------------
		print '>>Truncating table : bronze.erp_cust_az12'
		truncate table bronze.erp_cust_az12;

		print '>>Inserting Data into : bronze.erp_cust_az12'
		bulk insert bronze.erp_cust_az12 
		from 'D:\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		------------------------------------------------------
		print '>>Truncating table : bronze.erp_px_cat_g1v2'
		truncate table bronze.erp_px_cat_g1v2;

		print '>>Inserting Data into : bronze.erp_px_cat_g1v2'
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
	END TRY
	BEGIN CATCH
		print '==================================================================='
		print 'Eroor occur during loading bronze layer'
		print 'Error Meassage' + Error_Message();
		print 'Error Meassage' + CAST(Error_Number() AS Nvarchar);
		print 'Error Meassage' + CAST(Error_State()AS Nvarchar);
		print '==================================================================='
	END CATCH

END
