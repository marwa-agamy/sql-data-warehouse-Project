-- Load Data from Source Systems into the Bronze Layer
-- Truncate Target Tables before Load (Full Load strategy)
-- Use BULK INSERT to Ingest Data from Source Files into the DWH
-- Encapsulate Ingestion Logic inside a Stored Procedure
--------------------------------------------------------------------------------
--Create Stored Procedure with Naming Convention --> SchemaName.load_<layer>
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
 BEGIN
  DECLARE @start_time DATETIME , @end_time DATETIME ,@batch_start_time DATETIME ,@batch_end_time DATETIME;
	BEGIN TRY
	  SET @batch_start_time = GETDATE();
	  PRINT '==========================================================';
		PRINT 'Loading Bronze Layer ';
		PRINT '==========================================================';

		PRINT '----------------------------------------------------------';
		PRINT 'Loading CRM Tables ';
		PRINT '----------------------------------------------------------';

		--Load crm_cust_info Data
		SET @start_time = GETDATE();
		PRINT 'Truncating Table : bronze.crm_cust_info ';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT 'Inserting Data into Table : bronze.crm_cust_info ';
		BULK INSERT bronze.crm_cust_info FROM 'C:\Users\Mero\Downloads\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2 ,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +  'Seconds';

		PRINT '==========================================================';

		--Load crm_prd_info Data
		SET @start_time = GETDATE();
		PRINT 'Truncating Table : bronze.crm_prd_info ';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'Inserting Data into Table : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info FROM 'C:\Users\Mero\Downloads\source_crm\prd_info.csv'
		WITH(
		FIRSTROW = 2 ,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		print'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +  'Seconds';

		PRINT '==========================================================';

		--Load crm_sales_details Data
		SET @start_time = GETDATE();
		PRINT ' Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		Print 'Inserting Data into Table : bronze.crm_sales_details ';
		BULK INSERT bronze.crm_sales_details FROM 'C:\Users\Mero\Downloads\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		Set @end_time = GETDATE();
		print'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +  'Seconds';

		PRINT '==========================================================';


		PRINT '----------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------------------';
		--Load erp_CUST_AZ12 Data
		Set @start_time = GETDATE();
		PRINT 'Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT 'Inserting Data into Table : bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12 FROM 'C:\Users\Mero\Downloads\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		Set @end_time = GetDate();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +  'Seconds';
		
		PRINT '==========================================================';

		--Load erp_LOC_A101 Data
		Set @start_time = GETDATE();
		PRINT 'Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT 'Inserting Data into Table : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101 FROM 'C:\Users\Mero\Downloads\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		Set @end_time = GetDate();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +  'Seconds';

		PRINT '==========================================================';

		--Load erp_PX_CAT_G1V2 Data
		Set @start_time = GETDATE();
		PRINT 'Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT 'Inserting Data into Table : bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2 FROM 'C:\Users\Mero\Downloads\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		Set @end_time = GetDate();
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +  'Seconds';

		PRINT '==========================================================';

		PRINT '=========================================================='
		Set @batch_end_time = GetDate(); 
		PRINT 'Loadind Bronze Layer is Completed';
		PRINT 'Total Load Duration:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) +  'Seconds';
		PRINT '=========================================================='
	END TRY
      
	BEGIN CATCH
	PRINT '==========================================================';
	PRINT 'Erorr Occured During Loading Bronze Layer';
	PRINT 'Erorr Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT '==========================================================';
	END CATCH

 END
