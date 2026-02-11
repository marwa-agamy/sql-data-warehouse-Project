CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
 DECLARE @start_time DATETIME,@end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME
  BEGIN TRY
		SET @batch_start_time = GETDATE();
	    PRINT'===================================================================================================================='
		PRINT'Loading The Silver Layer'
		PRINT'===================================================================================================================='
		PRINT'Loading CRM Tables'
		PRINT'===================================================================================================================='
        SET @start_time = GETDATE();
	    PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Truncating the Table: silver.crm_cust_info'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
        TRUNCATE TABLE silver.crm_cust_info
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Inserting Data into: silver.crm_cust_info'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_gndr,
		cst_marital_status,
		cst_create_date
		)
		SELECT cst_id,cst_key,
		cst_firstname = TRIM(cst_firstname) ,
		cst_lastname = TRIM(cst_lastname) ,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			ELSE 'unknown'
		END cst_gndr,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
			ELSE 'unknown'
		END cst_marital_status	,
		cst_create_date
		FROM
			(
			SELECT *, ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date desc ) AS flag_last
			FROM bronze.crm_cust_info ) t WHERE flag_last = 1 AND cst_id IS NOT NULL
		SET @end_time = GETDATE();
		PRINT'Duration For Loading :'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'Seconds'
		----------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
        PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Truncating the Table: silver.crm_prd_info'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Inserting Data into: silver.crm_prd_info'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		INSERT INTO silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_' )AS cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
		TRIM(prd_nm) AS prd_nm ,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN(prd_line='M') THEN 'Mountain'
			WHEN(prd_line='R') THEN 'Road'
			WHEN(prd_line='T') THEN 'Touring'
			WHEN(prd_line='S') THEN 'Other Sales'
		ELSE 'unknown'
		END prd_line,
		prd_start_dt,
		DATEADD(DAY,-1,LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt
		FROM bronze.crm_prd_info 
		SET @end_time = GETDATE();
		PRINT'Duration For Loading :'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'Seconds'
		---------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Truncating the Table: silver.crm_sales_details'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Inserting Data into: silver.crm_sales_details'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		INSERT INTO silver.crm_sales_details (sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT  sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			 END sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			 END sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			 END sls_due_dt,
		CASE WHEN sls_sales <= 0 OR sls_sales is null OR sls_sales != ABS(sls_price)*sls_quantity
		THEN ABS(sls_price)*sls_quantity
			 ELSE sls_sales
		END sls_sales,
		sls_quantity,
		CASE WHEN sls_price < 0  THEN ABS(sls_price)
			 WHEN sls_price is null THEN sls_sales/sls_quantity
			 ELSE sls_price
		END sls_price
		FROM bronze.crm_sales_details
		SET @end_time = GETDATE();
		PRINT'Duration For Loading :'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'Seconds'
		---------------------------------------------------------------------------------------------------------------------------
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Loading ERP Tables'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		SET @start_time = GETDATE();
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Truncating the Table: silver.erp_cust_az12'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Inserting Data into: silver.erp_cust_az12'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
		SELECT CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
				 ELSE cid
				END cid,
		CASE WHEN  bdate < '1930-01-01' or bdate > GETDATE() THEN NULL
			  ELSE bdate
		END bdate,
		CASE  WHEN gen in ('F','Female') THEN 'Female'
			   WHEN gen in ('M','Male') THEN 'Male'
			   ELSE 'unknown'
		END gen
		FROM bronze.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT'Duration For Loading :'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'Seconds'
		--------------------------------------------------------------------------------------------------------------------------
       SET @start_time = GETDATE();
	    PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Truncating the Table: silver.erp_loc_a101'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Inserting Data into: silver.erp_loc_a101'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		INSERT INTO silver.erp_loc_a101(cid,cntry)
		SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE WHEN TRIM(cntry) IN ('US','USA','United States') THEN 'United States'
			 WHEN TRIM(cntry) ='DE' THEN 'Germany'
			 WHEN TRIM(cntry) = ' ' OR TRIM(cntry) is NULL THEN 'unknown'
			 ELSE TRIM(cntry)
		END cntry

		FROM bronze.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT'Duration For Loading :'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'Seconds'
        ---------------------------------------------------------------------------------------------------------------------------
		SET @start_time = GETDATE();
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Truncating the Table: silver.erp_px_cat_g1v2'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		PRINT'Inserting Data into: silver.erp_px_cat_g1v2'
		PRINT'--------------------------------------------------------------------------------------------------------------------'
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT * 
		FROM bronze.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT'Duration For Loading :'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +'Seconds'
		PRINT'===================================================================================================================='
		PRINT'Loading Silver Layer is completed'
   END TRY
   BEGIN CATCH
   PRINT'ERROR During Loading Silver Layer';
   PRINT'ERROR MESSAGE' + error_message();
   END CATCH
SET @batch_end_time = GETDATE();
PRINT'Duration For Loading All Batch :'+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) +'Seconds'
END
