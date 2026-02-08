--=====================================================
-- Quality Checks
--=====================================================

-------------------------------------------------------
-- Bronze Layer: crm_cust_info Table
-------------------------------------------------------
-- Check cst_id (Primary Key) for NULLs and Duplicates
select cst_id ,count(*)
from bronze.crm_cust_info group by cst_id 
having count(cst_id )>1 or cst_id is null
---------------------------------------------------------
-- If duplicates exist, investigate a specific cst_id
-- Then decide the best handling approach
-- (Window Function using ROW_NUMBER)
--------------------------------------------------------------------------------------------------------------
--Check Unwanted Spaces
select * from bronze.crm_cust_info where cst_firstname != trim(cst_firstname) --this means there are spaces in first name if had a value
select * from bronze.crm_cust_info where cst_lastname != trim(cst_lastname) --this means there are spaces in last name if had a value
--------------------------------------------------------------------------------------------------------------------------------------------
--Data Standardization & Consistency
select distinct cst_gndr from bronze.crm_cust_info
select distinct cst_marital_status from bronze.crm_cust_info
--------------------------------------------------------------------------------------------------------------------------------------------
--=====================================================
-- Bronze Layer: crm_prd_info Table
--=====================================================
-- Check prd_id (Primary Key) for NULLs and Duplicates
select prd_id,count(*) from bronze.crm_prd_info
group by prd_id having count(prd_id)>1 or prd_id is null -- No results indicate no issues
---------------------------------------------------------------------------------------------------------------------------------------------
-- Check prd_key for NULL values
select prd_key from bronze.crm_prd_info where  prd_key is null -- No results indicate no issues
----------------------------------------------------------------------------------------------------------------------------------------------
--Check Unwanted Spaces in prd_nm
select * from bronze.crm_prd_info
where  prd_nm != trim(prd_nm)  --no result --> no unwanted spaces
-----------------------------------------------------------------------------------------------------------------------------------------------
-- Check for NULL or Negative Values in prd_cost
select * from bronze.crm_prd_info where prd_cost < 0 or prd_cost is null --there is 2 values had null 
------------------------------------------------------------------------------------------------------------------------------------------------
--Data Standardization & Consistency
select distinct prd_line from bronze.crm_prd_info
------------------------------------------------------------------------------------------------------------------------------------------------
--Check for Invalid Date Orders
select * from bronze.crm_prd_info where prd_end_dt < prd_start_dt --All start dates is bigger than end dates and this is big problem
------------------------------------------------------------------------------------------------------------------------------------------------
--=====================================================
-- Bronze Layer: crm_sales_details Table
--=====================================================
-----------------------------------------
--Check sls_ord_num unwanted spaces
select * FROM bronze.crm_sales_details where sls_ord_num != trim(sls_ord_num) -- No results indicate no issues
------------------------------------------------------------------------------------------------------------------------------------------------
--Check sls_prd_key is Not in crm_prd_info
select * FROM bronze.crm_sales_details where sls_prd_key not in (select prd_key from silver.crm_prd_info)  -- No results indicate no issues
------------------------------------------------------------------------------------------------------------------------------------------------
--Check sls_cust_id is Not in crm_cust_info
select * FROM bronze.crm_sales_details where sls_cust_id not in (select cst_id from silver.crm_cust_info)  -- No results indicate no issues
------------------------------------------------------------------------------------------------------------------------------------------------
--Check Invalid Dates
select * FROM bronze.crm_sales_details  where len(sls_order_dt) < 8 or len(sls_order_dt) > 8 
or sls_order_dt < 19000101 or sls_order_dt >20500101                  -- Contains zero values and dates with invalid length
select * FROM bronze.crm_sales_details  where len(sls_ship_dt) < 8 or len(sls_ship_dt) > 8 
or sls_ship_dt < 19000101 or sls_ship_dt >20500101                    -- No results indicate no issues
select * FROM bronze.crm_sales_details  where len(sls_due_dt) < 8 or len(sls_due_dt) > 8 
or sls_due_dt < 19000101 or sls_due_dt >20500101                       -- No results indicate no issues
select * FROM bronze.crm_sales_details  where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt or
sls_ship_dt > sls_due_dt  -- No results indicate no issues
------------------------------------------------------------------------------------------------------------------------------------------------
--Check Zeros or Negative or Null Values For sls_sales and sls_price and sls_quantity
select * FROM bronze.crm_sales_details where sls_sales != sls_price*sls_quantity or sls_sales <= 0 or sls_sales is null or sls_quantity <= 0 or sls_quantity is null 
or sls_price <= 0 or  sls_price is null
------------------------------------------------------------------------------------------------------------------------------------------------
--=====================================================
-- Bronze Layer: erp_cust_az12 Table
--=====================================================
-- cid contains extra prefix characters (e.g., 'NAS')
-- These should be removed to align with cst_key
-- Best Approach: SUBSTRING 
-----------------------------------------------------------------------------------------------------------------------------------------------
--check invalid Dates
select * from bronze.erp_cust_az12 where bdate < '1930-01-01' or bdate > GETDATE() --solution --> set future and past data before 1930 to Null
-----------------------------------------------------------------------------------------------------------------------------------------------
--Data Standardization & Consistency
select distinct gen from bronze.erp_cust_az12 --solution --> Normalize gender values and handle unknown cases
------------------------------------------------------------------------------------------------------------------------------------------------
--=====================================================
-- Bronze Layer: erp_loc_a101 Table
--=====================================================
-- Check Customer IDs Not Matching CRM Customer Keys
select cid from bronze.erp_loc_a101 where cid not in (select cst_key from silver.crm_cust_info ) -- Issue detected: Extra '-' character after 'AW'
------------------------------------------------------------------------------------------------------------------------------------------------
--Data Standardization & Consistency
select distinct cntry from bronze.erp_loc_a101          -- Normalize country values and handle unknown cases
------------------------------------------------------------------------------------------------------------------------------------------------
--=====================================================
-- Bronze Layer: erp_px_cat_g1v2 Table
--=====================================================
--check unwanted spaces
select * from bronze.erp_px_cat_g1v2 where id != TRIM(id) or cat != TRIM(cat) or subcat != TRIM(subcat) or maintenance != TRIM(maintenance) -- No results indicate no issues
------------------------------------------------------------------------------------------------------------------------------------------------
--Data Standardization & Consistency
select distinct cat from bronze.erp_px_cat_g1v2           -- No results indicate no issues
select distinct subcat from bronze.erp_px_cat_g1v2        -- No results indicate no issues
select distinct maintenance from bronze.erp_px_cat_g1v2   -- No results indicate no issues
------------------------------------------------------------------------------------------------------------------------------------------------
