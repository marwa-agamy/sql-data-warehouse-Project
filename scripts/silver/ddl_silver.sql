--Create Tables in Silver Layer 
-- dwh_create_date:
-- ----------------
-- Stores the timestamp when the record is loaded into the Silver layer
-- Used for data lineage, auditing, and troubleshooting data loads
-- This column does NOT represent source system creation time

--------------------------------------------------------------------------------------
--Create Tables for each source_crm file
--Create cust_info Table
CREATE TABLE silver.crm_cust_info(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
)
--Create prd_info Table
CREATE TABLE silver.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost DECIMAL(10,2),
prd_line NVARCHAR(50),
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
)
--Create sales_details Table
CREATE TABLE silver.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales DECIMAL(10,2),
sls_quantity INT,
sls_price DECIMAL(10,2),
dwh_create_date DATETIME2 DEFAULT GETDATE()
)
------------------------------------------
--Create Tables for each source_erp file
--Create LOC_A101 Table
CREATE TABLE silver.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
)
--Create CUST_AZ12 Table
CREATE TABLE silver.erp_cust_az12(
cid NVARCHAR(50),
bdate DATE,
gen NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
)
--Create PX_CAT_G1V2 Table
CREATE TABLE silver.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
)
