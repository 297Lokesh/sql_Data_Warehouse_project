-- ============================================================
-- CRM CUSTOMER INFO
-- ============================================================
SET @start_time = NOW();

TRUNCATE TABLE bronze.crm_cust_info;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/CRM/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@c1, @c2, @c3, @c4, @c5, @c6, @c7)
SET
cst_id = @c1,
cst_key = @c2,
cst_firstname = @c3,
cst_lastname = @c4,
cst_martial_status = @c5,
cst_gender = @c6,
cst_create_date = @c7;

SET @end_time = NOW();

SELECT 'crm_cust_info' AS table_name,
TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS execution_time_seconds;



-- ============================================================
-- CRM PRODUCT INFO
-- ============================================================
SET @start_time = NOW();

TRUNCATE TABLE bronze.crm_prd_info;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/CRM/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@c1, @c2, @c3, @c4, @c5, @c6, @c7)
SET
prd_id = NULLIF(TRIM(@c1), ''),
prd_key = NULLIF(TRIM(@c2), ''),
prd_nm = NULLIF(TRIM(@c3), ''),
prd_cost = NULLIF(TRIM(@c4), ''),
prd_line = NULLIF(TRIM(@c5), ''),
prd_start_dt = NULLIF(TRIM(@c6), ''),
prd_end_dt = NULLIF(TRIM(@c7), '');

SET @end_time = NOW();

SELECT 'crm_prd_info' AS table_name,
TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS execution_time_seconds;



-- ============================================================
-- CRM SALES DETAILS
-- ============================================================
SET @start_time = NOW();

TRUNCATE TABLE bronze.crm_sales_details;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/CRM/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@c1, @c2, @c3, @c4, @c5, @c6, @c7, @c8, @c9)
SET
sls_ord_num = NULLIF(TRIM(@c1), ''),
sls_prd_key = NULLIF(TRIM(@c2), ''),
sls_cust_id = NULLIF(TRIM(@c3), ''),
sls_order_dt = NULLIF(TRIM(@c4), ''),
sls_ship_dt = NULLIF(TRIM(@c5), ''),
sls_due_dt = NULLIF(TRIM(@c6), ''),
sls_sales = NULLIF(TRIM(@c7), ''),
sls_quantity = NULLIF(TRIM(@c8), ''),
sls_price = NULLIF(TRIM(@c9), '');

SET @end_time = NOW();

SELECT 'crm_sales_details' AS table_name,
TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS execution_time_seconds;



-- ============================================================
-- ERP CUSTOMER
-- ============================================================
SET @start_time = NOW();

TRUNCATE TABLE bronze.erp_cust_az12;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/ERP/cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@c1, @c2, @c3)
SET
cid = NULLIF(TRIM(@c1), ''),
bdate = NULLIF(TRIM(@c2), ''),
gen = NULLIF(TRIM(@c3), '');

SET @end_time = NOW();

SELECT 'erp_cust_az12' AS table_name,
TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS execution_time_seconds;



-- ============================================================
-- ERP LOCATION
-- ============================================================
SET @start_time = NOW();

TRUNCATE TABLE bronze.erp_loc_a101;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/ERP/loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@c1, @c2)
SET
cid = NULLIF(TRIM(@c1), ''),
cntry = NULLIF(TRIM(@c2), '');

SET @end_time = NOW();

SELECT 'erp_loc_a101' AS table_name,
TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS execution_time_seconds;



-- ============================================================
-- ERP PRODUCT CATEGORY
-- ============================================================
SET @start_time = NOW();

TRUNCATE TABLE bronze.erp_px_cat_g1v2;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/datasets/ERP/px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(@c1, @c2, @c3, @c4)
SET
id = NULLIF(TRIM(@c1), ''),
cat = NULLIF(TRIM(@c2), ''),
subcat = NULLIF(TRIM(@c3), ''),
maintenance = NULLIF(TRIM(@c4), '');

SET @end_time = NOW();

SELECT 'erp_px_cat_g1v2' AS table_name,
TIMESTAMPDIFF(SECOND, @start_time, @end_time) AS execution_time_seconds;