DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_martial_status VARCHAR(50),
    cst_gender VARCHAR(50),
    cst_create_date DATETIME,
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname,
    cst_martial_status, cst_gender, cst_create_date
)
SELECT
    CASE 
        WHEN cst_id REGEXP '^[0-9]+$' THEN CAST(cst_id AS SIGNED)
        ELSE NULL
    END,

    cst_key,
    TRIM(cst_firstname),
    TRIM(cst_lastname),

    CASE 
        WHEN UPPER(TRIM(cst_martial_status)) IN ('S','SINGLE') THEN 'Single'
        WHEN UPPER(TRIM(cst_martial_status)) IN ('M','MARRIED') THEN 'Married'
        ELSE 'N/A'
    END,

    CASE 
        WHEN UPPER(TRIM(cst_gender)) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(cst_gender)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'N/A'
    END,


    CASE 
        WHEN cst_create_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
        THEN STR_TO_DATE(cst_create_date, '%Y-%m-%d')

        WHEN cst_create_date REGEXP '^[0-9]{8}$'
        THEN STR_TO_DATE(cst_create_date, '%Y%m%d')

        ELSE NULL
    END

FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cst_id 
               ORDER BY cst_create_date DESC
           ) rn
    FROM bronze.crm_cust_info
) t
WHERE rn = 1;

DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    cat_id VARCHAR(50),
    prd_key VARCHAR(50),
    prd_nm VARCHAR(100),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.crm_prd_info (
    prd_id, cat_id, prd_key, prd_nm,
    prd_cost, prd_line, prd_start_dt, prd_end_dt
)
SELECT
    CAST(prd_id AS SIGNED),

    REPLACE(SUBSTRING(prd_key,1,5),'-','_'),
    SUBSTRING(prd_key,7),

    prd_nm,

    CASE 
        WHEN prd_cost REGEXP '^[0-9]+$' THEN CAST(prd_cost AS SIGNED)
        ELSE 0
    END,

    CASE 
        WHEN UPPER(TRIM(prd_line))='M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line))='R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line))='S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line))='T' THEN 'Touring'
        ELSE 'N/A'
    END,

    STR_TO_DATE(prd_start_dt,'%Y-%m-%d'),

    DATE_SUB(
        LEAD(STR_TO_DATE(prd_start_dt,'%Y-%m-%d'))
        OVER(PARTITION BY prd_key ORDER BY prd_start_dt),
        INTERVAL 1 DAY
    )

FROM bronze.crm_prd_info;

DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,

    -- Safe INT
    CASE 
        WHEN sls_cust_id REGEXP '^[0-9]+$'
        THEN CAST(sls_cust_id AS SIGNED)
        ELSE NULL
    END,

    -- Safe DATE
    CASE 
        WHEN sls_order_dt REGEXP '^[0-9]{8}$'
        THEN STR_TO_DATE(sls_order_dt, '%Y%m%d')
        ELSE NULL
    END,

    CASE 
        WHEN sls_ship_dt REGEXP '^[0-9]{8}$'
        THEN STR_TO_DATE(sls_ship_dt, '%Y%m%d')
        ELSE NULL
    END,

    CASE 
        WHEN sls_due_dt REGEXP '^[0-9]{8}$'
        THEN STR_TO_DATE(sls_due_dt, '%Y%m%d')
        ELSE NULL
    END,

    -- Safe SALES calculation
    CASE 
        WHEN sls_sales REGEXP '^[0-9]+$' 
             AND sls_quantity REGEXP '^[0-9]+$'
             AND sls_price REGEXP '^-?[0-9]+$'
             AND CAST(sls_sales AS SIGNED) > 0
        THEN CAST(sls_sales AS SIGNED)

        WHEN sls_quantity REGEXP '^[0-9]+$'
             AND sls_price REGEXP '^-?[0-9]+$'
        THEN CAST(sls_quantity AS SIGNED) * ABS(CAST(sls_price AS SIGNED))

        ELSE NULL
    END,

    -- Safe QUANTITY
    CASE 
        WHEN sls_quantity REGEXP '^[0-9]+$'
        THEN CAST(sls_quantity AS SIGNED)
        ELSE NULL
    END,

    -- Safe PRICE
    CASE 
        WHEN sls_price REGEXP '^-?[0-9]+$'
             AND CAST(sls_price AS SIGNED) > 0
        THEN CAST(sls_price AS SIGNED)

        WHEN sls_sales REGEXP '^[0-9]+$'
             AND sls_quantity REGEXP '^[0-9]+$'
             AND CAST(sls_quantity AS SIGNED) != 0
        THEN CAST(sls_sales AS SIGNED) / CAST(sls_quantity AS SIGNED)

        ELSE NULL
    END

FROM bronze.crm_sales_details;

-- Step 1: Drop table
DROP TABLE IF EXISTS silver.erp_cust_az12;

-- Step 2: Create table
CREATE TABLE silver.erp_cust_az12 (
    cid INT,
    bdate DATE,
    gen VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Step 3: Insert cleaned data
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    -- Safe ID (remove 'NAS' prefix if exists)
    CASE 
        WHEN cid LIKE 'NAS%' 
             AND SUBSTRING(cid,4) REGEXP '^[0-9]+$'
        THEN CAST(SUBSTRING(cid,4) AS SIGNED)

        WHEN cid REGEXP '^[0-9]+$'
        THEN CAST(cid AS SIGNED)

        ELSE NULL
    END,

    -- Safe DATE (avoid future + invalid)
    CASE 
        WHEN bdate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
             AND STR_TO_DATE(bdate,'%Y-%m-%d') <= CURRENT_DATE
        THEN STR_TO_DATE(bdate,'%Y-%m-%d')

        WHEN bdate REGEXP '^[0-9]{8}$'
             AND STR_TO_DATE(bdate,'%Y%m%d') <= CURRENT_DATE
        THEN STR_TO_DATE(bdate,'%Y%m%d')

        ELSE NULL
    END,

    -- Normalize gender
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        ELSE 'N/A'
    END

FROM bronze.erp_cust_az12;

-- Step 1: Drop table
DROP TABLE IF EXISTS silver.erp_loc_a101;

-- Step 2: Create table
CREATE TABLE silver.erp_loc_a101 (
    cid INT,
    cntry VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Step 3: Insert cleaned data
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    -- Safe ID (remove '-' and validate)
    CASE 
        WHEN REPLACE(cid,'-','') REGEXP '^[0-9]+$'
        THEN CAST(REPLACE(cid,'-','') AS SIGNED)
        ELSE NULL
    END,

    -- Normalize country
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
        WHEN cntry IS NULL OR TRIM(cntry) = '' THEN 'N/A'
        ELSE TRIM(cntry)
    END

FROM bronze.erp_loc_a101;

-- Step 1: Drop table
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

-- Step 2: Create table
CREATE TABLE silver.erp_px_cat_g1v2 (
    id INT,
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- Step 3: Insert cleaned data
INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    -- Safe ID
    CASE 
        WHEN id REGEXP '^[0-9]+$'
        THEN CAST(id AS SIGNED)
        ELSE NULL
    END,

    TRIM(cat),
    TRIM(subcat),
    TRIM(maintenance)

FROM bronze.erp_px_cat_g1v2;


select * from silver.crm_prd_info ;