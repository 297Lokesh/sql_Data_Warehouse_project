/*
===============================================================================
SILVER LAYER CREATION SCRIPT (MySQL)
===============================================================================
Purpose:
    - Transform raw data (Bronze) → Clean data (Silver)
    - Perform typecasting, validation, and basic cleaning
    - Prepare data for Gold layer (analytics)

Key Operations:
    - Drop & recreate tables
    - Convert datatypes (VARCHAR → INT / DATE)
    - Handle invalid values → NULL
===============================================================================
*/



DROP TABLE IF EXISTS silver.crm_cust_info;

CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_martial_status VARCHAR(50),
    cst_gender VARCHAR(50),
    cst_create_date DATE,
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_martial_status,
    cst_gender,
    cst_create_date
)
SELECT
    -- INT conversion
    CASE 
        WHEN cst_id REGEXP '^[0-9]+$' THEN CAST(cst_id AS SIGNED)
        ELSE NULL
    END,

    cst_key,
    cst_firstname,
    cst_lastname,
    cst_martial_status,
    cst_gender,

    -- DATE conversion
    CASE 
        WHEN cst_create_date REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN STR_TO_DATE(cst_create_date, '%Y-%m-%d')
        ELSE NULL
    END

FROM bronze.crm_cust_info;

DROP TABLE IF EXISTS silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost INT,
    prd_line VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME,
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
); 


INSERT INTO silver.crm_prd_info (
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    -- INT conversion
    CASE 
        WHEN prd_id REGEXP '^[0-9]+$' THEN CAST(prd_id AS SIGNED)
        ELSE NULL
    END,

    prd_key,
    prd_nm,

    CASE 
        WHEN prd_cost REGEXP '^[0-9]+$' THEN CAST(prd_cost AS SIGNED)
        ELSE NULL
    END,

    prd_line,

    -- DATETIME conversion
    CASE 
        WHEN prd_start_dt REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN STR_TO_DATE(prd_start_dt, '%Y-%m-%d')
        ELSE NULL
    END,

    CASE 
        WHEN prd_end_dt REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN STR_TO_DATE(prd_end_dt, '%Y-%m-%d')
        ELSE NULL
    END

FROM bronze.crm_prd_info;




select count(*) from bronze.crm_sales_details;
select count(*) from silver.crm_sales_details;
DROP TABLE IF EXISTS silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
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

    -- INT conversion
    CASE 
        WHEN sls_cust_id REGEXP '^[0-9]+$' THEN CAST(sls_cust_id AS SIGNED)
        ELSE NULL
    END,

    -- DATE as INT (epoch seconds)
    CASE 
        WHEN sls_order_dt REGEXP '^[0-9]+$' THEN CAST(sls_order_dt AS SIGNED)
        ELSE NULL
    END,

    CASE 
        WHEN sls_ship_dt REGEXP '^[0-9]+$' THEN CAST(sls_ship_dt AS SIGNED)
        ELSE NULL
    END,

    CASE 
        WHEN sls_due_dt REGEXP '^[0-9]+$' THEN CAST(sls_due_dt AS SIGNED)
        ELSE NULL
    END,

    -- Numeric conversions
    CASE 
        WHEN sls_sales REGEXP '^[0-9]+$' THEN CAST(sls_sales AS SIGNED)
        ELSE NULL
    END,

    CASE 
        WHEN sls_quantity REGEXP '^[0-9]+$' THEN CAST(sls_quantity AS SIGNED)
        ELSE NULL
    END,

    CASE 
        WHEN sls_price REGEXP '^[0-9]+$' THEN CAST(sls_price AS SIGNED)
        ELSE NULL
    END

FROM bronze.crm_sales_details;


DROP TABLE IF EXISTS silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
    cid INT,
    bdate DATE,
    gen VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    -- INT conversion
    CASE 
        WHEN cid REGEXP '^[0-9]+$' THEN CAST(cid AS SIGNED)
        ELSE NULL
    END,

    -- DATE conversion
    CASE 
        WHEN bdate REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN STR_TO_DATE(bdate, '%Y-%m-%d')
        ELSE NULL
    END,

    gen

FROM bronze.erp_cust_az12;

select count(*) from bronze.erp_cust_az12 ;
select count(*) from silver.erp_cust_az12 ;

DROP TABLE IF EXISTS silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
    cid INT,
    cntry VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    -- INT conversion
    CASE 
        WHEN cid REGEXP '^[0-9]+$' THEN CAST(cid AS SIGNED)
        ELSE NULL
    END,

    cntry

FROM bronze.erp_loc_a101;

select count(*) from bronze.erp_loc_a101 ;
select count(*) from silver.erp_loc_a101 ;


DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
    id INT,
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50),
    dwh_created_date DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    -- INT conversion
    CASE 
        WHEN id REGEXP '^[0-9]+$' THEN CAST(id AS SIGNED)
        ELSE NULL
    END,

    cat,
    subcat,
    maintenance

FROM bronze.erp_px_cat_g1v2;

select count(*) from bronze.erp_px_cat_g1v2 ;
select count(*) from silver.erp_px_cat_g1v2 ;