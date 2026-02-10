-------------------------------------------------------------------------------
-- Stored Procedure: Load Silver Layer
-------------------------------------------------------------------------------
-- Purpose:
--    This procedure automates the Data Cleansing and Transformation process 
--    moving data from Bronze (Raw) to Silver (Standardized) layer.
--
-- Logic & Transformations Applied:
--    1. De-duplication: Using ROW_NUMBER() to keep only the latest customer records.
--    2. Standardization: Standardizing Gender, Marital Status, and Country names.
--    3. String Cleaning: Trimming spaces and fixing keys using SUBSTRING & REPLACE.
--    4. Data Validation: Handling invalid dates (e.g., future birthdays) and nulls.
--    5. Business Logic: 
--       - Implementing SCD Type 2 logic for products (prd_end_dt calculation).
--       - Re-calculating/validating Sales and Price to ensure data consistency.
--    6. Data Integrity: Converting legacy integer dates into proper SQL DATE types.
-------------------------------------------------------------------------------

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	TRUNCATE TABLE silver.crm_cust_info;
	INSERT INTO silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firsrname,
		cst_lastname,
		cst_gndr,
		cst_marital_status,
		cst_create_date)

	SELECT 
		cst_id,
		cst_key,
		TRIM (cst_firsrname) AS cst_firstname,
		TRIM (cst_lastname) AS cst_lastname,
		CASE
			WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'male'
			WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'female'
			ELSE 'n/a'
		END AS cst_gndr,
		CASE
			WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'married'
			WHEN UPPER(TRIM(cst_marital_status)) = 's' THEN 'singel'
			ELSE 'n/a'
		END  AS cst_marital_status,
		cst_create_date
	FROM (
		SELECT * ,
		ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC ) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
	)t WHERE flag_last = 1;

	TRUNCATE TABLE silver.crm_prd_info;
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt 
		)

	SELECT
		prd_id,
		REPLACE (SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING (prd_key, 7, LEN (prd_key)) AS prd_key,
		prd_nm,
		ISNULL( prd_cost,0) AS prd_cost,
		CASE UPPER (TRIM (prd_line))
			WHEN 'M' THEN 'Mountain' 
			WHEN 'R' THEN 'Road'
			WHEN 'S' THEN 'Other sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		prd_start_dt,
		DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS prd_end_dt 
	FROM bronze.crm_prd_info;

	TRUNCATE TABLE silver.crm_sales_details;
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
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN (sls_order_dt) != 8 THEN NULL
			ELSE CAST(sls_order_dt AS DATE)
		END AS sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN (sls_ship_dt) != 8 THEN NULL
			ELSE CAST(sls_ship_dt AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN (sls_due_dt) != 8 THEN NULL
			ELSE CAST(sls_due_dt AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales = NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS (sls_price)
				THEN sls_quantity * ABS (sls_price)
			ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
			CASE WHEN sls_price = NULL OR sls_price <=0
				THEN sls_quantity / NULLIF (sls_sales,0)
			ELSE sls_price
		END AS sls_price
	FROM bronze.crm_sales_details;

	TRUNCATE TABLE silver.erp_cust_az12;
	INSERT INTO silver.erp_cust_az12 (
		cid,
		bdate,
		gen
	)
	SELECT 
		CASE WHEN cid like 'NAS%' THEN SUBSTRING (cid, 4, LEN (cid))
			ELSE cid
		END AS cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
			else 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;

	TRUNCATE TABLE silver.erp_loc_a101;
	INSERT INTO silver.erp_loc_a101
		(cid,cntry)
	SELECT 
		REPLACE(cid, '-', '' ) cid,
		CASE WHEN TRIM (cntry)  = 'DE' THEN 'germany'
			 WHEN TRIM (cntry)  IN ('US','USA')  THEN 'United states'
			 WHEN TRIM (cntry) = '' OR cntry IS NULL  THEN 'n/a'
			 ELSE TRIM (cntry)
		END AS cntry
	FROM bronze.erp_loc_a101;

	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2
		(id, cat, subcat, maintenance)
	SELECT
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
END;
