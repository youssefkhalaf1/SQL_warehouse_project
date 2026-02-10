-------------------------------------------------------------------------------
-- Script: Gold Layer Creation (Star Schema / Presentation Layer)
-------------------------------------------------------------------------------
-- Purpose:
--    This script creates the final consumption layer (Gold) using VIEWS.
--    It integrates data from CRM and ERP systems into a business-friendly format.
--
-- Logic Applied:
--    1. Dimensional Modeling: Creating Dimensions (Customers, Products) and Fact (Orders).
--    2. Master Data Management: Merging CRM and ERP attributes (e.g., Customer Gender & Location).
--    3. Surrogate Keys: Generating 'Customer_Key' and 'Product_Key' using ROW_NUMBER().
--    4. Data Filtering: 'dim_products' only includes active products (prd_end_dt IS NULL).
--    5. Business Friendly Naming: Mapping technical codes (cst_id) to readable aliases (Customer_ID).
--
-- Note: These views act as the direct source for BI reporting and dashboards.
-------------------------------------------------------------------------------

CREATE VIEW gold.dim_coustmers AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) AS Customer_Key,
	ci.cst_id             AS Customer_ID,
	ci.cst_key            AS Customer_Number,
	ci.cst_firsrname      AS Customer_Firstname,
	ci.cst_lastname       AS Customer_Lastname,
	ci.cst_marital_status AS Customer_Marital_Status,
	CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr		-- CRM is a master of gender 
		 ELSE COALESCE (ca.gen, 'n/a')
	END AS Customer_gender,
	cl.cntry              AS Customer_Cuntry,
	ca.bdate              AS Customer_Birth_Date,
	ci.cst_create_date    AS Customer_Create_Date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl
ON ci.cst_key = cl.cid 
GO
CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY pin.prd_start_dt, pin.prd_key) As Product_key,
	pin.prd_id         AS Product_ID,
	pin.prd_key        AS Product_Number,
	pin.prd_nm         AS Product_Name,
	pin.prd_cost       AS Product_Cost,
	pin.prd_line       AS Product_Line,
	pin.cat_id         AS Category_ID,
	pc.cat             AS Category,
	pc.subcat          AS Sub_Category,
	pc.maintenance,
	pin.prd_start_dt   AS Product_start_date
FROM silver.crm_prd_info pin
LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pin.cat_id = pc.id
WHERE pin.prd_end_dt IS NULL 
GO
CREATE VIEW gold.fact_orders AS
SELECT 
	sd.sls_ord_num     AS Order_Number,
	dp.Product_Number,
	dc.Customer_Number,
	sd.sls_order_dt    AS Order_Date,
	sd.sls_ship_dt     AS Shiping_Date,
	sd.sls_due_dt      AS due_Date,
	sd.sls_sales       AS Sales_Amount,
	sd.sls_quantity    AS quantity,
	sd.sls_price       AS unit_price 
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products dp
	ON sd.sls_prd_key = dp.Product_Number
LEFT JOIN gold.dim_coustmers dc
	ON sd.sls_cust_id = dc.Customer_Number
