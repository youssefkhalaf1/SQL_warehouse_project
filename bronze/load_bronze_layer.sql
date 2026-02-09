-- Stored Procedure Description
--This stored procedure is responsible for loading raw data into the Bronze layer of the data warehouse.

--It performs the following steps:

--Truncates existing data from all Bronze tables to ensure a clean reload.

--Loads fresh data from multiple CSV source files using BULK INSERT.

--Handles data from both CRM and ERP source systems.

--Uses a TRY / CATCH block to capture and report any errors that occur during the loading process.



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
		PRINT '===================================';
		PRINT 'bronze loaded';
		PRINT '===================================';


		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
			FROM 'D:\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
			WITH (
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK
			);

		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
			FROM 'D:\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
			WITH (
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK
				);

		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
			FROM 'D:\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
			WITH (
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK
				);

		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
			FROM 'D:\sql-data-warehouse-project-main\datasets\source_erp\cust_az12.csv'
			WITH (
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK
				);

		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
			FROM 'D:\sql-data-warehouse-project-main\datasets\source_erp\loc_a101.csv'
			WITH (
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK
				);

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
			FROM 'D:\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
			WITH (
				FIRSTROW = 2 ,
				FIELDTERMINATOR = ',',
				TABLOCK
				);
	END TRY 
	BEGIN CATCH
		PRINT 'ERROR_load_bronze';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	END CATCH
END
