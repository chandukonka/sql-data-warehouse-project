/*
============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
============================================================================

Script Purpose:
     This stored procesdure loads data into the 'bronze' schema from external csv files.
     It performs the folowing actions:
     - Truncates the bronze tables before loading data.
     - Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
     None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
     Call bronxe.load_bronze();
=================================================================================
*/
     




CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$

Declare 
batch_start_time Timestamp;
batch_end_time Timestamp;
start_time Timestamp;
end_time Timestamp;



BEGIN

batch_start_time := NOW();


RAISE NOTICE '=====================================================';
RAISE NOTICE 'Loading Bronze Layer';
RAISE NOTICE 'Batch Start Time: %', batch_start_time;
RAISE NOTICE '=====================================================';

RAISE NOTICE '-----------------------------------------------------';
RAISE NOTICE 'Loading CRM Tables';
RAISE NOTICE '-----------------------------------------------------';


Start_time := now();

RAISE NOTICE '>> truncating table: crm_cust_info';
TRUNCATE TABLE bronze.crm_cust_info;

RAISE NOTICE '>> inserting data into: crm_cust_info';
copy bronze.crm_cust_info
from 'E:\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
format csv,
header true,
delimiter ',' );

End_time := NOW();

    RAISE NOTICE '>> Load duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '>> -------------------';


Start_time := now();

RAISE NOTICE '>> truncating table: crm_prd_info';
TRUNCATE TABLE bronze.crm_prd_info;

RAISE NOTICE '>> inserting data into: crm_prd_info';
copy bronze.crm_prd_info
from 'E:\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
format csv,
header true,
delimiter ',' );

End_time := NOW();

    RAISE NOTICE '>> Load duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '>> -------------------';


Start_time := now();

RAISE NOTICE '>> truncating table: crm_sales_details';
TRUNCATE TABLE bronze.crm_sales_details;


RAISE NOTICE '>> inserting data into: crm_sales_details';
copy bronze.crm_sales_details
from 'E:\Data Warehouse Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
format csv,
header true,
delimiter ',' );

End_time := NOW();

    RAISE NOTICE '>> Load duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '>> -------------------';





RAISE NOTICE '-----------------------------------------------------';
RAISE NOTICE 'Loading ERP Tables';
RAISE NOTICE '-----------------------------------------------------';


Start_time := now();

RAISE NOTICE '>> truncating table: erp_cust_az12';
TRUNCATE TABLE bronze.erp_cust_az12;


RAISE NOTICE '>> inserting data into: erp_cust_az12';
copy bronze.erp_cust_az12
from 'E:\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
with (
format csv,
header true,
delimiter ',' );

End_time := NOW();

    RAISE NOTICE '>> Load duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '>> -------------------';


Start_time := now();

RAISE NOTICE '>> truncating table: erp_loc_a101';
TRUNCATE TABLE bronze.erp_loc_a101;



RAISE NOTICE '>> inserting data into: erp_loc_a101';
copy bronze.erp_loc_a101
from 'E:\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
with (
format csv,
header true,
delimiter ',' );


End_time := NOW();

    RAISE NOTICE '>> Load duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '>> -------------------';




Start_time := now();

RAISE NOTICE '>> truncating table: erp_px_cat_g1v2';
TRUNCATE TABLE bronze.erp_px_cat_g1v2;


RAISE NOTICE '>> inserting data into: erp_px_cat_g1v2';
copy bronze.erp_px_cat_g1v2
from 'E:\Data Warehouse Project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
with (
format csv,
header true,
delimiter ',' );

End_time := NOW();

    RAISE NOTICE '>> Load duration: % seconds',
        EXTRACT(EPOCH FROM (end_time - start_time));

    RAISE NOTICE '>> -------------------';

batch_end_time := NOW();
    RAISE NOTICE 'Batch End Time: %', batch_end_time;

    RAISE NOTICE 'Total Batch Duration: % seconds',
 EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));





RAISE NOTICE '=====================================================';
RAISE NOTICE 'Bronze Layer Loading Completed';
RAISE NOTICE '=====================================================';

END;
$$;

 
