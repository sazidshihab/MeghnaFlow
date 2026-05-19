--PLAY GROUND FOR NEW QUERIES AND TESTING-- IGNORE --

==========================================================================================================
--PLAY GROUND--
==========================================================================================================

select table_name,ingestion_id, sum(row_count) from operational_log.bronze_raw_daily_ingest_log
where table_name='customers' group by table_name,2
order by ingestion_id desc ;

select * from operational_log.bronze_ingest_safetynet;

select count(*) from bronze.customers_raw_daily;





======================


call bronze.create_bronze_daily_payments_index();






call bronze.create_bronze_daily_products_index();
call silver.ingest_silver_daily_products()

call bronze.create_bronze_daily_customers_index();
call silver.ingest_silver_daily_customers()

