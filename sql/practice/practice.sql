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



select * from silver.customers_raw_p where source_file_id='478';
select * from silver.customers_daily where source_file_id='478';



call silver.create_silver_daily_tables();



call silver.ingest_silver_daily_products();
call silver.ingest_silver_raw_products();



call silver.ingest_silver_daily_customers();
call silver.ingest_silver_raw_customers();




call silver.ingest_silver_daily_payments();
call silver.ingest_silver_raw_payments();




call silver.ingest_silver_daily_order_items();
call silver.ingest_silver_raw_order_items();



call silver.ingest_silver_daily_orders();
call silver.ingest_silver_raw_orders();






select * from silver.order_items_raw_p where source_file_id='696';
select * from silver.order_items_daily where source_file_id='696';