-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@bronze

====================
--PG tuning START--
===================

/*
Performance tuning : Permanant change.
*/
    ALTER SYSTEM SET shared_buffers = '2GB'; 
    -- Planner & I/O
    ALTER SYSTEM SET random_page_cost = 1.1;
    ALTER SYSTEM SET effective_cache_size = '6GB';
    -- Parallelism
    ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
    ALTER SYSTEM SET max_parallel_workers = 8;
    -- Write Performance (Heavy Lifting)
    ALTER SYSTEM SET synchronous_commit = OFF;
    ALTER SYSTEM SET max_wal_size = '8GB';
    ALTER SYSTEM SET min_wal_size = '1GB';
    ALTER SYSTEM SET checkpoint_completion_target = 0.9;
    -- Maintenance & Operations
    ALTER SYSTEM SET maintenance_work_mem = '1GB'; /*1GB*/
    ALTER SYSTEM SET work_mem = '512MB'; /*128MB*/
    -- Apply (Note: shared_buffers requires a full DB restart)
    SELECT pg_reload_conf();

SELECT name, setting, unit, source, sourcefile
FROM pg_settings 
WHERE source NOT IN ('default', 'override');

/*
END
*/
=======================
--PG tuning END--
=======================


------------------------------------------------------------------------------------------------------------


=========================================
--PROCEDURES (Used by Python)-- START
=========================================

/*Bronze main table ingest + safetynet log table creation*/
/*Those procedures will be call from bronze_daily_to_bronze_main.py and data will load from daily table to main bronze table. */


create or replace procedure bronze.brone_daily_to_bronze_main_customers()
language plpgsql
as $$

DECLARE
local_bronze_row_count int;
first_time timestamp;
index_first_time timestamp;
index_time interval;
insert_time interval;
bronze_copy_time interval;

begin 


        /*Dropping silver daily temporary table before inserting new data to bronze*/
        call silver.silver_daily_table_drop();

        bronze_copy_time :=(select max(executing_time) from operational_log.bronze_raw_daily_ingest_log where table_name = 'customers' and ingestion_id = (select ingestion_id from operational_log.ingestion_id))* interval '1 second';
        



        /*
        IMPORTING DATA INTO THE BRONZE LAYER (customers_raw)
        */

        RAISE NOTICE 'Step 1: Customer data ingested successfully using Python Multithreading. Now creating index for it....';

        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.customers_raw_daily(created_at_bronze, customer_id)include(name, signup_date, source_file_id);
        index_time := clock_timestamp()-index_first_time;

        raise notice '->Index created successfully';

        RAISE NOTICE 'Step 2: Starting to ingest Customer data to main table....';


        first_time := clock_timestamp();
        insert into bronze.customers_raw
        select * from bronze.customers_raw_daily;
        insert_time := clock_timestamp()-first_time;


        get diagnostics local_bronze_row_count = row_count;

        raise notice 'Step 3: Customer data ingested successfully to main table...';

        --Inserting log and safeteynet data --
        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_row_count, silver_daily_row_count, silver_main_row_count, null_pk_count, other_null_count, duplicate_count,  future_past_count, negative_count, quarantine_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_insert_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'customers',
        local_bronze_row_count,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_copy_time,
        index_time,
        insert_time,
        (insert_time + (bronze_copy_time) +  index_time),
        current_date
        );

end;
$$;

--call bronze.brone_daily_to_bronze_main_customers();



create or replace procedure bronze.brone_daily_to_bronze_main_order_items()
language plpgsql
as $$

DECLARE
local_bronze_row_count int;
first_time timestamp;
index_first_time timestamp;
index_time interval;
insert_time interval;
bronze_copy_time interval;

begin

         /*
        IMPORTING DATA INTO THE BRONZE LAYER (order_items_raw)
        */

        bronze_copy_time := (select max(executing_time) from operational_log.bronze_raw_daily_ingest_log where table_name = 'order_items' and ingestion_id = (select ingestion_id from operational_log.ingestion_id))* interval '1 second';
        RAISE NOTICE 'Step 1: Order Item data ingested successfully using Python Multithreading. Now creating index for it....';

        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.order_items_raw_daily(created_at_bronze, order_id, product_id)include( quantity, unit_price, total, source_file_id);
        index_time := clock_timestamp()-index_first_time;
        
        raise notice '->Index created successfully';

        RAISE NOTICE 'Step 2: Starting to ingest Order Item data into main table...';

        first_time := clock_timestamp();
        insert into bronze.order_items_raw
        select * from bronze.order_items_raw_daily;
        insert_time := clock_timestamp()-first_time;
        get diagnostics local_bronze_row_count = row_count;

        raise notice 'Step 3: Order Item data ingested successfully to main table...';

        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_row_count, silver_daily_row_count, silver_main_row_count, null_pk_count, other_null_count, duplicate_count,  future_past_count, negative_count, quarantine_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_insert_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'order_items',
        local_bronze_row_count,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_copy_time,
        index_time,
        insert_time,
        (insert_time + (bronze_copy_time) +  index_time),
        current_date
        );

end;
$$;

--call bronze.brone_daily_to_bronze_main_order_items();







create or replace procedure bronze.brone_daily_to_bronze_main_payments()

language plpgsql
as $$

DECLARE
local_bronze_row_count int;
first_time timestamp;
index_first_time timestamp;
index_time interval;
insert_time interval;
bronze_copy_time interval;

begin

            /*
            IMPORTING DATA INTO THE BRONZE LAYER (payments_raw)
            */

            bronze_copy_time := (select max(executing_time) from operational_log.bronze_raw_daily_ingest_log where table_name = 'payments' and ingestion_id = (select ingestion_id from operational_log.ingestion_id))* interval '1 second';
            
            RAISE NOTICE 'Step 1: Payment data ingested successfully using Python Multithreading. Now creating index for it....';
    
            index_first_time := clock_timestamp();
            CREATE INDEX ON bronze.payments_raw_daily(created_at_bronze, payment_id, order_id)include( method, order_date, total, payment_date, source_file_id);
            index_time := clock_timestamp()-index_first_time;
            
            raise notice '->Index created successfully';
    
            RAISE NOTICE 'Step 2: Starting to ingest Payment data into main table...';
    
            first_time := clock_timestamp();
            insert into bronze.payments_raw
            select * from bronze.payments_raw_daily;
            insert_time := clock_timestamp()-first_time;
            get diagnostics local_bronze_row_count = row_count;
    
            raise notice 'Step 3: Payment data ingested successfully to main table...';
    
            insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_row_count, silver_daily_row_count, silver_main_row_count, null_pk_count, other_null_count, duplicate_count,  future_past_count, negative_count, quarantine_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_insert_executing_time,total_bronze_process_executing_time,created_at)
            values((select ingestion_id from operational_log.ingestion_id),
            'payments',
            local_bronze_row_count,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            bronze_copy_time,
            index_time,
            insert_time,
            (insert_time + (bronze_copy_time) +  index_time),
            current_date
            );

end;
$$;

--call bronze.brone_daily_to_bronze_main_payments();



create or replace procedure bronze.brone_daily_to_bronze_main_orders()

language plpgsql
as $$

DECLARE
local_bronze_row_count int;
first_time timestamp;
index_first_time timestamp;
index_time interval;
insert_time interval;
bronze_copy_time interval;

begin 

       /*
        IMPORTING DATA INTO THE BRONZE LAYER (orders_raw)
        */

        bronze_copy_time := (select max(executing_time) from operational_log.bronze_raw_daily_ingest_log where table_name = 'orders' and ingestion_id = (select ingestion_id from operational_log.ingestion_id))* interval '1 second';

        RAISE NOTICE 'Step 1: Order data ingested successfully using Python Multithreading. Now creating index for it....';


        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.orders_raw_daily(created_at_bronze, order_id, customer_id)include( order_date, status, source_file_id);
        index_time := clock_timestamp()-index_first_time;

        raise notice '->Index created successfully';

        RAISE NOTICE 'Step 2: Starting to ingest Order data to main table....';

        first_time := clock_timestamp();
        insert into bronze.orders_raw
        select * from bronze.orders_raw_daily;
        insert_time := clock_timestamp()-first_time;
        get diagnostics local_bronze_row_count = row_count;

        raise notice 'Step 3: Order data ingested successfully to main table...';

        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_row_count, silver_daily_row_count, silver_main_row_count, null_pk_count, other_null_count, duplicate_count,  future_past_count, negative_count, quarantine_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_insert_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'orders',
        local_bronze_row_count,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_copy_time,
        index_time,
        insert_time,
        (insert_time + (bronze_copy_time) +  index_time),
        current_date
        );

end;
$$;

--call bronze.brone_daily_to_bronze_main_orders();




create or replace procedure bronze.brone_daily_to_bronze_main_products()

language plpgsql
as $$    

DECLARE
local_bronze_row_count int;
first_time timestamp;
index_first_time timestamp;
index_time interval;
insert_time interval;
bronze_copy_time interval;

begin 

       /*
        IMPORTING DATA INTO THE BRONZE LAYER (products_raw)
        */
        bronze_copy_time := (select max(executing_time) from operational_log.bronze_raw_daily_ingest_log where table_name = 'products' and ingestion_id = (select ingestion_id from operational_log.ingestion_id))* interval '1 second';
        
        RAISE NOTICE 'Step 1: Product data ingested successfully using Python Multithreading. Now creating index for it....';


        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.products_raw_daily(created_at_bronze, product_id)include( name, category, price, source_file_id);
        index_time := clock_timestamp()-index_first_time;

        raise notice '->Index created successfully';

        RAISE NOTICE 'Step 2: Starting to ingest Product data to main table....';

        first_time := clock_timestamp();
        insert into bronze.products_raw
        select * from bronze.products_raw_daily;
        insert_time := clock_timestamp()-first_time;
        get diagnostics local_bronze_row_count = row_count;

        raise notice 'Step 3: Product data ingested successfully to main table...';

        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_row_count, silver_daily_row_count, silver_main_row_count, null_pk_count, other_null_count, duplicate_count,  future_past_count, negative_count, quarantine_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_insert_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'products',
        local_bronze_row_count,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_copy_time,
        index_time,
        insert_time,
        (insert_time + (bronze_copy_time) +  index_time),
        current_date
        );

end;
$$;

--call bronze.brone_daily_to_bronze_main_products();

=====================
--BRONZE IMPORT-- END
=====================



------------------------------------------------------------------------------------------------------------




========================
--PG Activity and WAL monitoring-- START
========================



select datname,pid, (now()-query_start)::time as time_,query as "query/command", state as "state/bytes_total" , backend_type as "backend_type/tuples_processed" from pg_stat_activity where state='active' and query not like '%select datname%' 
union all
select datname, pid,null::time, 
command as "que
ry/comman
d",bytes_total::text as "state/
bytes_tot
al",tuples_processed::text as "backend_type/tuples_processed" from   pg_stat_progress_copy;


SELECT * FROM pg_stat_wal;


select * from pg_stat_activity where state='active';
select * from information_schema.tables where table_name like '%stat%';

========================================================
--PG Activity and WAL monitoring-- END
========================================================




------------------------------------------------------------------------------------------------------------




/*

COPY silver.customers(customer_id, name, signup_date) 
TO PROGRAM 'cut -d "," -f 1,2,3 > "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/customers/customers_2026-05-04.csv"' 
WITH (FORMAT CSV, HEADER);

copy silver.order_items(order_id,product_id,quantity,unit_price,total) 
TO PROGRAM ' cut -d "," -f 1,2,3,4,5 > "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/order_items/order_items_2026-05-04.csv"'
 WITH (FORMAT CSV, HEADER);
copy silver.orders(order_id,customer_id,order_date,status) 
TO PROGRAM 'cut -d "," -f 1,2,3,4 > "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/orders/orders.csv_2026-05-04.csv"'
 WITH (FORMAT CSV, HEADER);
copy silver.payments(payment_id,method,order_id,order_date,total,payment_date) 
TO PROGRAM 'cut -d "," -f 1,2,3,4,5,6 > "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/payments/payments_2026-05-04.csv"'
 WITH (FORMAT CSV, HEADER);
copy silver.products(product_id,name,category,price) 
TO PROGRAM 'cut -d "," -f 1,2,3,4 > "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/products/products_2026-05-04.csv"' WITH (FORMAT CSV, HEADER);

*/
