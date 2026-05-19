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
    -- Write Performance (Heavy Lifting)
    ALTER SYSTEM SET synchronous_commit = OFF;
    ALTER SYSTEM SET max_wal_size = '8GB';
    ALTER SYSTEM SET min_wal_size = '1GB';
    ALTER SYSTEM SET checkpoint_completion_target = 0.9;
    -- Maintenance & Operations
    ALTER SYSTEM SET maintenance_work_mem = '1GB'; /*1GB*/
    ALTER SYSTEM SET work_mem = '512MB'; /*128MB*/
    -- Apply (Note: shared_buffers requires a full DB restart)
    ALTER SYSTEM SET max_parallel_maintenance_workers = 4;
    ALTER SYSTEM SET max_parallel_workers = 12;  

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
--PROCEDURES (Used by Python)--((Creating INDEX & INSERT TO SAFETYNET)) START
=========================================

/*Bronze main table ingest + safetynet log table creation*/
/*Those procedures will be call from bronze_daily_to_bronze_main.py and data will load from daily table to main bronze table. */


create or replace procedure bronze.create_bronze_daily_customers_index()
language plpgsql
as $$

DECLARE
bronze_main_copy_time interval;
bronze_daily_copy_time interval;
index_first_time timestamp;
index_time interval;

begin 

        /*
        IMPORTING DATA INTO THE BRONZE LAYER (customers_raw)
        */

        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.customers_raw_daily( customer_id,created_at_bronze);
        index_time := clock_timestamp()-index_first_time;

        
        bronze_daily_copy_time := (select max(executing_time) from operational_log.bronze_ingest_log where table_name='customers' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily') * interval '1 second';
        bronze_main_copy_time :=  (select max(executing_time) from operational_log.bronze_ingest_log where table_name='customers' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw') * interval '1 second';
        
        --Inserting log and safeteynet data --
        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count, silver_daily_row_count, silver_main_row_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'customers',
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='customers' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily' ),
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='customers' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw'),
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_daily_copy_time ,
        index_time ,
        bronze_main_copy_time,
        bronze_daily_copy_time + index_time + bronze_main_copy_time,
        current_date
        );

end;
$$;

--call bronze.create_bronze_daily_customers_index();



create or replace procedure bronze.create_bronze_daily_order_items_index()
language plpgsql
as $$

DECLARE
bronze_main_copy_time interval;
bronze_daily_copy_time interval;
index_first_time timestamp;
index_time interval;


begin


        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.order_items_raw_daily( order_id, product_id, created_at_bronze);
        index_time := clock_timestamp()-index_first_time;

        
        bronze_daily_copy_time := (select max(executing_time) from operational_log.bronze_ingest_log where table_name='order_items' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily') * interval '1 second';
        bronze_main_copy_time :=  (select max(executing_time) from operational_log.bronze_ingest_log where table_name='order_items' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw') * interval '1 second';



        --Inserting log and safeteynet data --
        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count, silver_daily_row_count, silver_main_row_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'order_items',
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='order_items' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily' ),
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='order_items' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw'),
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_daily_copy_time ,
        index_time ,
        bronze_main_copy_time,
        bronze_daily_copy_time + index_time + bronze_main_copy_time,
        current_date
        );

end;
$$;

--call bronze.create_bronze_daily_order_items_index();




create or replace procedure bronze.create_bronze_daily_payments_index()

language plpgsql
as $$

DECLARE
bronze_main_copy_time interval;
bronze_daily_copy_time interval;
index_first_time timestamp;
index_time interval;

begin

    
        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.payments_raw_daily( payment_id, order_id, created_at_bronze);
        index_time := clock_timestamp()-index_first_time;

        bronze_daily_copy_time := (select max(executing_time) from operational_log.bronze_ingest_log where table_name='payments' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily') * interval '1 second';
        bronze_main_copy_time :=  (select max(executing_time) from operational_log.bronze_ingest_log where table_name='payments' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw') * interval '1 second';


        --Inserting log and safeteynet data --
        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count, silver_daily_row_count, silver_main_row_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'payments',
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='payments' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily' ),
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='payments' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw'),
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_daily_copy_time ,
        index_time ,
        bronze_main_copy_time,
        bronze_daily_copy_time + index_time + bronze_main_copy_time,
        current_date
        );

end;
$$;

--call bronze.create_bronze_daily_payments_index();



create or replace procedure bronze.create_bronze_daily_main_orders_index()

language plpgsql
as $$

DECLARE
bronze_main_copy_time interval;
bronze_daily_copy_time interval;
index_first_time timestamp;
index_time interval;


begin 

        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.orders_raw_daily( order_id, customer_id, created_at_bronze);
        index_time := clock_timestamp()-index_first_time;

        bronze_daily_copy_time := (select max(executing_time) from operational_log.bronze_ingest_log where table_name='orders' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily') * interval '1 second';
        bronze_main_copy_time :=  (select max(executing_time) from operational_log.bronze_ingest_log where table_name='orders' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw') * interval '1 second';


        --Inserting log and safeteynet data --
        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count, silver_daily_row_count, silver_main_row_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'orders',
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='orders' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily' ),
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='orders' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw'),
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_daily_copy_time ,
        index_time ,
        bronze_main_copy_time,
        bronze_daily_copy_time + index_time + bronze_main_copy_time,
        current_date
        );

end;
$$;

--call bronze.create_bronze_daily_main_orders_index();




create or replace procedure bronze.create_bronze_daily_products_index()

language plpgsql
as $$    

DECLARE
bronze_main_copy_time interval;
bronze_daily_copy_time interval;
index_first_time timestamp;
index_time interval;


begin 

        index_first_time := clock_timestamp();
        CREATE INDEX ON bronze.products_raw_daily( product_id, created_at_bronze);
        index_time := clock_timestamp()-index_first_time;


        bronze_daily_copy_time := (select max(executing_time) from operational_log.bronze_ingest_log where table_name='products' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily') * interval '1 second';
        bronze_main_copy_time :=  (select max(executing_time) from operational_log.bronze_ingest_log where table_name='products' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw') * interval '1 second';


        --Inserting log and safeteynet data --
        insert into operational_log.bronze_ingest_safetynet(ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count, silver_daily_row_count, silver_main_row_count,silver_daily_insert_executing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,created_at)
        values((select ingestion_id from operational_log.ingestion_id),
        'products',
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='products' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_daily' ),
        (select sum(row_count) from operational_log.bronze_ingest_log where table_name='products' and ingestion_id=(select ingestion_id from operational_log.ingestion_id) and ingestion_for='bronze_raw'),
        null,
        null,
        null,
        null,
        null,
        null,
        bronze_daily_copy_time ,
        index_time ,
        bronze_main_copy_time,
        bronze_daily_copy_time + index_time + bronze_main_copy_time,
        current_date
        );
end;
$$;

--call bronze.create_bronze_daily_products_index();

=====================
--BRONZE IMPORT-- END
=====================



------------------------------------------------------------------------------------------------------------



===================================================
--




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
