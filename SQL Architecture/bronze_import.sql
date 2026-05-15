-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@bronze

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

========================

/*
Creating tables in bronze: Main/final table for bronze(Run only once)
*/

create or replace procedure create_tables_bronze()
language PLPGSQL
as $$

BEGIN
        drop table if exists bronze.customers_raw;
        create table bronze.customers_raw
                (
                    customer_id text,
                    name text,
                    signup_date text, 
                    created_at_bronze timestamp default current_timestamp
                );


        drop table if exists bronze.products_raw;
        create table bronze.products_raw(
                    product_id text ,
                    name text,
                    category text,
                    price text,
                    created_at_bronze timestamp default current_timestamp
                );


        drop table if exists bronze.orders_raw;
        create table bronze.orders_raw(
                order_id text,
                customer_id text,
                order_date text,
                status text,
                created_at_bronze timestamp default current_timestamp
                );

        drop table if exists bronze.order_items_raw;
        create table bronze.order_items_raw(
                order_id text,
                product_id text,
                quantity text,
                unit_price text,
                total text,
                created_at_bronze timestamp default current_timestamp
                );


        drop table if exists bronze.payments_raw;
        create table bronze.payments_raw(
                    payment_id text ,
                    method text,
                    order_id text,
                    order_date text,
                    total text,
                    payment_date text,
                    created_at_bronze timestamp default current_timestamp
                );

end;
$$;


call create_tables_bronze();

/*
Table creation complete. Now we will ingest data into the bronze layer from CSV files.
*/


========================


create or replace procedure bronze_ingest()
LANGUAGE plpgsql
as $$
DECLARE
local_bronze_row_count int;
first_time timestamp;
second_time timestamp;
third_time timestamp;

Begin


        /*Inserting ingestion_id to table:[This table is for one session] */
        truncate table operational_log.ingestion_id;
        insert into operational_log.ingestion_id(created_at) values(now());


        /*Dropping silver daily temporary table before inserting new data to bronze*/
        call silver.silver_daily_table_drop();


        /*
        IMPORTING DATA INTO THE BRONZE LAYER (customers_raw)
        */
        if not exists(select 1 from operational_log.bronze_ingest_safetynet where table_name = 'customers' and created_at = current_date)
        then /*safetynet check to prevent same day ingestion*/

                RAISE NOTICE 'Step 1: Starting to ingest Customer data to bronze daily table...';

                create UNLOGGED table bronze.customers_raw_daily(customer_id text, name text, signup_date text, created_at_bronze timestamp default current_timestamp);

                first_time := clock_timestamp();
                COPY bronze.customers_raw_daily(customer_id, name, signup_date)
                FROM  program 'head -n 1000 "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/customers/customers_2026-05-03.csv"'
                WITH (FORMAT csv, HEADER true);
                second_time := clock_timestamp();
               

                get diagnostics local_bronze_row_count = row_count;


                RAISE NOTICE 'Step 2: Customer data ingested successfully in % mins to bronze daily table and creating index for it....',second_time-first_time;

                CREATE INDEX ON bronze.customers_raw_daily(created_at_bronze, customer_id)include(name, signup_date);

                raise notice '->Index created successfully';

                RAISE NOTICE 'Step 3: Starting to ingest Customer data to main table....';


                third_time := clock_timestamp();
                insert into bronze.customers_raw
                select * from bronze.customers_raw_daily;

                raise notice 'Step 4: Customer data ingested successfully to main table...';

                --Inserting log and safeteynet data --
                insert into operational_log.bronze_ingest_safetynet(ingestion_id,file_name,table_name,file_path,bronze_row_count,copy_executing_time,insert_executing_time,created_at)
                values((select ingestion_id from operational_log.ingestion_id),
                'customers_' || current_date || '.csv',
                'customers',
                '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/customers/customers_ '|| current_date || '.csv',
                local_bronze_row_count,
                second_time-first_time,
                clock_timestamp()-third_time,
                current_date
                );


        else 
                raise notice 'Data already ingested for today for CUSTOMERS table';
        end if;

        
        


        /*
        IMPORTING DATA INTO THE BRONZE LAYER (products_raw)
       */

        if not exists(select 1 from operational_log.bronze_ingest_safetynet where table_name = 'products' and created_at = current_date)
        then

                RAISE NOTICE 'Step 1: Starting to ingest Product data to bronze daily table ...';

                create UNLOGGED table bronze.products_raw_daily(product_id text, name text, category text, price text, created_at_bronze timestamp default current_timestamp);

                first_time := clock_timestamp();
                copy bronze.products_raw_daily(product_id, name, category, price)
                from  program 'head -n 1000 "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/products/products_2026-05-04.csv"'
                with (format csv, header true);
                second_time := clock_timestamp();

                get diagnostics local_bronze_row_count = row_count;

                RAISE NOTICE 'Step 2: Product data ingested successfully to brone daily table and creating index for it....';

                CREATE INDEX ON bronze.products_raw_daily(created_at_bronze, product_id)include(name, category, price);

                raise notice '->Index created successfully';

                RAISE NOTICE 'Step 3: Starting to ingest Product data to main table....';

                third_time := clock_timestamp();
                insert into bronze.products_raw
                select * from bronze.products_raw_daily;

                raise notice 'Step 4: Product data ingested successfully to main table...';

                insert into operational_log.bronze_ingest_safetynet(ingestion_id,file_name,table_name,file_path,bronze_row_count,copy_executing_time,insert_executing_time,created_at)
                values((select ingestion_id from operational_log.ingestion_id),
                'products_' || current_date || '.csv',
                'products',
                '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/products/products_'|| current_date || '.csv',
                local_bronze_row_count,
                second_time-first_time,
                clock_timestamp()-third_time,
                current_date
                );


        else 
                raise notice 'Data already ingested for today for PRODUCTS table';
        end if;



        /*
        IMPORTING DATA INTO THE BRONZE LAYER (orders_raw)
        */
        if not exists(select 1 from operational_log.bronze_ingest_safetynet where table_name = 'orders' and created_at = current_date)
        then

                RAISE NOTICE 'Step 1: Starting to ingest Order data to bronze daily table...';
                
                create UNLOGGED table bronze.orders_raw_daily(order_id text, customer_id text, order_date text, status text, created_at_bronze timestamp default current_timestamp);
                
                first_time := clock_timestamp();
                copy bronze.orders_raw_daily(order_id, customer_id, order_date, status)
                from  program 'head -n 1000 "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/orders/orders_2026-05-04.csv"'
                with(format csv, header true);
                second_time := clock_timestamp();

                get diagnostics local_bronze_row_count = row_count;

                RAISE NOTICE 'Step 2: Order data ingested successfully to bronze daily table and creating index for it....';

                CREATE INDEX ON bronze.orders_raw_daily(created_at_bronze, order_id, customer_id)include( order_date, status);

                raise notice '->Index created successfully';

                RAISE NOTICE 'Step 3: Starting to ingest Order data to main table....';

                third_time := clock_timestamp();
                insert into bronze.orders_raw
                select * from bronze.orders_raw_daily;

                raise notice 'Step 4: Order data ingested successfully to main table...';

                insert into operational_log.bronze_ingest_safetynet(ingestion_id,file_name,table_name,file_path,bronze_row_count,copy_executing_time,insert_executing_time,created_at)
                values((select ingestion_id from operational_log.ingestion_id),
                'orders_' || current_date || '.csv',
                'orders',
                '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/orders/orders_'|| current_date || '.csv',
                local_bronze_row_count,
                second_time-first_time,
                clock_timestamp()-third_time,
                current_date
                );

        else 
                raise notice 'Data already ingested for today for ORDERS table';
        end if;
 

        /*
        IMPORTING DATA INTO THE BRONZE LAYER (order_items_raw)
        */
        if not exists(select 1 from operational_log.bronze_ingest_safetynet where table_name = 'order_items' and created_at = current_date)
        then

                RAISE notice 'Step 1: Starting to ingest Order Item data into bronze daily table...';
                
                create UNLOGGED table bronze.order_items_raw_daily(order_id text, product_id text, quantity text, unit_price text, total text, created_at_bronze timestamp default current_timestamp);
        
                first_time := clock_timestamp();
                copy bronze.order_items_raw_daily(order_id, product_id, quantity, unit_price, total)
                from  program 'head -n 1000 "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/order_items/order_items_2026-05-04.csv"'
                with(format csv, header true);
                second_time := clock_timestamp();

                get diagnostics local_bronze_row_count = row_count;

                RAISE NOTICE 'Step 2: Order Item data ingested successfully to bronze daily table and creating index for it....';

                CREATE INDEX ON bronze.order_items_raw_daily(created_at_bronze, order_id, product_id)include( quantity, unit_price, total);

                raise notice '->Index created successfully';

                RAISE NOTICE 'Step 3: Starting to ingest Order Item data into main table...';
                
                third_time := clock_timestamp();
                insert into bronze.order_items_raw
                select * from bronze.order_items_raw_daily;

                raise notice 'Step 4: Order Item data ingested successfully to main table...';
                
                insert into operational_log.bronze_ingest_safetynet(ingestion_id,file_name,table_name,file_path,bronze_row_count,copy_executing_time,insert_executing_time,created_at)
                values((select ingestion_id from operational_log.ingestion_id),
                'order_items_' || current_date || '.csv',
                'order_items',
                '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/order_items/order_items_'|| current_date || '.csv',
                local_bronze_row_count,
                second_time-first_time,
                clock_timestamp()-third_time,
                current_date    
                );

        else 
                raise notice 'Data already ingested for today for ORDER_ITEMS table';
        end if;

        

        /*
        IMPORTING DATA INTO THE BRONZE LAYER (payments_raw)
        */
        if not exists(select 1 from operational_log.bronze_ingest_safetynet where table_name = 'payments' and created_at = current_date)
        then
        
                RAISE NOTICE 'Step 1: Starting to ingest Payment data into bronze daily table...';
                
                create UNLOGGED table bronze.payments_raw_daily(payment_id text,method text, order_id text,  order_date text, total text, payment_date text,   created_at_bronze timestamp default current_timestamp);
                
                first_time := clock_timestamp();
                copy bronze.payments_raw_daily(payment_id,  method, order_id,  order_date, total, payment_date)
                from  program 'head -n 1000 "/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/payments/payments_2026-05-04.csv"'
                with(format csv, header true);
                second_time := clock_timestamp();

                get diagnostics local_bronze_row_count = row_count;

                RAISE NOTICE 'Step 2: Payment data ingested successfully to bronze daily table and creating index for it......';

                CREATE INDEX ON bronze.payments_raw_daily(created_at_bronze, payment_id, order_id)include( method,  order_date, total, payment_date);

                RAISE NOTICE 'Step 3: Starting to ingest Payment data into main table...';

                third_time := clock_timestamp();
                insert into bronze.payments_raw
                select * from bronze.payments_raw_daily;

                raise notice 'Step 4: Payment data ingested successfully to main table...';

                insert into operational_log.bronze_ingest_safetynet(ingestion_id,file_name,table_name,file_path,bronze_row_count,copy_executing_time,insert_executing_time,created_at)
                values((select ingestion_id from operational_log.ingestion_id),
                'payments_' || current_date || '.csv',
                'payments',
                '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/payments/payments_'|| current_date || '.csv',
                local_bronze_row_count,
                second_time-first_time,
                clock_timestamp()-third_time,
                current_date
                );

        else 
                raise notice 'Data already ingested for today for PAYMENTS table';
        end if;
         
        
end;
$$;

call bronze_ingest();


/*
Data ingestion into the bronze layer is complete. We can now proceed to create tables in the silver layer and ingest data from bronze to silver.
*/

========================

/*
Procedure to drop all table :
*/

create or replace procedure drop_all_tables_bronze_daily()
language PLPGSQL
as $$

BEGIN

    drop table if exists bronze.customers_raw_daily cascade;
    drop table if exists bronze.products_raw_daily cascade;
    drop table if exists bronze.orders_raw_daily cascade;
    drop table if exists bronze.order_items_raw_daily cascade;
    drop table if exists bronze.payments_raw_daily cascade;

end;
$$;

call drop_all_tables_bronze_daily();


========================





select datname,pid, (now()-query_start)::time as time_,query as "query/command", state as "state/bytes_total" , backend_type as "backend_type/tuples_processed" from pg_stat_activity where state='active' and query not like '%select datname%' 
union all
select datname, pid,null::time, command as "query/command",bytes_total::text as "state/bytes_total",tuples_processed::text as "backend_type/tuples_processed" from   pg_stat_progress_copy;


SELECT * FROM pg_stat_wal;


select * from pg_stat_activity where state='active';
select * from information_schema.tables where table_name like '%stat%';














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
