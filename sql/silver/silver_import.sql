-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@silver

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

===============


===============




===============================
=============================== --PARALLEL IMPORT SILVER (EXPERIMENTAL)--
===============================


/*Payments*/
create or replace procedure silver.ingest_silver_daily_payments()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
rows_count int;
BEGIN


        if  exists(select 1 from information_schema.tables where table_name = 'payments_raw_daily' and table_schema = 'bronze')
        then    /* Checking if payments_daily alreday exists or not*/

                raise notice 'started,,,,';
                first_time := clock_timestamp();

                insert into silver.payments_daily(payment_id,method,order_id,order_date,total,payment_date,created_at_bronze,source_file_id)
                select distinct on(payment_id,order_id) 
                        lower(trim(payment_id))::varchar(255),
                        lower(trim(method))::varchar(50),
                        lower(trim(order_id))::varchar(255),
                        case when nullif(trim(order_date),'') ~'^\d{4}-\d{2}-\d{2}$'
                        then to_date(trim(order_date),'YYYY-MM-DD')
                        end ,
                        trim(total)::numeric(10,2),
                        case when nullif(trim(payment_date),'') ~ '^\d{4}-\d{2}-\d{2}$'
                        then to_date(trim(payment_date),'YYYY-MM-DD')
                        end,
                        created_at_bronze,
                        source_file_id
                from bronze.payments_raw_daily order BY
                payment_id,order_id,created_at_bronze desc;

                insert_time := clock_timestamp() - first_time;


                

                RAISE NOTICE 'Data full loaded to [payments_daily] table in % min.',insert_time;


                call silver.payments_validation_optimized(insert_time);
   
                /* Data Validation procedure call-> */

                

                RAISE NOTICE 'PK task started for [payments_daily] table,,,';

                first_time:=clock_timestamp();

                alter table silver.payments_daily  /*Creating PK for payments_daily table*/
                add constraint payment_order_pk_daily primary key (payment_id,order_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(payment_id) into rows_count from silver.payments_daily;

                update operational_log.payments_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [payments_daily] in % min.', clock_timestamp()-first_time;


        else 
                RAISE NOTICE 'New data already inserted to [payments] table, no data to load...';

        end if;


end;
$$;



create or replace procedure silver.ingest_silver_raw_payments()
language PLPGSQL
as $$
BEGIN
                        /*Insert into Payments main table, optimized option:*/
                /*Update first if new data came for the same payment_id and order_id*/
                
                update silver.payments p
                SET
                payment_date = pd.payment_date,
                method = pd.method,
                order_date = pd.order_date,
                total = pd.total,
                created_at_bronze = pd.created_at_bronze,
                created_at_silver = current_timestamp
                FROM silver.payments_daily pd
                WHERE p.payment_id = pd.payment_id AND p.order_id = pd.order_id
                and (p.payment_date,p.method,p.order_date,p.total) is distinct from 
                (pd.payment_date,pd.method,pd.order_date,pd.total); 


                get DIAGNOSTICS local_rows_updated_count = ROW_COUNT;  /*Updating log table for update count*/
                update  operational_log.payments_log
                set silver_main_rows_updated_count =  local_rows_updated_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);


                /*Now insert remaining rows those are new:*/
                
                insert into silver.payments(payment_id,payment_date,method,order_id,order_date,total,created_at_bronze,created_at_silver)
                select a.payment_id,a.payment_date,a.method,a.order_id,a.order_date,a.total,a.created_at_bronze,current_timestamp
                from silver.payments_daily a
                where not exists (select 1 from silver.payments pd
                where pd.payment_id=a.payment_id and pd.order_id=a.order_id);

                get DIAGNOSTICS local_rows_inserted_count = ROW_COUNT;  /*Updating log table for insert count*/
                update  operational_log.payments_log
                set silver_main_rows_inserted_count =  local_rows_inserted_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);
                


                RAISE NOTICE 'Data full loaded to [payments] main table in % min.<---', clock_timestamp()-first_time;
                

                drop table bronze.payments_raw_daily; /*Dropping payments bronze daily table */ 


END;
$$;     








/*Order_items*/
create or replace procedure silver.parallel_silver_import_order_items()
language PLPGSQL
as $$
declare first_time timestamp;
order_items_start_time timestamp;
local_rows_updated_count int;
local_rows_inserted_count int;
BEGIN
        /*Full load Order_items*/
        first_time := clock_timestamp();
        order_items_start_time := clock_timestamp();

        if exists (select 1 from information_schema.tables where table_name = 'order_items_raw_daily' and table_schema = 'bronze')
        then     /* Checking if order_items_daily alreday exists or not*/

                RAISE NOTICE 'Data loading started for [order_items_daily] table,,,--->';
               
                create unlogged table silver.order_items_daily(order_id VARCHAR(255),product_id VARCHAR(255),quantity NUMERIC(10,2),unit_price NUMERIC(10,2),total NUMERIC(10,2),created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);
                
                insert into silver.order_items_daily (order_id, product_id, quantity, unit_price, total, created_at_bronze)
                select distinct on (order_id,product_id) 
                lower(trim(order_id))::varchar(255),
                lower(trim(product_id))::varchar(255),
                trim(quantity::text)::numeric(10,2),
                trim(unit_price::text)::numeric(10,2),
                trim(total::text)::numeric(10,2),
                created_at_bronze
                from bronze.order_items_raw_daily
                order by order_id,product_id, created_at_bronze desc;

                RAISE NOTICE 'Data full loaded to [order_items_daily] table in % min.', clock_timestamp()-first_time;


                call silver.order_items_validation_optimized();
                /*call silver.silver_order_items_validation();*/
                /* Data Validation procedure call-> */

                

                RAISE NOTICE 'PK task started for [order_items_daily] table,,,';

                first_time:=clock_timestamp();

                if not exists (select 1 from pg_constraint where conname='order_product_pk')
                THEN   /*If order_items table has no PK already, creating one*/
                        alter table silver.order_items
                        add constraint order_product_pk primary key (order_id, product_id);
                end if;

                alter table silver.order_items_daily /*Creating PK for order_items table*/
                add constraint order_product_pk_daily primary key (order_id, product_id);

                RAISE NOTICE 'PK loaded, complete [order_items_daily] in % min.', clock_timestamp()-first_time;

                RAISE NOTICE 'Now inserting to [order_items] main table';

                first_time:= clock_timestamp();

                /*
                insert into silver.order_items  
                select * from silver.order_items_daily
                on conflict(order_id,product_id)
                do update SET
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total = EXCLUDED.total,
                created_at_bronze = EXCLUDED.created_at_bronze,
                created_at_silver = current_timestamp
                where (silver.order_items.quantity,
                silver.order_items.unit_price,
                silver.order_items.total) 
                is distinct from
                (EXCLUDED.quantity,
                EXCLUDED.unit_price,
                EXCLUDED.total);
                */

                /*Optimized Insert:*/
                update silver.order_items a
                SET
                quantity = b.quantity,
                unit_price = b.unit_price,
                total = b.total,
                created_at_bronze = b.created_at_bronze,
                created_at_silver = current_timestamp
                from silver.order_items_daily b
                where a.order_id = b.order_id and a.product_id = b.product_id
                and (a.quantity,a.unit_price,a.total) 
                is distinct from
                (b.quantity,b.unit_price,b.total);

                get diagnostics local_rows_updated_count = row_count; /*Getting number of rows updated*/
                update operational_log.order_items_log
                set silver_main_rows_updated_count = local_rows_updated_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);


                /*Inserting new rows:*/
                insert into silver.order_items(order_id,product_id,quantity,unit_price,total,created_at_bronze,created_at_silver)
                select order_id,product_id,quantity,unit_price,total,created_at_bronze,current_timestamp
                from silver.order_items_daily a
                where not exists (select 1 from silver.order_items o
                where o.order_id=a.order_id and o.product_id=a.product_id);

                get diagnostics local_rows_inserted_count = row_count; /*Getting number of rows inserted*/
                update operational_log.order_items_log
                set silver_main_rows_inserted_count = local_rows_inserted_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);


                RAISE NOTICE 'Data full loaded to [order_items] main table in % min.<---', clock_timestamp()-first_time;

                drop table bronze.order_items_raw_daily; /*Dropping order_items bronze daily table */

        else
                RAISE NOTICE 'New data already inserted to [order_items] table, no data to load...';
        end if;

        update operational_log.order_items_log /*Updating log table[calculating executing time]*/
        set executing_time= clock_timestamp()-order_items_start_time
        where ingestion_id=(select ingestion_id from operational_log.ingestion_id);

end;
$$;


/*Orders*/

create or replace procedure silver.parallel_silver_import_orders()
language PLPGSQL
as $$
declare first_time timestamp;
orders_start_time timestamp;
local_rows_updated_count int;
local_rows_inserted_count int;
BEGIN
          /*Full load orders*/
        
        first_time := clock_timestamp();
        orders_start_time:= clock_timestamp();

        if exists (select 1 from information_schema.tables where table_name = 'orders_raw_daily' and table_schema = 'bronze')
        then /*If orders table exists, loading data to orders table*/

                RAISE NOTICE 'Data loading started for [orders_daily] table,,,--->';
                
                create unlogged table silver.orders_daily(order_id VARCHAR(255),customer_id VARCHAR(255),order_date date,status VARCHAR(255),created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);
                
                insert into silver.orders_daily(order_id,customer_id,order_date,status,created_at_bronze)
                select distinct on (order_id,customer_id)
                lower(trim(order_id))::varchar(255),
                lower(trim(customer_id))::varchar(255),
                case when nullif(trim(order_date),'') ~ '^\d{4}-\d{2}-\d{2}$'
                then to_date(trim(order_date),'YYYY-MM-DD') end,
                lower(trim(status))::varchar(255),
                created_at_bronze
                from bronze.orders_raw_daily order BY
                order_id,customer_id,created_at_bronze desc;

                RAISE NOTICE 'Data full loaded to [orders_daily] table in % min.', clock_timestamp()-first_time;


                /*call silver.silver_orders_validation();*/
                call silver.orders_validation_optimized();


                

                RAISE NOTICE 'PK task started for [orders_daily] table,,,';

                first_time:=clock_timestamp();

                if not exists (select 1 from pg_constraint where conname='order_customer_pk')
                then /*If PK does not exist, creating one*/
                        alter table silver.orders
                        add constraint order_customer_pk primary key (order_id,customer_id);
                end if;

                alter table silver.orders_daily            /*Creating PK for orders_daily table*/
                add constraint order_customer_pk_daily primary key (order_id,customer_id);

                RAISE NOTICE 'PK loaded, completed in [orders_daily] % min.', clock_timestamp()-first_time;

                RAISE NOTICE 'Now inserting to [orders_daily] main table';

                first_time:=clock_timestamp();

                /*Initial Insert, not optimized:*/

                 /*
                insert into silver.orders
                select * from silver.orders_daily
                on conflict(order_id,customer_id)
                do update SET
                status = EXCLUDED.status,
                order_date = EXCLUDED.order_date,
                created_at_silver = current_timestamp
                where (silver.orders.status,
                silver.orders.order_date)is distinct from(EXCLUDED.status,
                EXCLUDED.order_date); */
                

                /*Optimized Insert:*/
                
                update silver.orders a
                SET
                status = b.status,
                order_date = b.order_date,
                created_at_bronze = b.created_at_bronze,
                created_at_silver = current_timestamp
                from silver.orders_daily b
                where a.order_id = b.order_id and a.customer_id = b.customer_id
                and (a.status,a.order_date)
                is distinct from
                (b.status,b.order_date); 


                /*Getting number of rows updated*/
                
                get diagnostics local_rows_updated_count = row_count;
                update operational_log.orders_log
                set silver_main_rows_updated_count = local_rows_updated_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);





                /*Now insert remaining rows those are new:*/
                
                insert into silver.orders(order_id,customer_id,order_date,status,created_at_bronze,created_at_silver)
                select order_id,customer_id,order_date,status,created_at_bronze,current_timestamp
                from silver.orders_daily a
                where not exists (select 1 from silver.orders o
                where o.order_id=a.order_id and o.customer_id=a.customer_id); 

                /*Getting number of rows inserted*/
                
                get diagnostics local_rows_inserted_count = row_count; 
                update operational_log.orders_log
                set silver_main_rows_inserted_count = local_rows_inserted_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);




                RAISE NOTICE 'Data full loaded to [orders] main table in % min.<---', clock_timestamp()-first_time;

                drop table bronze.orders_raw_daily;

        else 
                RAISE NOTICE 'New data already inserted to [orders] table, no data to load...';
        end if;

        update operational_log.orders_log /*Updating log table*/
        set executing_time=clock_timestamp()-orders_start_time
        where ingestion_id=(select ingestion_id from operational_log.ingestion_id);

END;
$$;




/*Customers + Products*/

create or replace procedure silver.parallel_silver_import_customers_products()
language PLPGSQL
as $$
declare first_time timestamp;
customers_start_time timestamp;
products_start_time timestamp;
local_rows_updated_count int;
local_rows_inserted_count int;
BEGIN
          /*Full load Customers*/
        first_time := clock_timestamp();
        customers_start_time:= clock_timestamp();

        if exists (select 1 from information_schema.tables where table_name = 'customers_raw_daily' and table_schema = 'bronze')
        then  /* Checking if customers_daily alreday exists or not*/

                RAISE NOTICE 'Data loading started for [customers_daily] table,,,--->';
                first_time:=clock_timestamp();

                create unlogged table
                silver.customers_daily(customer_id VARCHAR(255),name VARCHAR(255),signup_date date,created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);

                insert into silver.customers_daily(customer_id,name,signup_date,created_at_bronze)
                select distinct on (customer_id)
                lower(trim(customer_id))::varchar(255),
                lower(trim(name))::varchar(255),
                case when nullif(trim(signup_date),'') ~ '^\d{4}-\d{2}-\d{2}$'
                then to_date(trim(signup_date),'YYYY-MM-DD') end,
                created_at_bronze
                from bronze.customers_raw_daily
                order by customer_id, created_at_bronze desc;

                RAISE NOTICE 'Data full loaded to [customers_daily] table in % min.', clock_timestamp()-first_time;



                /*call silver.silver_customer_validation(); */
                call silver.customer_validation_optimized();
                /* Data Validation procedure call-> */

                

                RAISE NOTICE 'PK task started for [customers_daily] table,,,';

                first_time:=clock_timestamp();

                if not exists (select 1 from pg_constraint where conname='customer_id_pk')
                then /*If customers table has no PK already, creating one*/
                        alter table silver.customers
                        add constraint customer_id_pk primary key (customer_id,valid_from);
                        end if;

                if not exists(select 1 from pg_indexes  where indexname='partial_index_customer')
                then /*If customers table has no partial index already, creating one*/
                        create unique index partial_index_customer on silver.customers(customer_id)
                        where is_valid=true;
                end if;

                alter table silver.customers_daily             /*Creating PK for customers_daily table*/
                add constraint customer_id_pk_daily primary key (customer_id);

                RAISE NOTICE 'PK loaded, complete [customers_daily] in % min.', clock_timestamp()-first_time;

                RAISE NOTICE 'Now inserting to [customers] main table';

                first_time:=clock_timestamp();


                /*We are tracking old data of Customers table, so to track we add is_valid, valid_from and valid_to columns to Customers main table. So if new version of information arrived we don't lost previous version data.*/
                
                /*Updating old version data when new data with same ID came, updating valid_to date, is_valid flag and creating new version of data.*/
                update silver.customers a
                set valid_to=current_date - interval '1 day',
                is_valid=false
                from silver.customers_daily b 
                WHERE a.customer_id=b.customer_id
                and a.is_valid=true 
                and (a.name,a.signup_date) is distinct from (b.name,b.signup_date); /* Now, as new version of customer info arrived, this block will automatically update the previous state as false/old state.*/



                get diagnostics local_rows_updated_count = row_count; /*Getting number of rows updated*/
                update operational_log.customers_log
                set silver_main_rows_updated_count = local_rows_updated_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);



                /*Inserting new version of data*/
                insert into silver.customers(customer_id, name, signup_date, created_at_bronze, 
                created_at_silver, valid_from, valid_to, is_valid)
                select customer_id, name, signup_date, created_at_bronze,
                current_timestamp, current_date, '2050-01-01', true from silver.customers_daily
                where not exists(
                select 1 from silver.customers where 
                silver.customers.customer_id=silver.customers_daily.customer_id
                and silver.customers.is_valid=true
                ); /*Here, we are inserting new rows, if previous version is already updated to false.*/


                get diagnostics local_rows_inserted_count = row_count; /*Getting number of rows inserted*/
                update operational_log.customers_log
                set silver_main_rows_inserted_count = local_rows_inserted_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);


                RAISE NOTICE 'Data full loaded to [customers] main table in % min.<---', clock_timestamp()-first_time;


                drop table bronze.customers_raw_daily;

        else 
                RAISE NOTICE 'New data already inserted to [customers] table, no data to load...';
        end if;

        update operational_log.customers_log /*Updating log table*/
        set executing_time=clock_timestamp()-customers_start_time
        where ingestion_id=(select ingestion_id from operational_log.ingestion_id);





        /*Full load products*/

        first_time := clock_timestamp();

        products_start_time:= clock_timestamp();

        if exists (select 1 from information_schema.tables where table_name = 'products_raw_daily' and table_schema = 'bronze')
        then /*If products table exists, loading data to products table*/

                RAISE NOTICE 'Data loading started for [products_daily] table,,,--->';

            
                create unlogged table silver.products_daily(product_id VARCHAR(255),name VARCHAR(255),category VARCHAR(255),price NUMERIC(10,2),created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);
                insert into silver.products_daily(product_id,name,category,price,created_at_bronze)
                select distinct on(product_id)
                lower(trim(product_id))::varchar(255),
                lower(trim(name))::varchar(255),
                lower(trim(category))::varchar(255),
                trim(price::text)::numeric(10,2),
                created_at_bronze
                from bronze.products_raw_daily order by 
                product_id, created_at_bronze desc;

                RAISE NOTICE 'Data full loaded to [products_daily] table in % min.', clock_timestamp()-first_time;

                /*call silver.silver_products_validation();*/

                CALL silver.products_validation_optimized();


                RAISE NOTICE 'PK task started for [products_daily] table,,,';

                first_time:=clock_timestamp();

                IF NOT EXISTS (SELECT 1 FROM pg_constraint where conname='product_pk') 
                THEN /*If PK does not exist, creating one*/
                        ALTER TABLE silver.products ADD CONSTRAINT product_pk PRIMARY KEY (product_id);
                END IF;

                alter table silver.products_daily    /*Creating PK for products_daily table*/
                add constraint product_pk_daily primary key (product_id);

                RAISE NOTICE 'PK loaded, completed in [Products_daily] % min.', clock_timestamp()-first_time;

                RAISE NOTICE 'Now inserting to [Products_daily] main table';

                first_time:=clock_timestamp();

                
                /*insert into silver.products
                select * from silver.products_daily
                on conflict(product_id)
                do update SET
                name = EXCLUDED.name,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                created_at_silver = current_timestamp
                where (silver.products.name,
                silver.products.category,
                silver.products.price)is distinct from(EXCLUDED.name,
                EXCLUDED.category,
                EXCLUDED.price);*/

                /*Optimized Insert:*/
                /*update*/
                update silver.products a
                SET
                name = b.name,
                category = b.category,
                price = b.price,
                created_at_bronze = b.created_at_bronze,
                created_at_silver = current_timestamp
                from silver.products_daily b
                where a.product_id = b.product_id
                and (a.name,a.category,a.price) 
                is distinct from 
                (b.name,b.category,b.price);

                get diagnostics local_rows_updated_count = row_count; /*Getting number of rows updated*/
                update operational_log.products_log
                set silver_main_rows_updated_count = local_rows_updated_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);


                /*Now insert remaining rows those are new:*/
                insert into silver.products(product_id,name,category,price,created_at_bronze,created_at_silver)
                select product_id,name,category,price,created_at_bronze,current_timestamp
                from silver.products_daily a
                where not exists (select 1 from silver.products p
                where p.product_id=a.product_id);        

                get diagnostics local_rows_inserted_count = row_count; /*Getting number of rows inserted*/
                update operational_log.products_log
                set silver_main_rows_inserted_count = local_rows_inserted_count
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);



                RAISE NOTICE 'Data full loaded to [products] main table in % min.<---', clock_timestamp()-first_time;

                drop table bronze.products_raw_daily;

        else 
                RAISE NOTICE 'No data to load, table [products] is empty';
        end if;

        update operational_log.products_log /*Updating log table*/
        set executing_time=clock_timestamp()-products_start_time
        where ingestion_id=(select ingestion_id from operational_log.ingestion_id);

END;
$$;





show data_directory;

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM bronze.order_items_raw_daily;






