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
    ALTER SYSTEM SET max_wal_size = '4GB';
    ALTER SYSTEM SET min_wal_size = '1GB';
    ALTER SYSTEM SET checkpoint_completion_target = 0.9;
    -- Maintenance & Operations
    ALTER SYSTEM SET maintenance_work_mem = '512MB';
    ALTER SYSTEM SET work_mem = '512MB';
    -- Apply (Note: shared_buffers requires a full DB restart)
    SELECT pg_reload_conf();

SELECT name, setting, unit, source, sourcefile
FROM pg_settings 
WHERE source NOT IN ('default', 'override');

/*
END
*/

===============


/*
Creating 5 tables in the silver layer: 
*/

create or replace procedure create_tables_silver()
language PLPGSQL
as $$
BEGIN

        drop table if exists silver.customers cascade;
        create table silver.customers (
            customer_id varchar(255),
            name varchar(255),
            signup_date date ,
            created_at_bronze timestamp ,
            created_at_silver timestamp default current_timestamp,
            valid_from TIMESTAMP default current_date,
            valid_to TIMESTAMP default '2050-01-01',
            is_valid boolean default true
        );

        drop table if exists silver.products cascade;
        create table silver.products (
            product_id varchar(100) ,
            name VARCHAR(255),
            category VARCHAR(255),
            price numeric(10,2),
            created_at_bronze timestamp ,
            created_at_silver timestamp default current_timestamp
        );

        drop table if exists silver.orders cascade;
        create table silver.orders (
            order_id VARCHAR(255) ,
                customer_id VARCHAR(255),
                order_date date,
                status VARCHAR(50),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp

        );


        drop table if exists silver.order_items cascade;
        create table silver.order_items (

                order_id VARCHAR(255) ,
                product_id VARCHAR(255) ,
                quantity numeric(10,2),
                unit_price numeric(10,2),
                total numeric(10,2),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp

        );

        drop table if exists silver.payments cascade;
        create table silver.payments(
            payment_id varchar(255) ,
                    payment_date date,
                    method VARCHAR(50),
                    order_id VARCHAR(255),
                    order_date date,
                    total numeric(10,2),
                    created_at_bronze timestamp ,
                    created_at_silver timestamp default current_timestamp

        );

end;
$$;

call create_tables_silver();

===============


/*
Ingesting data into the silver layer from the bronze layer (FULL LOAD): Only run this procedure once after creating tables.
*/


CREATE OR REPLACE PROCEDURE silver.silver_import_full()
 LANGUAGE plpgsql
AS $$ 
declare first_time timestamp;
second_time timestamp;
customers_start_time timestamp ;
order_items_start_time timestamp ;
orders_start_time timestamp ;
payments_start_time timestamp ;
products_start_time timestamp ;

BEGIN



        SET LOCAL work_mem = '512MB';
        SET LOCAL synchronous_commit = OFF;
        SET LOCAL maintenance_work_mem = '512MB';



        /*Full load payments*/
        first_time := clock_timestamp();
        payments_start_time := clock_timestamp();


        if  exists(select 1 from information_schema.tables where table_name = 'payments_raw_daily' and table_schema = 'bronze')
        then    /* Checking if payments_daily alreday exists or not*/

                RAISE NOTICE 'Data loading started for [payments_daily] table,,,';

                create unlogged table silver.payments_daily(payment_id VARCHAR(255),payment_date date,method VARCHAR(50),order_id VARCHAR
                (255),order_date date,total NUMERIC(10,2),created_at_bronze TIMESTAMP,created_at_silver timestamp default current_timestamp);

                insert into silver.payments_daily(payment_id,payment_date,method,order_id,order_date,total,created_at_bronze)
                select distinct on(payment_id,order_id) 
                lower(trim(payment_id))::varchar(255),
                case when nullif(trim(payment_date),'') ~ '^\d{4}-\d{2}-\d{2}$'
                then to_date(trim(payment_date),'YYYY-MM-DD')
                end ,
                lower(trim(method))::varchar(50),
                lower(trim(order_id))::varchar(255),
                case when nullif(trim(order_date),'') ~'^\d{4}-\d{2}-\d{2}$'
                then to_date(trim(order_date),'YYYY-MM-DD')
                end ,
                trim(total)::numeric(10,2),
                created_at_bronze
                from bronze.payments_raw_daily order BY
                payment_id,order_id,created_at_bronze desc;

                call silver.silver_payments_validation(); /* Data Validation procedure call-> */

                RAISE NOTICE 'Data full loaded to [payments_daily] table in % min.', clock_timestamp()-first_time;

                RAISE NOTICE 'PK task started for [Payments] table,,,';

                first_time:= clock_timestamp();
                if not exists (select 1 from pg_constraint where conname='payment_order_pk') THEN /*If payments table has no PK already, creating one*/
                        alter table silver.payments
                        add constraint payment_order_pk primary key (payment_id,order_id);
                        end if;

                alter table silver.payments_daily  /*Creating PK for payments_daily table*/
                add constraint payment_order_pk_daily primary key (payment_id,order_id);

                RAISE NOTICE 'PK task completed [Payments] in % min.', clock_timestamp()-first_time;

                RAISE NOTICE 'Now inserting to [Payments] main table';
                first_time:= clock_timestamp();


                insert into silver.payments  /*From payments_daily table, inserting already cleaned data to payment mian table*/
                select  * from silver.payments_daily
                on conflict(payment_id,order_id)
                do update set
                payment_date = EXCLUDED.payment_date,
                method = EXCLUDED.method,
                order_date = EXCLUDED.order_date,
                total = EXCLUDED.total,
                created_at_bronze = EXCLUDED.created_at_bronze,
                created_at_silver = current_timestamp
                where (silver.payments.payment_date,
                silver.payments.method,
                silver.payments.order_date,
                silver.payments.total
                ) is distinct from (EXCLUDED.payment_date,
                EXCLUDED.method,
                EXCLUDED.order_date,
                EXCLUDED.total);

                RAISE NOTICE 'Loaded completed in % min.', clock_timestamp()-first_time;
                

                drop table bronze.payments_raw_daily; /*Dropping payments bronze daily table */ 

        else 
                RAISE NOTICE 'New data already inserted to [Payments] table, no data to load...';

        end if;

        update operational_log.payments_log  /*Updating log table[calculating executing time]*/
        set executing_time = clock_timestamp()-payments_start_time
        where ingestion_id = (select ingestion_id from operational_log.ingestion_id);
        
        



        /*Full load Order_items*/
        first_time := clock_timestamp();
        order_items_start_time := clock_timestamp();

        if exists (select 1 from information_schema.tables where table_name = 'order_items_raw_daily' and table_schema = 'bronze')
        then     /* Checking if order_items_daily alreday exists or not*/

                RAISE NOTICE 'Data loading started for [order_items] table,,,';
               
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

                call silver.silver_order_items_validation(); /* Data Validation procedure call-> */

                RAISE NOTICE 'Data full loaded to [order_items_daily] table in % min.', clock_timestamp()-first_time;

                RAISE NOTICE 'PK task started for [order_items] table,,,';

                first_time:=clock_timestamp();

                if not exists (select 1 from pg_constraint where conname='order_product_pk')
                THEN   /*If order_items table has no PK already, creating one*/
                        alter table silver.order_items
                        add constraint order_product_pk primary key (order_id, product_id);
                end if;

                alter table silver.order_items_daily /*Creating PK for order_items table*/
                add constraint order_product_pk_daily primary key (order_id, product_id);

                RAISE NOTICE 'PK loaded, complete [ORDER_ITEMS] in % min.', clock_timestamp()-first_time;

                raise notice 'Now inserting to [ORDER_ITEMS] main table';

                first_time:= clock_timestamp();

                insert into silver.order_items  /*From order_items_daily table, inserting already cleaned data to order_items main table*/
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

                raise notice 'Loaded completed in % min.', clock_timestamp()-first_time;

                drop table bronze.order_items_raw_daily; /*Dropping order_items bronze daily table */

        else
                raise notice 'New data already inserted to [ORDER_ITEMS] table, no data to load...';
        end if;

        update operational_log.order_items_log /*Updating log table[calculating executing time]*/
        set executing_time= clock_timestamp()-order_items_start_time
        where ingestion_id=(select ingestion_id from operational_log.ingestion_id);



        /*Full load Customers*/
        first_time := clock_timestamp();
        customers_start_time:= clock_timestamp();

        if exists (select 1 from information_schema.tables where table_name = 'customers_raw_daily' and table_schema = 'bronze')
        then  /* Checking if customers_daily alreday exists or not*/

                RAISE NOTICE 'Data loading started for [customers] table,,,';
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

                call silver.silver_customer_validation(); /* Data Validation procedure call-> */

                RAISE NOTICE 'Data full loaded to [customers_daily] table in % min.', clock_timestamp()-first_time;

                RAISE NOTICE 'PK task started for [customers] table,,,';

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

                RAISE NOTICE 'PK loaded, complete [CUSTOMERS] in % min.', clock_timestamp()-first_time;

                raise notice ' Now inserting to [CUSTOMERS] main table';

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


                RAISE NOTICE 'Loaded completed in % min.', clock_timestamp()-first_time;


                drop table bronze.customers_raw_daily;

        else 
                RAISE NOTICE 'New data already inserted to [ORDER_ITEMS] table, no data to load...';
        end if;

        update operational_log.customers_log /*Updating log table*/
        set executing_time=clock_timestamp()-customers_start_time
        where ingestion_id=(select ingestion_id from operational_log.ingestion_id);



        /*Full load orders*/

        orders_start_time:= clock_timestamp();

        if exists (select 1 from information_schema.tables where table_name = 'orders_raw_daily' and table_schema = 'bronze')
        then

        raise notice 'Data full load to [ORDERS] daily table...';
        first_time:=clock_timestamp();
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

        call silver.silver_orders_validation();

        raise notice 'Full loaded completed in % min, Creating PK for [ORDERS], both table(daily+main)', clock_timestamp()-first_time;
        first_time:=clock_timestamp();
        if not exists (select 1 from pg_constraint where conname='order_customer_pk') then
        alter table silver.orders
        add constraint order_customer_pk primary key (order_id,customer_id);
        end if;

        alter table silver.orders_daily
        add constraint order_customer_pk_daily primary key (order_id,customer_id);

        RAISE NOTICE 'PK loaded, completed in [ORDERS] % min.', clock_timestamp()-first_time;

        raise notice 'Now inserting to [ORDERS] main table';
        first_time:=clock_timestamp();

        insert into silver.orders
        select * from silver.orders_daily
        on conflict(order_id,customer_id)
        do update SET
        status = EXCLUDED.status,
        order_date = EXCLUDED.order_date,
        created_at_silver = current_timestamp
        where (silver.orders.status,
        silver.orders.order_date)is distinct from(EXCLUDED.status,
        EXCLUDED.order_date);

        raise notice 'Loaded completed in % min.', clock_timestamp()-first_time;



        drop table bronze.orders_raw_daily;

        else 
        raise notice 'No data to load, table [orders_raw_daily] is empty';
        end if;

        update operational_log.orders_log
        set executing_time=clock_timestamp()-orders_start_time
        where ingestion_id=(select ingestion_id from operational_log.ingestion_id);




        /*Full load products*/

        products_start_time:= clock_timestamp();

        if exists (select 1 from information_schema.tables where table_name = 'products_raw_daily' and table_schema = 'bronze')
        then

        raise notice 'Data full loading to [Products] daily table...';
        first_time:=clock_timestamp();
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

        call silver.silver_products_validation();


        raise notice 'Full loaded completed in % min, Creating PK for [Products], daily table', clock_timestamp()-first_time;
        first_time:=clock_timestamp();
        IF NOT EXISTS (SELECT 1 FROM pg_constraint where conname='product_pk') THEN
        ALTER TABLE silver.products ADD CONSTRAINT product_pk PRIMARY KEY (product_id);
        END IF;
        alter table silver.products_daily
        add constraint product_pk_daily primary key (product_id);
        RAISE NOTICE 'PK loaded completed in [Products] % min.', clock_timestamp()-first_time;

        raise notice 'Now inserting to [Products] main table';
        first_time:=clock_timestamp();

        insert into silver.products
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
        EXCLUDED.price);

        RAISE NOTICE 'Full data loaded completed in [Products] % min.', clock_timestamp()-first_time;

        drop table bronze.products_raw_daily;

        else 
        raise notice 'No data to load, table [products_raw_daily] is empty';
        end if;

        update operational_log.products_log
        set executing_time=clock_timestamp()-products_start_time
        where ingestion_id=(select ingestion_id from operational_log.ingestion_id);


end;
$$;

call silver.silver_import_full();




==================





===============

create or replace procedure silver.silver_daily_table_drop()
language PLPGSQL
as $$
BEGIN
        drop table if exists silver.customers_daily;
        drop table if exists silver.products_daily;
        drop table if exists silver.orders_daily;
        drop table if exists silver.order_items_daily;
        drop table if exists silver.payments_daily;

END;
$$;

call silver_daily_table_drop();


show data_directory;






