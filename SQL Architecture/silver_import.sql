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
            created_at_silver timestamp default current_timestamp 
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
declare first_time timestamp := clock_timestamp();
second_time timestamp;
BEGIN



        SET LOCAL work_mem = '512MB';
        SET LOCAL synchronous_commit = OFF;
        SET LOCAL maintenance_work_mem = '512MB';



        /*Full load payments*/
        raise notice 'Data full loading to [Payments] daily table...';

        create unlogged table silver.payments_daily(payment_id VARCHAR(255),payment_date date,method VARCHAR(50),order_id VARCHAR
        (255),order_date date,total NUMERIC(10,2),created_at_bronze TIMESTAMP,created_at_silver timestamp default current_timestamp);
        insert into silver.payments_daily(payment_id,payment_date,method,order_id,order_date,total,created_at_bronze)
        select distinct on(payment_id,order_id) payment_id,payment_date,method,order_id,order_date,total,created_at_bronze
        from bronze.payments_raw_daily order BY
        payment_id,order_id,created_at_bronze desc;

        raise notice 'loaded completed in % min. Creating PK for [Payments], both table(daily+main)', clock_timestamp()-first_time;
        first_time:= clock_timestamp();
        if not exists (select 1 from silver.payments) THEN
        alter table silver.payments
        add constraint payment_order_pk primary key (payment_id,order_id);
        end if;

        alter table silver.payments_daily
        add constraint payment_order_pk_daily primary key (payment_id,order_id);

        RAISE NOTICE 'PK task completed [Payments] in % min.', clock_timestamp()-first_time;

        raise notice 'Now inserting to [Payments] main table';
        first_time:= clock_timestamp();
        insert into silver.payments
        select  * from silver.payments_daily;

        raise notice 'Loaded completed in % min.', clock_timestamp()-first_time;
        
        

        drop table bronze.payments_raw_daily;



        /*Full load Order_items*/

        raise notice 'Data full loading to [ORDER_ITEMS] daily table...';
        first_time:= clock_timestamp();
        create unlogged table silver.order_items_daily(order_id VARCHAR(255),product_id VARCHAR(255),quantity NUMERIC(10,2),unit_price NUMERIC(10,2),total NUMERIC(10,2),created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);
        
        insert into silver.order_items_daily (order_id, product_id, quantity, unit_price, total, created_at_bronze)
        select distinct on (order_id,product_id) 
        * from bronze.order_items_raw_daily
        order by order_id,product_id, created_at_bronze desc;

        raise notice 'Loaded completed in % min, Creating PK for [ORDER_ITEMS], both table(daily+main)', clock_timestamp()-first_time;
        first_time:=clock_timestamp();
        if not exists (select 1 from silver.order_items) THEN
        alter table silver.order_items
        add constraint order_product_pk primary key (order_id, product_id);
        end if;

        alter table silver.order_items_daily
        add constraint order_product_pk_daily primary key (order_id, product_id);

        RAISE NOTICE 'PK loaded, complete [ORDER_ITEMS] in % min.', clock_timestamp()-first_time;

        raise notice 'Now inserting to [ORDER_ITEMS] main table';
        first_time:= clock_timestamp();
        insert into silver.order_items
        select * from silver.order_items_daily;

        raise notice 'loaded completed in % min.', clock_timestamp()-first_time;
    

        drop table bronze.order_items_raw_daily;



        /*Full load Customers*/
        raise notice 'Data full load to [CUSTOMERS] daily table...';
        first_time=clock_timestamp();
        create unlogged table silver.customers_daily(customer_id VARCHAR(255),name VARCHAR(255),signup_date date,created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);
        insert into silver.customers_daily(customer_id,name,signup_date,created_at_bronze)
        select distinct on (customer_id) * 
        from bronze.customers_raw_daily
        order by customer_id, created_at_bronze desc;

        raise notice 'Loaded completed in % min, Creating PK for [CUSTOMERS], both table(daily+main)', clock_timestamp()-first_time;
        first_time:=clock_timestamp();
        if not exists (select 1 from silver.customers)then
        alter table silver.customers
        add constraint customer_id_pk primary key (customer_id);
        end if;

        alter table silver.customers_daily
        add constraint customer_id_pk_daily primary key (customer_id);

        RAISE NOTICE 'PK loaded, complete [CUSTOMERS] in % min.', clock_timestamp()-first_time;

        raise notice ' Now inserting to [CUSTOMERS] main table';
        first_time:=clock_timestamp();
        insert into silver.customers
        select  * from silver.customers_daily;

        raise notice 'Loaded completed in % min.', clock_timestamp()-first_time;


        drop table bronze.customers_raw_daily;



        /*Full load orders*/
        raise notice 'Data full load to [ORDERS] daily table...';
        first_time:=clock_timestamp();
        create unlogged table silver.orders_daily(order_id VARCHAR(255),customer_id VARCHAR(255),order_date date,status VARCHAR(255),created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);
        insert into silver.orders_daily(order_id,customer_id,order_date,status,created_at_bronze)
        select distinct on (order_id,customer_id) *
        from bronze.orders_raw_daily order BY
        order_id,customer_id,created_at_bronze desc;

        raise notice 'Full loaded completed in % min, Creating PK for [ORDERS], both table(daily+main)', clock_timestamp()-first_time;
        first_time:=clock_timestamp();
        if not exists (select 1 from silver.orders) then
        alter table silver.orders
        add constraint order_customer_pk primary key (order_id,customer_id);
        end if;

        alter table silver.orders_daily
        add constraint order_customer_pk_daily primary key (order_id,customer_id);

        RAISE NOTICE 'PK loaded, completed in [ORDERS] % min.', clock_timestamp()-first_time;

        raise notice 'Now inserting to [ORDERS] main table';
        first_time:=clock_timestamp();
        insert into silver.orders
        select * from silver.orders_daily;

        raise notice 'Loaded completed in % min.', clock_timestamp()-first_time;



        drop table bronze.orders_raw_daily;




        /*Full load products*/
        raise notice 'Data full loading to [Products] daily table...';
        first_time:=clock_timestamp();
        create unlogged table silver.products_daily(product_id VARCHAR(255),name VARCHAR(255),category VARCHAR(255),price NUMERIC(10,2),created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);
        insert into silver.products_daily(product_id,name,category,price,created_at_bronze)
        select distinct on(product_id) *
        from bronze.products_raw_daily order by 
        product_id, created_at_bronze desc;


        raise notice 'Full loaded completed in % min, Creating PK for [Products], daily table', clock_timestamp()-first_time;
        first_time:=clock_timestamp();
        IF NOT EXISTS (SELECT 1 FROM silver.products) THEN
        ALTER TABLE silver.products ADD CONSTRAINT product_pk PRIMARY KEY (product_id);
        END IF;
        alter table silver.products_daily
        add constraint product_pk_daily primary key (product_id);
        RAISE NOTICE 'PK loaded completed in [Products] % min.', clock_timestamp()-first_time;

        raise notice 'Now inserting to [Products] main table';
        first_time:=clock_timestamp();
        insert into silver.products
        select * from silver.products_daily;

        RAISE NOTICE 'Full data loaded completed in [Products] % min.', clock_timestamp()-first_time;

        drop table bronze.products_raw_daily;


end;
$$;

call silver.silver_import_full();


drop table silver.products;
create table silver.products(
        product_id varchar(100) ,
        name VARCHAR(255),
        category VARCHAR(255),
        price numeric(10,2),
        created_at_bronze timestamp ,
        created_at_silver timestamp default current_timestamp
);

==================


select count(*) from silver.customers;


alter table silver.order_items drop constraint order_product_pk;

alter table silver.customers drop constraint customer_id_pk;

alter table silver.orders drop constraint order_customer_pk;

alter table silver.products drop constraint product_pk;

alter table silver.payments drop constraint payment_order_pk;


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

            




select count(*) from bronze.order_items_raw;
select count(*) from silver.order_items;









show data_directory;