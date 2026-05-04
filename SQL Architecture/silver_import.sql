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
            created_at_bronze timestamp default current_timestamp,
            created_at_silver timestamp default current_timestamp 
        );

        drop table if exists silver.products cascade;
        create table silver.products (
            product_id varchar(100) ,
            name VARCHAR(255),
            category VARCHAR(255),
            price numeric(10,2),
            created_at_bronze timestamp default current_timestamp,
            created_at_silver timestamp default current_timestamp
        );

        drop table if exists silver.orders cascade;
        create table silver.orders (
            order_id VARCHAR(255) ,
                customer_id VARCHAR(255),
                order_date date,
                status VARCHAR(50),
                created_at_bronze timestamp default current_timestamp,
                created_at_silver timestamp default current_timestamp

        );


        drop table if exists silver.order_items cascade;
        create table silver.order_items (

                order_id VARCHAR(255) ,
                product_id VARCHAR(255) ,
                quantity numeric(10,2),
                unit_price numeric(10,2),
                total numeric(10,2),
                created_at_bronze timestamp default current_timestamp,
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
                    created_at_bronze timestamp default current_timestamp,
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
        raise notice 'Data full load to [Payments]';
        truncate table  silver.payments;

        insert into silver.payments(payment_id,payment_date,method,order_id,order_date,total,created_at_bronze)
        select distinct on(payment_id,order_id) payment_id,payment_date,method,order_id,order_date,total,created_at_bronze
        from bronze.payments_raw order BY
        payment_id,order_id,created_at_bronze desc;

        raise notice 'loaded completed in % min. Creating PK for [Payments]', clock_timestamp()-first_time;
        second_time:= clock_timestamp();
        alter table silver.payments
        add constraint payment_order_pk primary key (payment_id,order_id);

        RAISE NOTICE 'PK task completed [Payments] in % min.', clock_timestamp()-second_time;



        /*Full load Order_items*/

        raise notice 'Data full load to [ORDER_ITEMS]';
        first_time:= clock_timestamp();

        TRUNCATE  table silver.order_items;
        insert into silver.order_items (order_id, product_id, quantity, unit_price, total, created_at_bronze)
        select distinct on (order_id,product_id) 
        * from bronze.order_items_raw
        order by order_id,product_id, created_at_bronze desc;
        
        raise notice 'Loaded completed in % min, Creating PK for [ORDER_ITEMS]', clock_timestamp()-first_time;
        second_time:=clock_timestamp();
        alter table silver.order_items
        add constraint order_product_pk primary key (order_id, product_id);

        RAISE NOTICE 'PK loaded, complete [ORDER_ITEMS] in % min.', clock_timestamp()-second_time;



        /*Full load Customers*/
        raise notice 'Data full load to [CUSTOMERS]';
        first_time=clock_timestamp();
        TRUNCATE  table silver.customers;
        insert into silver.customers(customer_id,name,signup_date,created_at_bronze)
        select distinct on (customer_id) * 
        from bronze.customers_raw
        order by customer_id, created_at_bronze desc;

        raise notice 'Loaded completed in % min, Creating PK for [CUSTOMERS]', clock_timestamp()-first_time;
        second_time:=clock_timestamp();
        alter table silver.customers
        add constraint customer_id_pk primary key (customer_id);

        RAISE NOTICE 'PK loaded, complete [CUSTOMERS] in % min.', clock_timestamp()-second_time;



        /*Full load orders*/
        raise notice 'Data full load to [ORDERS]';
        first_time:=clock_timestamp();

        truncate table  silver.orders;
        insert into silver.orders(order_id,customer_id,order_date,status,created_at_bronze)
        select distinct on (order_id,customer_id) *
        from bronze.orders_raw order BY
        order_id,customer_id,created_at_bronze desc;

        raise notice 'Full loaded completed in % min, Creating PK for [ORDERS]', clock_timestamp()-first_time;
        second_time:=clock_timestamp();
        alter table silver.orders
        add constraint order_customer_pk primary key (order_id,customer_id);

        RAISE NOTICE 'PK loaded, completed in [ORDERS] % min.', clock_timestamp()-second_time;




        /*Full load products*/
        raise notice 'Data full load to [Products]';
        first_time:=clock_timestamp();

        TRUNCATE table  silver.products;
        insert into silver.products(product_id,name,category,price,created_at_bronze)
        select distinct on(product_id) *
        from bronze.products_raw order by 
        product_id, created_at_bronze desc;

        raise notice 'Full load completed in % min, Creating PK for [Products]', clock_timestamp()-first_time;
        second_time:=clock_timestamp();
        alter table silver.products
        add constraint product_pk primary key (product_id);

        RAISE NOTICE 'PK loaded, completed in [Products] % min.', clock_timestamp()-second_time;


end;
$$;

call silver.silver_import_full();


===============

/*
Create Incremental Load: Only run this procedure after running FULL LOAD once.
*/

CREATE OR REPLACE PROCEDURE silver_import_incremental()
LANGUAGE plpgsql
AS $$
DECLARE
    check_date timestamp;
    start_time timestamp;
    section_start timestamp;
BEGIN
    -- 1. Performance Optimizations (Session Level)
    SET LOCAL work_mem = '512MB';
    SET LOCAL maintenance_work_mem = '512MB';
    SET LOCAL synchronous_commit = OFF;
    SET LOCAL parallel_tuple_cost = 0;
    SET LOCAL parallel_setup_cost = 0;

    start_time := clock_timestamp();

    /* -------------------------------------------------------------------------
       ORDER_ITEMS TABLE
    ------------------------------------------------------------------------- */
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting [ORDER_ITEMS]...';

    SELECT COALESCE(MAX(created_at_bronze), '1900-01-01 00:00:00') 
    INTO check_date FROM silver.order_items;

    DROP TABLE IF EXISTS silver_order_items_stage;
    
    CREATE UNLOGGED TABLE silver_order_items_stage AS
    SELECT DISTINCT ON (order_id, product_id) *
    FROM bronze.order_items_raw
    WHERE created_at_bronze > check_date
    ORDER BY order_id, product_id, created_at_bronze DESC;

    CREATE INDEX ON silver_order_items_stage (order_id, product_id);
    ANALYZE silver_order_items_stage;

    INSERT INTO silver.order_items (order_id, product_id, quantity, unit_price, total, created_at_bronze)
    SELECT order_id, product_id, quantity, unit_price, total, created_at_bronze
    FROM silver_order_items_stage
    ON CONFLICT (order_id, product_id)
    DO UPDATE SET
        quantity = EXCLUDED.quantity,
        unit_price = EXCLUDED.unit_price,
        total = EXCLUDED.total,
        created_at_bronze = EXCLUDED.created_at_bronze,
        created_at_silver = CURRENT_TIMESTAMP
    WHERE (silver.order_items.quantity IS DISTINCT FROM EXCLUDED.quantity)
       OR (silver.order_items.unit_price IS DISTINCT FROM EXCLUDED.unit_price)
       OR (silver.order_items.total IS DISTINCT FROM EXCLUDED.total);

    DROP TABLE IF EXISTS silver_order_items_stage;
    RAISE NOTICE 'Completed [ORDER_ITEMS] in %', clock_timestamp() - section_start;


    /* -------------------------------------------------------------------------
       CUSTOMERS TABLE
    ------------------------------------------------------------------------- */
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting [CUSTOMERS]...';

    SELECT COALESCE(MAX(created_at_bronze), '1900-01-01 00:00:00') 
    INTO check_date FROM silver.customers;

    DROP TABLE IF EXISTS silver_customers_stage;

    CREATE UNLOGGED TABLE silver_customers_stage AS
    SELECT DISTINCT ON (customer_id) *
    FROM bronze.customers_raw
    WHERE created_at_bronze > check_date
    ORDER BY customer_id, created_at_bronze DESC;

    CREATE INDEX ON silver_customers_stage (customer_id);
    ANALYZE silver_customers_stage;

    INSERT INTO silver.customers (customer_id, name, signup_date, created_at_bronze)
    SELECT customer_id, name, signup_date, created_at_bronze
    FROM silver_customers_stage
    ON CONFLICT (customer_id)
    DO UPDATE SET
        name = EXCLUDED.name,
        signup_date = EXCLUDED.signup_date,
        created_at_bronze = EXCLUDED.created_at_bronze,
        created_at_silver = CURRENT_TIMESTAMP
    WHERE (silver.customers.name IS DISTINCT FROM EXCLUDED.name)
       OR (silver.customers.signup_date IS DISTINCT FROM EXCLUDED.signup_date);

    DROP TABLE IF EXISTS silver_customers_stage;
    RAISE NOTICE 'Completed [CUSTOMERS] in %', clock_timestamp() - section_start;


    /* -------------------------------------------------------------------------
       ORDERS TABLE
    ------------------------------------------------------------------------- */
    section_start := clock_timestamp();
    RAISE NOTICE 'Starting [ORDERS]...';

    SELECT COALESCE(MAX(created_at_bronze), '1900-01-01 00:00:00') 
    INTO check_date FROM silver.orders;

    DROP TABLE IF EXISTS silver_orders_stage;

    CREATE UNLOGGED TABLE silver_orders_stage AS
    SELECT DISTINCT ON (order_id, customer_id) *
    FROM bronze.orders_raw
    WHERE created_at_bronze > check_date
    ORDER BY order_id, customer_id, created_at_bronze DESC;

    CREATE INDEX ON silver_orders_stage (order_id, customer_id);
    ANALYZE silver_orders_stage;

    INSERT INTO silver.orders (order_id, customer_id, order_date, status, created_at_bronze)
    SELECT order_id, customer_id, order_date, status, created_at_bronze
    FROM silver_orders_stage
    ON CONFLICT (order_id, customer_id)
    DO UPDATE SET
        order_date = EXCLUDED.order_date,
        status = EXCLUDED.status,
        created_at_bronze = EXCLUDED.created_at_bronze,
        created_at_silver = CURRENT_TIMESTAMP
    WHERE (silver.orders.order_date IS DISTINCT FROM EXCLUDED.order_date)
       OR (silver.orders.status IS DISTINCT FROM EXCLUDED.status);

    DROP TABLE IF EXISTS silver_orders_stage;
    RAISE NOTICE 'Completed [ORDERS] in %', clock_timestamp() - section_start;

    RAISE NOTICE 'Total execution time: %', clock_timestamp() - start_time;


     /* -------------------------------------------------------------------------
    PAYMENTS TABLE
    ------------------------------------------------------------------------ */ 

    section_start := clock_timestamp();
    RAISE NOTICE 'Starting [PAYMENTS]...';

    SELECT COALESCE(MAX(created_at_bronze), '1900-01-01 00:00:00') 
    INTO check_date FROM silver.payments;

    DROP TABLE IF EXISTS silver_payments_stage;

    CREATE UNLOGGED TABLE silver_payments_stage AS
    SELECT DISTINCT ON (payment_id, order_id) *
    FROM bronze.payments_raw
    WHERE created_at_bronze > check_date
    ORDER BY payment_id, order_id, created_at_bronze DESC;

    CREATE INDEX ON silver_payments_stage (payment_id, order_id);
    ANALYZE silver_payments_stage;

    INSERT INTO silver.payments (payment_id, payment_date, method, order_id,order_date,total, created_at_bronze)
    SELECT payment_id, payment_date, method, order_id,order_date,total, created_at_bronze
    FROM silver_payments_stage
    ON CONFLICT (payment_id, order_id)
    DO UPDATE SET
        payment_date = EXCLUDED.payment_date,
        method = EXCLUDED.method,
        order_date = EXCLUDED.order_date,
        total=EXCLUDED.total,
        created_at_silver = CURRENT_TIMESTAMP
    WHERE (silver.payments.payment_date IS DISTINCT FROM EXCLUDED.payment_date)
       OR (silver.payments.method IS DISTINCT FROM EXCLUDED.method) or 
       (silver.payments.order_date IS DISTINCT FROM EXCLUDED.order_date)
       OR (silver.payments.total IS DISTINCT FROM EXCLUDED.total);

    DROP TABLE IF EXISTS silver_payments_stage;
    RAISE NOTICE 'Completed [PAYMENTS] in %', clock_timestamp() - section_start;

    RAISE NOTICE 'Total execution time: %', clock_timestamp() - start_time;


    /* -------------------------------------------------------------------------
    PRODUCTS TABLE
    ------------------------------------------------------------------------ */ 

    section_start := clock_timestamp();
    RAISE NOTICE 'Starting [PRODUCTS]...';

    SELECT COALESCE(MAX(created_at_bronze), '1900-01-01 00:00:00') 
    INTO check_date FROM silver.products;

    DROP TABLE IF EXISTS silver_products_stage;

    CREATE UNLOGGED TABLE silver_products_stage AS
    SELECT DISTINCT ON (product_id) *
    FROM bronze.products_raw
    WHERE created_at_bronze > check_date
    ORDER BY product_id, created_at_bronze DESC;

    CREATE INDEX ON silver_products_stage (product_id);
    ANALYZE silver_products_stage;

    INSERT INTO silver.products (product_id, name, category, price, created_at_bronze)
    SELECT product_id, name, category, price, created_at_bronze
    FROM silver_products_stage
    ON CONFLICT (product_id)
    DO UPDATE SET
        name = EXCLUDED.name,
        category = EXCLUDED.category,
        price = EXCLUDED.price,
        created_at_silver = CURRENT_TIMESTAMP
    WHERE (silver.products.name IS DISTINCT FROM EXCLUDED.name)
       OR (silver.products.category IS DISTINCT FROM EXCLUDED.category) or 
       (silver.products.price IS DISTINCT FROM EXCLUDED.price);

    DROP TABLE IF EXISTS silver_products_stage;
    RAISE NOTICE 'Completed [PRODUCTS] in %', clock_timestamp() - section_start;

    RAISE NOTICE 'Total execution time: %', clock_timestamp() - start_time;

END;
$$;


call silver_import_incremental();




            














show data_directory;