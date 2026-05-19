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
=============================== --PARALLEL IMPORT SILVER--
===============================


/*Payments -- daily load*/
create or replace procedure silver.ingest_silver_daily_payments()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
rows_count int;
BEGIN

        if exists(select 1 from information_schema.tables where table_name='payments_raw_daily' and table_schema='bronze')
        then
                raise notice 'started,,,,';
                first_time := clock_timestamp();

                insert into silver.payments_daily(payment_id,method,order_id,order_date,total,payment_date,created_at_bronze,source_file_id)
                select distinct on(payment_id,order_id)
                        lower(trim(payment_id))::varchar(255),
                        lower(trim(method))::varchar(50),
                        lower(trim(order_id))::varchar(255),
                        case when nullif(trim(order_date),'') ~'^\d{4}-\d{2}-\d{2}$'
                        then to_date(trim(order_date),'YYYY-MM-DD')
                        end,
                        trim(total)::numeric(10,2),
                        case when nullif(trim(payment_date),'') ~ '^\d{4}-\d{2}-\d{2}$'
                        then to_date(trim(payment_date),'YYYY-MM-DD')
                        end,
                        created_at_bronze,
                        source_file_id
                from bronze.payments_raw_daily order BY
                payment_id,order_id,created_at_bronze desc;

                insert_time := clock_timestamp() - first_time;

                RAISE NOTICE 'Data loaded to [payments_daily] table in %.', insert_time;

                call silver.payments_validation_optimized(insert_time);

                RAISE NOTICE 'PK task started for [payments_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.payments_daily
                add constraint payment_order_pk_daily primary key (payment_id,order_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(payment_id) into rows_count from silver.payments_daily;

                update operational_log.payments_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [payments_daily] in %.', clock_timestamp()-first_time;

        else
                RAISE NOTICE 'No new data to load for [payments]...';
        end if;

end;
$$;


/*Payments -- main silver table upsert*/
create or replace procedure silver.ingest_silver_raw_payments()
language PLPGSQL
as $$
DECLARE
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        first_time := clock_timestamp();

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

        get DIAGNOSTICS local_rows_updated_count = ROW_COUNT;
        update_time := clock_timestamp() - first_time;

        first_time := clock_timestamp();

        insert into silver.payments(payment_id,payment_date,method,order_id,order_date,total,created_at_bronze,created_at_silver)
        select a.payment_id,a.payment_date,a.method,a.order_id,a.order_date,a.total,a.created_at_bronze,current_timestamp
        from silver.payments_daily a
        where not exists (select 1 from silver.payments pd
        where pd.payment_id=a.payment_id and pd.order_id=a.order_id);

        get DIAGNOSTICS local_rows_inserted_count = ROW_COUNT;
        insert_time := clock_timestamp() - first_time;

        update operational_log.payments_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_row_count = (select count(*) from silver.payments)
        where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

        RAISE NOTICE 'Data loaded to [payments] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        drop table bronze.payments_raw_daily;
END;
$$;


/*Order_items -- daily load*/
create or replace procedure silver.ingest_silver_daily_order_items()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
rows_count int;
BEGIN
        if exists(select 1 from information_schema.tables where table_name='order_items_raw_daily' and table_schema='bronze')
        then
                raise notice 'started [order_items_daily],,,,';
                first_time := clock_timestamp();

                insert into silver.order_items_daily(order_id,product_id,quantity,unit_price,total,created_at_bronze,source_file_id)
                select distinct on(order_id,product_id)
                        lower(trim(order_id))::varchar(255),
                        lower(trim(product_id))::varchar(255),
                        trim(quantity::text)::numeric(10,2),
                        trim(unit_price::text)::numeric(10,2),
                        trim(total::text)::numeric(10,2),
                        created_at_bronze,
                        source_file_id
                from bronze.order_items_raw_daily order by
                order_id, product_id, created_at_bronze desc;

                insert_time := clock_timestamp() - first_time;

                RAISE NOTICE 'Data loaded to [order_items_daily] table in %.', insert_time;

                call silver.order_items_validation_optimized(insert_time);

                RAISE NOTICE 'PK task started for [order_items_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.order_items_daily
                add constraint order_product_pk_daily primary key (order_id, product_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(order_id) into rows_count from silver.order_items_daily;

                update operational_log.order_items_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [order_items_daily] in %.', clock_timestamp()-first_time;

        else
                RAISE NOTICE 'No new data to load for [order_items]...';
        end if;
end;
$$;


/*Order_items -- main silver table upsert*/
create or replace procedure silver.ingest_silver_raw_order_items()
language PLPGSQL
as $$
DECLARE
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        if not exists(select 1 from pg_constraint where conname='order_product_pk')
        then
                alter table silver.order_items
                add constraint order_product_pk primary key (order_id, product_id);
        end if;

        first_time := clock_timestamp();

        update silver.order_items a
        SET
        quantity = b.quantity,
        unit_price = b.unit_price,
        total = b.total,
        created_at_bronze = b.created_at_bronze,
        created_at_silver = current_timestamp
        from silver.order_items_daily b
        where a.order_id = b.order_id and a.product_id = b.product_id
        and (a.quantity,a.unit_price,a.total) is distinct from (b.quantity,b.unit_price,b.total);

        get diagnostics local_rows_updated_count = row_count;
        update_time := clock_timestamp() - first_time;

        first_time := clock_timestamp();

        insert into silver.order_items(order_id,product_id,quantity,unit_price,total,created_at_bronze,created_at_silver)
        select order_id,product_id,quantity,unit_price,total,created_at_bronze,current_timestamp
        from silver.order_items_daily a
        where not exists (select 1 from silver.order_items o
        where o.order_id=a.order_id and o.product_id=a.product_id);

        get diagnostics local_rows_inserted_count = row_count;
        insert_time := clock_timestamp() - first_time;

        update operational_log.order_items_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_row_count = (select count(*) from silver.order_items)
        where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

        RAISE NOTICE 'Data loaded to [order_items] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        drop table bronze.order_items_raw_daily;
END;
$$;


/*Orders -- daily load*/
create or replace procedure silver.ingest_silver_daily_orders()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
rows_count int;
BEGIN
        if exists(select 1 from information_schema.tables where table_name='orders_raw_daily' and table_schema='bronze')
        then
                raise notice 'started [orders_daily],,,,';
                first_time := clock_timestamp();

                insert into silver.orders_daily(order_id,customer_id,order_date,status,created_at_bronze,source_file_id)
                select distinct on(order_id,customer_id)
                        lower(trim(order_id))::varchar(255),
                        lower(trim(customer_id))::varchar(255),
                        case when nullif(trim(order_date),'') ~ '^\d{4}-\d{2}-\d{2}$'
                        then to_date(trim(order_date),'YYYY-MM-DD')
                        end,
                        lower(trim(status))::varchar(50),
                        created_at_bronze,
                        source_file_id
                from bronze.orders_raw_daily order by
                order_id, customer_id, created_at_bronze desc;

                insert_time := clock_timestamp() - first_time;

                RAISE NOTICE 'Data loaded to [orders_daily] table in %.', insert_time;

                call silver.orders_validation_optimized(insert_time);

                RAISE NOTICE 'PK task started for [orders_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.orders_daily
                add constraint order_customer_pk_daily primary key (order_id, customer_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(order_id) into rows_count from silver.orders_daily;

                update operational_log.orders_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [orders_daily] in %.', clock_timestamp()-first_time;

        else
                RAISE NOTICE 'No new data to load for [orders]...';
        end if;
end;
$$;


/*Orders -- main silver table upsert*/
create or replace procedure silver.ingest_silver_raw_orders()
language PLPGSQL
as $$
DECLARE
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        if not exists(select 1 from pg_constraint where conname='order_customer_pk')
        then
                alter table silver.orders
                add constraint order_customer_pk primary key (order_id, customer_id);
        end if;

        first_time := clock_timestamp();

        update silver.orders a
        SET
        status = b.status,
        order_date = b.order_date,
        created_at_bronze = b.created_at_bronze,
        created_at_silver = current_timestamp
        from silver.orders_daily b
        where a.order_id = b.order_id and a.customer_id = b.customer_id
        and (a.status,a.order_date) is distinct from (b.status,b.order_date);

        get diagnostics local_rows_updated_count = row_count;
        update_time := clock_timestamp() - first_time;

        first_time := clock_timestamp();

        insert into silver.orders(order_id,customer_id,order_date,status,created_at_bronze,created_at_silver)
        select order_id,customer_id,order_date,status,created_at_bronze,current_timestamp
        from silver.orders_daily a
        where not exists (select 1 from silver.orders o
        where o.order_id=a.order_id and o.customer_id=a.customer_id);

        get diagnostics local_rows_inserted_count = row_count;
        insert_time := clock_timestamp() - first_time;

        update operational_log.orders_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_row_count = (select count(*) from silver.orders)
        where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

        RAISE NOTICE 'Data loaded to [orders] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        drop table bronze.orders_raw_daily;
END;
$$;


/*Customers -- daily load*/
create or replace procedure silver.ingest_silver_daily_customers()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
rows_count int;
BEGIN
        if exists(select 1 from information_schema.tables where table_name='customers_raw_daily' and table_schema='bronze')
        then
                raise notice 'started [customers_daily],,,,';
                first_time := clock_timestamp();

                insert into silver.customers_daily(customer_id,name,signup_date,created_at_bronze,source_file_id)
                select distinct on(customer_id)
                        lower(trim(customer_id))::varchar(255),
                        lower(trim(name))::varchar(255),
                        case when nullif(trim(signup_date),'') ~ '^\d{4}-\d{2}-\d{2}$'
                        then to_date(trim(signup_date),'YYYY-MM-DD')
                        end,
                        created_at_bronze,
                        source_file_id
                from bronze.customers_raw_daily order by
                customer_id, created_at_bronze desc;

                insert_time := clock_timestamp() - first_time;

                RAISE NOTICE 'Data loaded to [customers_daily] table in %.', insert_time;

                call silver.customer_validation_optimized(insert_time);

                RAISE NOTICE 'PK task started for [customers_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.customers_daily
                add constraint customer_id_pk_daily primary key (customer_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(customer_id) into rows_count from silver.customers_daily;

                update operational_log.customers_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [customers_daily] in %.', clock_timestamp()-first_time;

        else
                RAISE NOTICE 'No new data to load for [customers]...';
        end if;
end;
$$;


/*Customers -- main silver table upsert (SCD Type 2)*/
create or replace procedure silver.ingest_silver_raw_customers()
language PLPGSQL
as $$
DECLARE
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        if not exists(select 1 from pg_constraint where conname='customer_id_pk')
        then
                alter table silver.customers
                add constraint customer_id_pk primary key (customer_id,valid_from);
        end if;

        if not exists(select 1 from pg_indexes where indexname='partial_index_customer')
        then
                create unique index partial_index_customer on silver.customers(customer_id)
                where is_valid=true;
        end if;

        first_time := clock_timestamp();

        update silver.customers a
        set valid_to=current_date - interval '1 day',
        is_valid=false
        from silver.customers_daily b
        WHERE a.customer_id=b.customer_id
        and a.is_valid=true
        and (a.name,a.signup_date) is distinct from (b.name,b.signup_date);

        get diagnostics local_rows_updated_count = row_count;
        update_time := clock_timestamp() - first_time;

        first_time := clock_timestamp();

        insert into silver.customers(customer_id,name,signup_date,created_at_bronze,
        created_at_silver,valid_from,valid_to,is_valid)
        select customer_id,name,signup_date,created_at_bronze,
        current_timestamp,current_date,'2050-01-01',true
        from silver.customers_daily
        where not exists(
        select 1 from silver.customers where
        silver.customers.customer_id=silver.customers_daily.customer_id
        and silver.customers.is_valid=true
        );

        get diagnostics local_rows_inserted_count = row_count;
        insert_time := clock_timestamp() - first_time;

        update operational_log.customers_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_row_count = (select count(*) from silver.customers where is_valid=true)
        where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

        RAISE NOTICE 'Data loaded to [customers] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        drop table bronze.customers_raw_daily;
END;
$$;


/*Products -- daily load*/
create or replace procedure silver.ingest_silver_daily_products()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
rows_count int;
BEGIN
        if exists(select 1 from information_schema.tables where table_name='products_raw_daily' and table_schema='bronze')
        then
                raise notice 'started [products_daily],,,,';
                first_time := clock_timestamp();

                insert into silver.products_daily(product_id,name,category,price,created_at_bronze,source_file_id)
                select distinct on(product_id)
                        lower(trim(product_id))::varchar(255),
                        lower(trim(name))::varchar(255),
                        lower(trim(category))::varchar(255),
                        trim(price::text)::numeric(10,2),
                        created_at_bronze,
                        source_file_id
                from bronze.products_raw_daily order by
                product_id, created_at_bronze desc;

                insert_time := clock_timestamp() - first_time;

                RAISE NOTICE 'Data loaded to [products_daily] table in %.', insert_time;

                call silver.products_validation_optimized(insert_time);

                RAISE NOTICE 'PK task started for [products_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.products_daily
                add constraint product_pk_daily primary key (product_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(product_id) into rows_count from silver.products_daily;

                update operational_log.products_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [products_daily] in %.', clock_timestamp()-first_time;

        else
                RAISE NOTICE 'No new data to load for [products]...';
        end if;
end;
$$;


/*Products -- main silver table upsert*/
create or replace procedure silver.ingest_silver_raw_products()
language PLPGSQL
as $$
DECLARE
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        if not exists(select 1 from pg_constraint where conname='product_pk')
        then
                alter table silver.products
                add constraint product_pk primary key (product_id);
        end if;

        first_time := clock_timestamp();

        update silver.products a
        SET
        name = b.name,
        category = b.category,
        price = b.price,
        created_at_bronze = b.created_at_bronze,
        created_at_silver = current_timestamp
        from silver.products_daily b
        where a.product_id = b.product_id
        and (a.name,a.category,a.price) is distinct from (b.name,b.category,b.price);

        get diagnostics local_rows_updated_count = row_count;
        update_time := clock_timestamp() - first_time;

        first_time := clock_timestamp();

        insert into silver.products(product_id,name,category,price,created_at_bronze,created_at_silver)
        select product_id,name,category,price,created_at_bronze,current_timestamp
        from silver.products_daily a
        where not exists (select 1 from silver.products p
        where p.product_id=a.product_id);

        get diagnostics local_rows_inserted_count = row_count;
        insert_time := clock_timestamp() - first_time;

        update operational_log.products_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_row_count = (select count(*) from silver.products)
        where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

        RAISE NOTICE 'Data loaded to [products] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        drop table bronze.products_raw_daily;
END;
$$;


show data_directory;

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM bronze.order_items_raw_daily;
