-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@silver

order_date_lookup


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
=============================== --PARALLEL IMPORT SILVER DAILY TABLES + VALIDATION--
===============================


/*Payments -- daily load*/
create or replace procedure silver.ingest_silver_daily_payments()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
validation_time interval;
rows_count int;
BEGIN


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

                call silver.payments_validation_optimized(insert_time, validation_time);

                RAISE NOTICE 'PK task started for [payments_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.payments_daily
                add constraint payment_order_pk_daily primary key (payment_id,order_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(payment_id) into rows_count from silver.payments_daily;

                update operational_log.payments_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time,
                silver_daily_validation_time = validation_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [payments_daily] in %.', clock_timestamp()-first_time;



end;
$$;





/*Order_items -- daily load*/
create or replace procedure silver.ingest_silver_daily_order_items()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
validation_time interval;
rows_count int;
BEGIN

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

                call silver.order_items_validation_optimized(insert_time, validation_time);

                RAISE NOTICE 'PK task started for [order_items_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.order_items_daily
                add constraint order_product_pk_daily primary key (order_id, product_id);

                pk_executing_time := clock_timestamp() - first_time;


                select count(order_id) into rows_count from silver.order_items_daily;

                update operational_log.order_items_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time,
                silver_daily_validation_time = validation_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [order_items_daily] in %.', clock_timestamp()-first_time;

end;
$$;


/*Order_items -- main silver table upsert*/



/*Orders -- daily load*/
create or replace procedure silver.ingest_silver_daily_orders()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
validation_time interval;
lookup_time interval;
rows_count int;
BEGIN

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

                call silver.orders_validation_optimized(insert_time, validation_time);

                RAISE NOTICE 'PK task started for [orders_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.orders_daily
                add constraint order_customer_pk_daily primary key (order_id, customer_id);

                pk_executing_time := clock_timestamp() - first_time;


                first_time := clock_timestamp();
                truncate table silver.order_date_lookup;
                insert into silver.order_date_lookup(order_id, order_date)
                select order_id, order_date
                from silver.orders_daily
                where order_date is not null;
                lookup_time := clock_timestamp() - first_time;
                validation_time := validation_time + lookup_time;

                select count(order_id) into rows_count from silver.orders_daily;

                update operational_log.orders_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time,
                silver_daily_validation_time = validation_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [orders_daily] in %.', clock_timestamp()-first_time;


end;
$$;




/*Customers -- daily load*/
create or replace procedure silver.ingest_silver_daily_customers()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
validation_time interval;
rows_count int;
BEGIN
 
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

                call silver.customer_validation_optimized(insert_time, validation_time);

                RAISE NOTICE 'PK task started for [customers_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.customers_daily
                add constraint customer_id_pk_daily primary key (customer_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(customer_id) into rows_count from silver.customers_daily;

                update operational_log.customers_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time,
                silver_daily_validation_time = validation_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [customers_daily] in %.', clock_timestamp()-first_time;

end;
$$;




/*Products -- daily load*/
create or replace procedure silver.ingest_silver_daily_products()
language PLPGSQL
as $$
declare
first_time timestamp;
insert_time interval;
pk_executing_time interval;
validation_time interval;
rows_count int;
BEGIN

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

                call silver.products_validation_optimized(insert_time, validation_time);

                RAISE NOTICE 'PK task started for [products_daily] table,,,';

                first_time := clock_timestamp();

                alter table silver.products_daily
                add constraint product_pk_daily primary key (product_id);

                pk_executing_time := clock_timestamp() - first_time;

                select count(product_id) into rows_count from silver.products_daily;

                update operational_log.products_log
                set silver_daily_row_count = rows_count,
                silver_daily_indexing_time = pk_executing_time,
                silver_daily_validation_time = validation_time
                where ingestion_id = (select ingestion_id from operational_log.ingestion_id);

                RAISE NOTICE 'PK task completed [products_daily] in %.', clock_timestamp()-first_time;

end;
$$;







show data_directory;

EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM bronze.order_items_raw_daily;




----------------------------------------------------------------------------------------------------------------

==============================================================
-- IMPORT TO SILVER RAW FROM SILVER DAILY -- START
==============================================================




/*Payments -- main silver table upsert*/
create or replace procedure silver.ingest_silver_raw_payments()
language PLPGSQL
as $$
DECLARE
local_ingestion_id int;
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN

        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);
        first_time := clock_timestamp();

        update silver.payments_raw_p p
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

        with inserted_rows as (
            insert into silver.payments_raw_p(payment_id,payment_date,method,order_id,order_date,total,created_at_bronze,source_file_id)
            select a.payment_id,a.payment_date,a.method,a.order_id,a.order_date,a.total,a.created_at_bronze,a.source_file_id
            from silver.payments_daily a
            on conflict (payment_id,order_id,payment_date) do nothing
            returning 1
        )
        select count(*) into local_rows_inserted_count from inserted_rows;
        insert_time := clock_timestamp() - first_time;

        update operational_log.payments_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_updated_row_count = COALESCE(local_rows_updated_count, 0),
        silver_main_inserted_row_count = COALESCE(local_rows_inserted_count, 0),
        silver_main_row_count = COALESCE(local_rows_updated_count, 0) + COALESCE(local_rows_inserted_count, 0),
        total_silver_process_executing_time = update_time + insert_time + (select (COALESCE(silver_daily_indexing_time, interval '0') + COALESCE(silver_daily_insert_executing_time, interval '0') + COALESCE(silver_daily_validation_time, interval '0')) from operational_log.payments_log where ingestion_id = local_ingestion_id )
        where ingestion_id = local_ingestion_id;

        RAISE NOTICE 'Data loaded to [payments] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        --drop table bronze.payments_raw_daily;
END;
$$;


call silver.ingest_silver_raw_payments();




create or replace procedure silver.ingest_silver_raw_order_items()
language PLPGSQL
as $$
DECLARE
local_ingestion_id int;
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        SET LOCAL work_mem = '2GB';

        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);
        first_time := clock_timestamp();

        update silver.order_items_raw_p a
        SET
        quantity = b.quantity,
        unit_price = b.unit_price,
        total = b.total,
        created_at_bronze = b.created_at_bronze,
        created_at_silver = current_timestamp
        from silver.order_items_daily b
        join silver.order_date_lookup l on b.order_id = l.order_id
        where a.order_id = b.order_id
        and a.product_id = b.product_id
        and a.order_date = l.order_date
        and (a.quantity,a.unit_price,a.total) is distinct from (b.quantity,b.unit_price,b.total);

        get diagnostics local_rows_updated_count = row_count;
        update_time := clock_timestamp() - first_time;
        raise notice 'update done';

        first_time := clock_timestamp();

        with inserted_rows as (
            insert into silver.order_items_raw_p(order_id,product_id,quantity,unit_price,total,order_date,created_at_bronze,source_file_id)
            select a.order_id, a.product_id, a.quantity, a.unit_price, a.total,
                   l.order_date,
                   a.created_at_bronze,
                   a.source_file_id
            from silver.order_items_daily a
            join silver.order_date_lookup l on a.order_id = l.order_id
            where l.order_date is not null
            on conflict (order_id,product_id,order_date) do nothing
            returning 1
        )
        select count(*) into local_rows_inserted_count from inserted_rows;
        insert_time := clock_timestamp() - first_time;

        raise notice 'insert done';

        update operational_log.order_items_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_updated_row_count = COALESCE(local_rows_updated_count, 0),
        silver_main_inserted_row_count = COALESCE(local_rows_inserted_count, 0),
        silver_main_row_count = COALESCE(local_rows_updated_count, 0) + COALESCE(local_rows_inserted_count, 0),
        total_silver_process_executing_time = update_time + insert_time + (select (COALESCE(silver_daily_indexing_time, interval '0') + COALESCE(silver_daily_insert_executing_time, interval '0') + COALESCE(silver_daily_validation_time, interval '0')) from operational_log.order_items_log where ingestion_id = local_ingestion_id )
        where ingestion_id = local_ingestion_id;

        RAISE NOTICE 'Data loaded to [order_items] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        --drop table bronze.order_items_raw_daily;
END;
$$;

call silver.ingest_silver_raw_order_items();


/*Orders -- main silver table upsert*/
create or replace procedure silver.ingest_silver_raw_orders()
language PLPGSQL
as $$
DECLARE
local_ingestion_id int;
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);
        first_time := clock_timestamp();

        update silver.orders_raw_p a
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

        with inserted_rows as (
            insert into silver.orders_raw_p(order_id,customer_id,order_date,status,created_at_bronze,source_file_id)
            select order_id,customer_id,order_date,status,created_at_bronze,source_file_id
            from silver.orders_daily a
            where order_date is not null
            on conflict (order_id,order_date) do nothing
            returning 1
        )
        select count(*) into local_rows_inserted_count from inserted_rows;

        insert_time := clock_timestamp() - first_time;



        update operational_log.orders_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_updated_row_count = COALESCE(local_rows_updated_count, 0),
        silver_main_inserted_row_count = COALESCE(local_rows_inserted_count, 0),
        silver_main_row_count = COALESCE(local_rows_updated_count, 0) + COALESCE(local_rows_inserted_count, 0),
        total_silver_process_executing_time = update_time + insert_time + (select (COALESCE(silver_daily_indexing_time, interval '0') + COALESCE(silver_daily_insert_executing_time, interval '0') + COALESCE(silver_daily_validation_time, interval '0')) from operational_log.orders_log where ingestion_id = local_ingestion_id )
        where ingestion_id = local_ingestion_id;

        RAISE NOTICE 'Data loaded to [orders] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        --drop table bronze.orders_raw_daily;
END;
$$;

call silver.ingest_silver_raw_orders();




/*Customers -- main silver table upsert (SCD Type 2)*/
create or replace procedure silver.ingest_silver_raw_customers()
language PLPGSQL
as $$
DECLARE
local_ingestion_id int;
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);
        first_time := clock_timestamp();

        update silver.customers_raw_p a
        set valid_to=current_date - interval '1 day',
        is_valid=false
        from silver.customers_daily b
        WHERE a.customer_id=b.customer_id
        and a.is_valid=true
        and (a.name,a.signup_date) is distinct from (b.name,b.signup_date);

        get diagnostics local_rows_updated_count = row_count;
        update_time := clock_timestamp() - first_time;

        first_time := clock_timestamp();

        insert into silver.customers_raw_p(customer_id,name,signup_date,created_at_bronze,
        created_at_silver,valid_from,valid_to,is_valid,source_file_id)
        select customer_id,name,signup_date,created_at_bronze,
        current_timestamp,current_date,'2050-01-01',true,source_file_id
        from silver.customers_daily
        where not exists(
        select 1 from silver.customers_raw_p where
        silver.customers_raw_p.customer_id=silver.customers_daily.customer_id
        and silver.customers_raw_p.is_valid=true
        )
        on conflict (customer_id,valid_from) do update
        set name              = excluded.name,
            signup_date       = excluded.signup_date,
            created_at_bronze = excluded.created_at_bronze,
            created_at_silver = excluded.created_at_silver,
            valid_to          = excluded.valid_to,
            is_valid          = excluded.is_valid;

        get diagnostics local_rows_inserted_count = row_count;
        insert_time := clock_timestamp() - first_time;

        update operational_log.customers_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_updated_row_count = COALESCE(local_rows_updated_count, 0),
        silver_main_inserted_row_count = COALESCE(local_rows_inserted_count, 0),
        silver_main_row_count = COALESCE(local_rows_inserted_count, 0),
        total_silver_process_executing_time = update_time + insert_time + (select (COALESCE(silver_daily_indexing_time, interval '0') + COALESCE(silver_daily_insert_executing_time, interval '0') + COALESCE(silver_daily_validation_time, interval '0')) from operational_log.customers_log where ingestion_id = local_ingestion_id)
        where ingestion_id = local_ingestion_id;

        RAISE NOTICE 'Data loaded to [customers] main table. Historized: %, New versions inserted: %', local_rows_updated_count, local_rows_inserted_count;

        --drop table bronze.customers_raw_daily;
END;
$$;

call silver.ingest_silver_raw_customers();







/*Products -- main silver table upsert*/
create or replace procedure silver.ingest_silver_raw_products()
language PLPGSQL
as $$
DECLARE
local_ingestion_id int;
local_rows_updated_count int;
local_rows_inserted_count int;
first_time timestamp;
update_time interval;
insert_time interval;
BEGIN
        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);
        first_time := clock_timestamp();

        update silver.products_raw a
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

        with inserted_rows as (
            insert into silver.products_raw(product_id,name,category,price,created_at_bronze,source_file_id)
            select product_id,name,category,price,created_at_bronze,source_file_id
            from silver.products_daily
            on conflict (product_id) do nothing
            returning 1
        )
        select count(*) into local_rows_inserted_count from inserted_rows;
        insert_time := clock_timestamp() - first_time;

        update operational_log.products_log
        set silver_main_update_executing_time = update_time,
        silver_main_insert_executing_time = insert_time,
        silver_main_updated_row_count = COALESCE(local_rows_updated_count, 0),
        silver_main_inserted_row_count = COALESCE(local_rows_inserted_count, 0),
        silver_main_row_count = COALESCE(local_rows_updated_count, 0) + COALESCE(local_rows_inserted_count, 0),
        total_silver_process_executing_time = update_time + insert_time + (select (COALESCE(silver_daily_indexing_time, interval '0') + COALESCE(silver_daily_insert_executing_time, interval '0') + COALESCE(silver_daily_validation_time, interval '0')) from operational_log.products_log where ingestion_id = local_ingestion_id )
        where ingestion_id = local_ingestion_id;

        RAISE NOTICE 'Data loaded to [products] main table. Updated: %, Inserted: %', local_rows_updated_count, local_rows_inserted_count;

        --drop table bronze.products_raw_daily;
END;
$$;

call silver.ingest_silver_raw_products();




