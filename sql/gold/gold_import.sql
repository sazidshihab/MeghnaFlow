-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@gold



==============================================================
--GOLD INDEXES (run once at setup, idempotent) -- START--
==============================================================

/*
Analytical indexes on fact tables.
FK columns are not indexed by default — these are required for fast
dim-to-fact joins in BI queries (customer_key, product_key, date_key).
CREATE INDEX IF NOT EXISTS makes this safe to re-run.
*/
create or replace procedure gold.create_gold_indexes()
language plpgsql as $$
begin

        create index if not exists idx_fact_sales_customer_key
            on gold.fact_sales(customer_key);

        create index if not exists idx_fact_sales_product_key
            on gold.fact_sales(product_key);

        create index if not exists idx_fact_sales_date_key
            on gold.fact_sales(date_key);

        create index if not exists idx_fact_payments_customer_key
            on gold.fact_payments(customer_key);

        create index if not exists idx_fact_payments_order_date_key
            on gold.fact_payments(order_date_key);

        create index if not exists idx_fact_payments_payment_date_key
            on gold.fact_payments(payment_date_key);

        create index if not exists idx_fact_payments_order_id
            on gold.fact_payments(order_id);

end;
$$;

call gold.create_gold_indexes();

==============================================================
--GOLD INDEXES -- END--
==============================================================




==============================================================
--GOLD LOAD: DIM_CUSTOMERS -- START--
==============================================================

/*
SCD-1 upsert from silver.customers_daily (today's validated customers).
Drives from the small daily table — never scans customers_raw_p.
first_seen_at is set on INSERT and never overwritten on UPDATE.
*/
create or replace procedure gold.load_gold_dim_customers()
language plpgsql as $$
declare
        local_ingestion_id  int;
        first_time          timestamp;
        load_time           interval;
        rows_upserted       int;
        v_bronze_count      int;
        v_silver_count      int;
begin

        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);

        select count(source_file_id) into v_bronze_count
        from operational_log.bronze_ingest_log
        where table_name = 'customers'
        and ingestion_for = 'bronze_daily'
        and ingestion_id = local_ingestion_id;

        select count(distinct source_file_id) into v_silver_count
        from silver.customers_daily;

        if v_silver_count < v_bronze_count then
            raise exception 'Gate failed [dim_customers]: customers — %/% source files reached silver_daily',
                v_silver_count, v_bronze_count;
        end if;

        first_time := clock_timestamp();

        with upserted as (
                insert into gold.dim_customers(
                        customer_id, name, signup_date, first_seen_at, updated_at_gold
                )
                select
                        customer_id,
                        name,
                        signup_date,
                        created_at_bronze,
                        current_timestamp
                from silver.customers_daily
                on conflict (customer_id) do update
                set
                        name            = excluded.name,
                        signup_date     = excluded.signup_date,
                        updated_at_gold = current_timestamp
                where (gold.dim_customers.name, gold.dim_customers.signup_date)
                   is distinct from (excluded.name, excluded.signup_date)
                returning 1
        )
        select count(*) into rows_upserted from upserted;

        load_time := clock_timestamp() - first_time;

        insert into operational_log.gold_load_log(ingestion_id, table_name, rows_upserted, executing_time)
        values (local_ingestion_id, 'dim_customers', coalesce(rows_upserted, 0), load_time)
        on conflict (ingestion_id, table_name) do update
        set rows_upserted  = excluded.rows_upserted,
            executing_time = excluded.executing_time;

        raise notice 'Gold dim_customers loaded. Upserted: %', rows_upserted;

end;
$$;

call gold.load_gold_dim_customers();

==============================================================
--GOLD LOAD: DIM_CUSTOMERS -- END--
==============================================================




==============================================================
--GOLD LOAD: DIM_PRODUCTS -- START--
==============================================================

/*
SCD-1 upsert from silver.products_daily (today's validated products).
price and category always reflect the latest value.
*/
create or replace procedure gold.load_gold_dim_products()
language plpgsql as $$
declare
        local_ingestion_id  int;
        first_time          timestamp;
        load_time           interval;
        rows_upserted       int;
        v_bronze_count      int;
        v_silver_count      int;
begin

        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);

        select count(source_file_id) into v_bronze_count
        from operational_log.bronze_ingest_log
        where table_name = 'products'
        and ingestion_for = 'bronze_daily'
        and ingestion_id = local_ingestion_id;

        select count(distinct source_file_id) into v_silver_count
        from silver.products_daily;

        if v_silver_count < v_bronze_count then
            raise exception 'Gate failed [dim_products]: products — %/% source files reached silver_daily',
                v_silver_count, v_bronze_count;
        end if;

        first_time := clock_timestamp();

        with upserted as (
                insert into gold.dim_products(
                        product_id, name, category, price, updated_at_gold
                )
                select
                        product_id,
                        name,
                        category,
                        price,
                        current_timestamp
                from silver.products_daily
                on conflict (product_id) do update
                set
                        name            = excluded.name,
                        category        = excluded.category,
                        price           = excluded.price,
                        updated_at_gold = current_timestamp
                where (gold.dim_products.name, gold.dim_products.category, gold.dim_products.price)
                   is distinct from (excluded.name, excluded.category, excluded.price)
                returning 1
        )
        select count(*) into rows_upserted from upserted;

        load_time := clock_timestamp() - first_time;

        insert into operational_log.gold_load_log(ingestion_id, table_name, rows_upserted, executing_time)
        values (local_ingestion_id, 'dim_products', coalesce(rows_upserted, 0), load_time)
        on conflict (ingestion_id, table_name) do update
        set rows_upserted  = excluded.rows_upserted,
            executing_time = excluded.executing_time;

        raise notice 'Gold dim_products loaded. Upserted: %', rows_upserted;

end;
$$;

call gold.load_gold_dim_products();

==============================================================
--GOLD LOAD: DIM_PRODUCTS -- END--
==============================================================




==============================================================
--GOLD LOAD: FACT_SALES -- START--
==============================================================

/*
Grain: one row per order_item (order_id + product_id).
Drives from silver.order_items_daily joined with silver.orders_daily
to get customer_id, order_date, and status — all small unlogged tables.
order_status can change (Pending → Completed) → DO UPDATE on conflict.
LEFT JOIN to dims so fact rows are never silently dropped on missing dim member.
*/
create or replace procedure gold.load_gold_fact_sales()
language plpgsql as $$
declare
        local_ingestion_id  int;
        first_time          timestamp;
        load_time           interval;
        rows_upserted       int;
        v_bronze_count      int;
        v_silver_count      int;
begin

        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);

        select count(source_file_id) into v_bronze_count
        from operational_log.bronze_ingest_log
        where table_name = 'orders'
        and ingestion_for = 'bronze_daily'
        and ingestion_id = local_ingestion_id;

        select count(distinct source_file_id) into v_silver_count
        from silver.orders_daily;

        if v_silver_count < v_bronze_count then
            raise exception 'Gate failed [fact_sales]: orders — %/% source files reached silver_daily',
                v_silver_count, v_bronze_count;
        end if;

        select count(source_file_id) into v_bronze_count
        from operational_log.bronze_ingest_log
        where table_name = 'order_items'
        and ingestion_for = 'bronze_daily'
        and ingestion_id = local_ingestion_id;

        select count(distinct source_file_id) into v_silver_count
        from silver.order_items_daily;

        if v_silver_count < v_bronze_count then
            raise exception 'Gate failed [fact_sales]: order_items — %/% source files reached silver_daily',
                v_silver_count, v_bronze_count;
        end if;

        first_time := clock_timestamp();

        with upserted as (
                insert into gold.fact_sales(
                        order_id, product_id,order_date, customer_key, product_key, date_key,
                        quantity, unit_price, revenue, order_status, created_at_gold
                )
                select
                        oi.order_id,
                        oi.product_id,
                        o.order_date,
                        COALESCE(dc.customer_key, -1),
                        COALESCE(dp.product_key,  -1),
                        COALESCE(dd.date_key,     -1),
                        oi.quantity,
                        oi.unit_price,
                        oi.total            as revenue,
                        o.status            as order_status,
                        current_timestamp
                from silver.order_items_daily oi
                join  silver.orders_daily     o  on  o.order_id    = oi.order_id
                left join gold.dim_customers  dc on  dc.customer_id = o.customer_id
                left join gold.dim_products   dp on  dp.product_id  = oi.product_id
                left join gold.dim_date       dd on  dd.full_date   = o.order_date
                on conflict (order_id, product_id, order_date) do update
                set
                        order_status = excluded.order_status,
                        quantity     = excluded.quantity,
                        unit_price   = excluded.unit_price,
                        revenue      = excluded.revenue,
                        customer_key = excluded.customer_key,
                        product_key  = excluded.product_key,
                        date_key     = excluded.date_key
                where (gold.fact_sales.order_status, gold.fact_sales.quantity, gold.fact_sales.unit_price)
                   is distinct from (excluded.order_status, excluded.quantity, excluded.unit_price)
                returning 1
        )
        select count(*) into rows_upserted from upserted;

        load_time := clock_timestamp() - first_time;

        insert into operational_log.gold_load_log(ingestion_id, table_name, rows_upserted, executing_time)
        values (local_ingestion_id, 'fact_sales', coalesce(rows_upserted, 0), load_time)
        on conflict (ingestion_id, table_name) do update
        set rows_upserted  = excluded.rows_upserted,
            executing_time = excluded.executing_time;

        raise notice 'Gold fact_sales loaded. Upserted: %', rows_upserted;

end;
$$;

call gold.load_gold_fact_sales();

==============================================================
--GOLD LOAD: FACT_SALES -- END--
==============================================================




==============================================================
--GOLD LOAD: FACT_PAYMENTS -- START--
==============================================================

/*
Grain: one row per payment (payment_id).
Payments are immutable → DO NOTHING on conflict (never overwrite a settled payment).
payments_daily has order_date, so the join on orders_raw_p uses
order_id + order_date → runtime partition pruning on orders_raw_p.
This handles payments for orders from previous days (not in orders_daily).
Two date keys: order_date_key (order placed) and payment_date_key (money received).
*/
create or replace procedure gold.load_gold_fact_payments()
language plpgsql as $$
declare
        local_ingestion_id  int;
        first_time          timestamp;
        load_time           interval;
        rows_upserted       int;
        v_bronze_count      int;
        v_silver_count      int;
begin

        local_ingestion_id := (select ingestion_id from operational_log.ingestion_id);

        select count(source_file_id) into v_bronze_count
        from operational_log.bronze_ingest_log
        where table_name = 'payments'
        and ingestion_for = 'bronze_daily'
        and ingestion_id = local_ingestion_id;

        select count(distinct source_file_id) into v_silver_count
        from silver.payments_daily;

        if v_silver_count < v_bronze_count then
            raise exception 'Gate failed [fact_payments]: payments — %/% source files reached silver_daily',
                v_silver_count, v_bronze_count;
        end if;

        first_time := clock_timestamp();

        with upserted as (
                insert into gold.fact_payments(
                        payment_id, order_id, payment_date, customer_key,
                        order_date_key, payment_date_key, method, amount, created_at_gold
                )
                select
                        p.payment_id,
                        p.order_id,
                        p.payment_date,
                        COALESCE(dc.customer_key, -1),
                        COALESCE(od.date_key,     -1)   as order_date_key,
                        COALESCE(pd.date_key,     -1)   as payment_date_key,
                        p.method,
                        p.total         as amount,
                        current_timestamp
                from silver.payments_daily       p
                left join gold.dim_customers     dc on  dc.customer_id = p.customer_id
                left join gold.dim_date          od on  od.full_date   = p.order_date
                left join gold.dim_date          pd on  pd.full_date   = p.payment_date
                on conflict (payment_id,payment_date) do nothing
                returning 1
        )
        select count(*) into rows_upserted from upserted;

        load_time := clock_timestamp() - first_time;

        insert into operational_log.gold_load_log(ingestion_id, table_name, rows_upserted, executing_time)
        values (local_ingestion_id, 'fact_payments', coalesce(rows_upserted, 0), load_time)
        on conflict (ingestion_id, table_name) do update
        set rows_upserted  = excluded.rows_upserted,
            executing_time = excluded.executing_time;

        raise notice 'Gold fact_payments loaded. Upserted: %', rows_upserted;

end;
$$;

call gold.load_gold_fact_payments();

==============================================================
--GOLD LOAD: FACT_PAYMENTS -- END--
==============================================================




==============================================================
--GOLD MAIN COORDINATOR -- START--
==============================================================

/*
Calls all gold load procedures in dependency order:
  dims first (customers, products) → then facts (sales, payments need FK keys).
For parallel execution, use the Python loader (landing_to_gold.py) which
runs dims concurrently then facts concurrently using ThreadPoolExecutor.
*/
create or replace procedure gold.gold_main()
language plpgsql as $$
begin
        call gold.load_gold_dim_customers();
        call gold.load_gold_dim_products();
        call gold.load_gold_fact_sales();
        call gold.load_gold_fact_payments();
        raise notice 'Gold layer load complete.';
end;
$$;

==============================================================
--GOLD MAIN COORDINATOR -- END--
==============================================================
