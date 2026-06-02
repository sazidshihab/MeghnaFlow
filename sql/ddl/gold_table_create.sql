-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@gold


==============================================================
--GOLD DIMENSION + FACT TABLE CREATION -- START--
==============================================================

create or replace procedure gold.create_gold_tables()
language plpgsql
as $$
begin

        -- -------------------------------------------------------
        -- dim_date
        -- Pre-populated once (2015-2035). Never written by pipeline.
        -- date_key is YYYYMMDD integer — fast int joins from fact tables.
        -- -------------------------------------------------------
        drop table if exists gold.fact_sales;
        drop table if exists gold.fact_payments;
        drop table if exists gold.dim_customers;
        drop table if exists gold.dim_products;
        drop table if exists gold.dim_date;

        create table gold.dim_date (
                date_key        int             primary key,        -- 20240115
                full_date       date            unique not null,     -- 2024-01-15
                year            int             not null,            -- 2024
                quarter         int             not null,            -- 1
                month           int             not null,            -- 1
                month_name      varchar(15)     not null,            -- January
                week            int             not null,            -- 3  (ISO week)
                day_of_month    int             not null,            -- 15
                day_of_week     int             not null,            -- 1=Mon, 7=Sun
                day_name        varchar(15)     not null,            -- Monday
                is_weekend      boolean         not null             -- false
        );


        -- -------------------------------------------------------
        -- dim_customers
        -- SCD-1: always reflects the latest known state of a customer.
        -- customer_key  = surrogate int  → used in fact table joins (fast)
        -- customer_id   = natural key    → used for ON CONFLICT dedup
        -- first_seen_at = copied from created_at_bronze on first insert, never overwritten
        -- updated_at_gold = refreshed on every upsert
        -- -------------------------------------------------------
        create table gold.dim_customers (
                customer_key    serial          primary key,
                customer_id     bigint          unique not null,
                name            varchar(255),
                signup_date     date,
                first_seen_at   timestamp,
                updated_at_gold timestamp       default current_timestamp
        );


        -- -------------------------------------------------------
        -- dim_products
        -- SCD-1: price and category can change, always keep latest.
        -- product_key = surrogate int → used in fact joins
        -- product_id  = natural key  → used for ON CONFLICT dedup
        -- -------------------------------------------------------
        create table gold.dim_products (
                product_key     serial          primary key,
                product_id      bigint          unique not null,
                name            varchar(255),
                category        varchar(255),
                price           numeric(10,2),
                updated_at_gold timestamp       default current_timestamp
        );


        -- -------------------------------------------------------
        -- fact_sales
        -- Grain: one row per order_item (order_id + product_id).
        -- One order can have multiple items → multiple rows per order.
        -- order_id + product_id kept as natural keys for deduplication.
        -- customer_key + product_key are surrogate ints for fast analytical joins.
        -- date_key links to dim_date via order_date.
        -- order_status can change (Pending → Completed) → needs UPDATE logic.
        -- revenue = quantity * unit_price (= total from silver order_items_daily).
        -- BIGSERIAL because this table grows the fastest.
        -- -------------------------------------------------------
        create table gold.fact_sales (
                sale_key        bigserial       ,
                order_id        bigint          ,
                product_id      bigint          ,
                order_date      date            ,
                customer_key    int             not null    references gold.dim_customers(customer_key),
                product_key     int             not null    references gold.dim_products(product_key),
                date_key        int             not null    references gold.dim_date(date_key),
                quantity        numeric(10,2),
                unit_price      numeric(10,2),
                revenue         numeric(10,2),
                order_status    varchar(50),
                created_at_gold timestamp       default current_timestamp,
                unique (order_id, product_id,order_date),
                primary key (sale_key,order_date)
        )partition by range (order_date);


        -- -------------------------------------------------------
        -- fact_payments
        -- Grain: one row per payment.
        -- payment_id is natural key → UNIQUE for dedup + ON CONFLICT DO NOTHING.
        -- Two date keys: order_date_key (when order was placed)
        --                payment_date_key (when money was received).
        -- Both FK to same dim_date table, different columns.
        -- Payments are immutable → no UPDATE logic, only INSERT + DO NOTHING.
        -- amount = total from silver payments_daily.
        -- -------------------------------------------------------
        create table gold.fact_payments (
                payment_key         bigserial       ,
                payment_id          bigint          ,
                order_id            bigint          ,
                payment_date        date            ,
                customer_key        int             not null    references gold.dim_customers(customer_key),
                order_date_key      int             not null    references gold.dim_date(date_key),
                payment_date_key    int             not null    references gold.dim_date(date_key),
                method              varchar(50),
                amount              numeric(10,2),
                created_at_gold     timestamp       default current_timestamp,
                unique (payment_id, payment_date),
                primary key (payment_key, payment_date)
        ) partition by range (payment_date);

        -- Unknown members: catch-all for facts whose dim member was quarantined
        insert into gold.dim_customers (customer_key, customer_id, name, signup_date, first_seen_at)
        values (-1, -1, 'Unknown', null, current_timestamp)
        on conflict (customer_id) do nothing;

        insert into gold.dim_products (product_key, product_id, name, category, price)
        values (-1, -1, 'Unknown', 'Unknown', null)
        on conflict (product_id) do nothing;

end;
$$;

call gold.create_gold_tables();

==============================================================
--GOLD DIMENSION + FACT TABLE CREATION -- END--
==============================================================

==============================================================
--GOLD PARTITION PARTMAN PROCEDURES -- START--
==============================================================


create or replace procedure gold.gold_pgpartman()
language plpgsql as $$
begin


        delete from partman.part_config
        where parent_table in ('gold.fact_sales', 'gold.fact_payments');

       perform partman.create_parent(
         p_parent_table => 'gold.fact_sales',
         p_control => 'order_date',
         p_interval => '1 month',
         p_start_partition => '2019-04-01',
         p_premake => 1
       );

       perform partman.create_parent(
         p_parent_table => 'gold.fact_payments',
         p_control => 'payment_date',
         p_interval => '1 month',
         p_start_partition => '2019-04-01',
         p_premake => 1
       );

 end;
 $$;

 call   gold.gold_pgpartman();    


==============================================================
--DIM_DATE POPULATION (RUN ONCE, 2015-2035) -- START--
==============================================================

create or replace procedure gold.populate_dim_date()
language plpgsql
as $$
begin

        delete from gold.dim_date;

        insert into gold.dim_date (
                date_key,
                full_date,
                year,
                quarter,
                month,
                month_name,
                week,
                day_of_month,
                day_of_week,
                day_name,
                is_weekend
        )
        select
                to_char(d, 'YYYYMMDD')::int             as date_key,
                d                                        as full_date,
                extract(year    from d)::int             as year,
                extract(quarter from d)::int             as quarter,
                extract(month   from d)::int             as month,
                to_char(d, 'Month')                      as month_name,
                extract(week    from d)::int             as week,
                extract(day     from d)::int             as day_of_month,
                extract(isodow  from d)::int             as day_of_week,
                to_char(d, 'Day')                        as day_name,
                extract(isodow  from d) in (5,6)             as is_weekend
        from generate_series(
                '2015-01-01'::date,
                '2035-12-31'::date,
                '1 day'
        ) as d;

        -- Unknown member: used when order_date or payment_date could not be resolved
        insert into gold.dim_date (date_key, full_date, year, quarter, month, month_name, week, day_of_month, day_of_week, day_name, is_weekend)
        values (-1, '1900-01-01', 0, 0, 0, 'Unknown', 0, 0, 0, 'Unknown', false)
        on conflict (date_key) do nothing;

end;
$$;

call gold.populate_dim_date();

==============================================================
--DIM_DATE POPULATION (RUN ONCE, 2015-2035) -- END--
==============================================================