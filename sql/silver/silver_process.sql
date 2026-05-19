-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@silver



================================
================================ OPTIMIZATION
================================

/* OPTIMIZED PART */

/*Customer optimized validation. */
create or replace procedure silver.customer_validation_optimized(insert_time interval)
language PLPGSQL
as $$
DECLARE
silver_customers_null_pk_count int;
silver_customers_null_count int;
silver_customers_duplicate_count int;
silver_customers_future_past_count int;
first_time timestamp:= clock_timestamp();
BEGIN

            /*Customers*/
            /*Quarantine(PK+required_fields+future_past date)+Count*/
            with deleted as(
            delete from silver.customers_daily
            where nullif(customer_id,'') is null or nullif(name,'') is null or nullif(signup_date::text,'') is null OR
            signup_date>now()+interval '1 day' or signup_date<'2015-01-01'
            returning *,
            case
            when nullif(customer_id,'') is null then 'missing_pk'
            when nullif(name,'') is null or nullif(signup_date::text,'') is null then 'missing_required_fields'
            when signup_date>now()+interval '1 day' or signup_date<'2015-01-01' then 'future_or_past_date'
            end as reject_reason
            ),inserted as(
            insert into operational_log.quarantine(ingestion_id,table_name, reject_reason, raw_row)
            select (select ingestion_id from operational_log.ingestion_id),
            'customers',
            reject_reason,
            row_to_json(d)::JSONB
            from deleted d
            returning reject_reason
            )select
            count(*) filter (where reject_reason ='missing_pk'),
            count(*) filter (where reject_reason ='missing_required_fields'),
            count(*) filter (where reject_reason ='future_or_past_date')
            into
            silver_customers_null_pk_count,
            silver_customers_null_count,
            silver_customers_future_past_count
            from inserted;

            /*duplicate count*/
            select count(*) into silver_customers_duplicate_count from (
            select customer_id,
            row_number() over(partition by customer_id,name,signup_date order by created_at_bronze desc) as rnk
            from bronze.customers_raw_daily
            ) as b where rnk>1;

            raise notice '[customers]null pk count: %',silver_customers_null_pk_count;
            raise notice '[customers]other null count: %',silver_customers_null_count;
            raise notice '[customers]future or past date count: %',silver_customers_future_past_count;
            raise notice '[customers]duplicate count: %',silver_customers_duplicate_count;

            insert into operational_log.customers_log
            (ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,silver_daily_insert_executing_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_daily_null_pk_count,silver_daily_required_null_count,silver_daily_duplicate_count,silver_daily_future_past_count,silver_daily_negative_count)
            select ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,insert_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_customers_null_pk_count,silver_customers_null_count,silver_customers_duplicate_count,silver_customers_future_past_count,null
            from operational_log.bronze_ingest_safetynet
            where table_name='customers' and ingestion_id=(select ingestion_id from operational_log.ingestion_id);

            RAISE NOTICE 'Full validation for [customers] completed in %', clock_timestamp() - first_time;

end;
$$;






/*Payments optimized validation. */
create or replace procedure silver.payments_validation_optimized(insert_time interval)
language PLPGSQL
as $$
DECLARE
bronze_payments_row_count int;
silver_payments_row_count int;
silver_payments_null_pk_count int;
silver_payments_null_count int;
silver_payments_duplicate_count int;
silver_payments_future_past_count int;
silver_payments_negative_count int;
first_time timestamp:= clock_timestamp();
BEGIN

            /*Payments*/
            /*Quarantine(PK+required_fields+future_past date)+Count(PK+required_fields+future_past date)*/
            with deleted as(
            delete from silver.payments_daily
            where nullif(payment_id,'') is null or nullif(order_id,'') is null or nullif(payment_date::text,'') is null or nullif(method,'') is null OR
            payment_date>now()+interval '1 day' or payment_date<'2015-01-01' or nullif(order_date::text,'') is null or order_date>now()+interval '1 day' or order_date<'2015-01-01' or nullif(total::text,'') is null or total<0
            returning *,
            case
            when nullif(payment_id,'') is null or nullif(order_id,'') is null then 'missing_pk'
            when nullif(payment_date::text,'') is null or nullif(method,'') is null or nullif(order_date::text,'') is null or nullif(total::text,'') is null then 'missing_required_fields'
            when payment_date>now()+interval '1 day' or payment_date<'2015-01-01' or order_date>now()+interval '1 day' or order_date<'2015-01-01' then 'future_or_past_date'
            when total<0 then 'negative_total'
            end as reject_reason
            ),
            inserted as(
            insert into operational_log.quarantine(ingestion_id,table_name, reject_reason, raw_row)
            select (select ingestion_id from operational_log.ingestion_id),
            'payments',
            reject_reason,
            row_to_json(d)::JSONB
            from deleted d
            returning reject_reason
            )select
            count(*) filter (where reject_reason ='missing_pk'), 
            count(*) filter (where reject_reason ='missing_required_fields'), 
            count(*) filter (where reject_reason ='future_or_past_date'),
            count(*) filter (where reject_reason ='negative_total')
            into
            silver_payments_null_pk_count,
            silver_payments_null_count,
            silver_payments_future_past_count,
            silver_payments_negative_count
            from inserted;

            raise notice '[payments]null pk count: %',silver_payments_null_pk_count;
            raise notice '[payments]other null count: %',silver_payments_null_count;
            raise notice '[payments]future or past date count: %',silver_payments_future_past_count;
            raise notice '[payments]negative count: %',silver_payments_negative_count;

         


            /*duplicate count */
            select count(*) into silver_payments_duplicate_count from (
            select payment_id,
            row_number() over(partition by payment_id,payment_date,method,order_id,order_date,total order by created_at_bronze desc) as rn
            from bronze.payments_raw_daily
            ) ranked
            where rn>1;
            


            /*insert into operational_log*/
            insert into operational_log.payments_log
            (ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,silver_daily_insert_executing_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_daily_null_pk_count,silver_daily_required_null_count,silver_daily_duplicate_count,silver_daily_future_past_count,silver_daily_negative_count)
            select ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,insert_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_payments_null_pk_count,silver_payments_null_count,silver_payments_duplicate_count,silver_payments_future_past_count,silver_payments_negative_count
            from operational_log.bronze_ingest_safetynet
            where table_name='payments' and ingestion_id=(select ingestion_id from operational_log.ingestion_id)
            ;

            RAISE NOTICE 'Full validation for [payments] completed in %', clock_timestamp() - first_time;


end;
$$;


/*Order_items optimized validation. */
create or replace procedure silver.order_items_validation_optimized(insert_time interval)
language PLPGSQL
as $$
declare
silver_order_items_duplicate_count int;
silver_order_items_null_pk_count int;
silver_order_items_null_count int;
silver_order_items_negative_count int;
first_time timestamp := clock_timestamp();
begin

            /*Quarantine */
            with deleted as(
            delete from silver.order_items_daily
            where nullif(order_id,'') is null or nullif(product_id,'') is null or nullif(quantity::text,'') is null or quantity<0 or nullif(unit_price::text,'') is null or unit_price<0 or nullif(total::text,'') is null or total<0
            returning *,
            case
            when nullif(order_id,'') is null or nullif(product_id,'') is null then 'missing_pk'
            when nullif(unit_price::text,'') is null or nullif(total::text,'') is null or nullif(quantity::text,'') is null then 'missing_required_fields'
            when unit_price<0 or total<0 or quantity<0 then 'negative_values'
            end as reject_reason
            ),
            inserted as(
            insert into operational_log.quarantine(ingestion_id,table_name, reject_reason, raw_row)
            select (select ingestion_id from operational_log.ingestion_id),
            'order_items',
            reject_reason,
            row_to_json(d)::JSONB
            from deleted d
            returning reject_reason
            )select
            count(*) filter (where reject_reason ='missing_pk'),
            count(*) filter (where reject_reason ='missing_required_fields'),
            count(*) filter (where reject_reason ='negative_values')
            INTO
            silver_order_items_null_pk_count,
            silver_order_items_null_count,
            silver_order_items_negative_count
            from inserted;

            /*Duplicate count */
            select count(*) into silver_order_items_duplicate_count from (
            select order_id,
            row_number() over(partition by order_id,product_id,quantity,unit_price,total order by created_at_bronze desc) as rn
            from bronze.order_items_raw_daily
            ) ranked
            where rn>1;

            raise notice '[order_items]null pk count: %',silver_order_items_null_pk_count;
            raise notice '[order_items]null count: %',silver_order_items_null_count;
            raise notice '[order_items]duplicate count: %',silver_order_items_duplicate_count;
            raise notice '[order_items]negative count: %',silver_order_items_negative_count;

            insert into operational_log.order_items_log
            (ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,silver_daily_insert_executing_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_daily_null_pk_count,silver_daily_required_null_count,silver_daily_duplicate_count,silver_daily_future_past_count,silver_daily_negative_count)
            select ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,insert_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_order_items_null_pk_count,silver_order_items_null_count,silver_order_items_duplicate_count,null,silver_order_items_negative_count
            from operational_log.bronze_ingest_safetynet
            where table_name='order_items' and ingestion_id=(select ingestion_id from operational_log.ingestion_id);

            RAISE NOTICE 'Full validation for [order_items] completed in %', clock_timestamp() - first_time;

end;
$$;




/*Orders optimized validation */
create or replace procedure silver.orders_validation_optimized(insert_time interval)
language PLPGSQL
as $$
declare
silver_orders_null_pk_count int;
silver_orders_null_count int;
silver_orders_future_or_past_date_count int;
silver_orders_duplicate_count int;
first_time timestamp := clock_timestamp();
begin

        /*Quarantine */
        with deleted as(
        delete from silver.orders_daily
        where nullif(order_id,'') is null or nullif(customer_id,'') is null or nullif(order_date::text,'') is null or nullif(status,'') is null OR
        order_date>now()+interval '1 day' or order_date<'2015-01-01'
        returning *,
        case
        when nullif(order_id,'') is null or nullif(customer_id,'') is null then 'missing_pk'
        when nullif(order_date::text,'') is null or nullif(status,'') is null then 'missing_required_fields'
        when order_date>now()+interval '1 day' or order_date<'2015-01-01' then 'future_or_past_date'
        end as reject_reason
        ),
        inserted as(
        insert into operational_log.quarantine(ingestion_id,table_name, reject_reason, raw_row)
        select (select ingestion_id from operational_log.ingestion_id),
        'orders',
        reject_reason,
        row_to_json(d)::JSONB
        from deleted d
        returning reject_reason
        )select
        count(*) filter (where reject_reason ='missing_pk'),
        count(*) filter (where reject_reason ='missing_required_fields'),
        count(*) filter (where reject_reason ='future_or_past_date')
        INTO
        silver_orders_null_pk_count,
        silver_orders_null_count,
        silver_orders_future_or_past_date_count
        from inserted;

        /*Duplicate count */
        select count(*) into silver_orders_duplicate_count from (
        select order_id,
        row_number() over(partition by order_id,customer_id,order_date,status order by created_at_bronze desc) as rn
        from bronze.orders_raw_daily
        ) ranked
        where rn>1;

        raise notice '[orders]null pk count: %',silver_orders_null_pk_count;
        raise notice '[orders]null count: %',silver_orders_null_count;
        raise notice '[orders]duplicate count: %',silver_orders_duplicate_count;
        raise notice '[orders]future or past date count: %',silver_orders_future_or_past_date_count;

        insert into operational_log.orders_log
        (ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,silver_daily_insert_executing_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_daily_null_pk_count,silver_daily_required_null_count,silver_daily_duplicate_count,silver_daily_future_past_count,silver_daily_negative_count)
        select ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,insert_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_orders_null_pk_count,silver_orders_null_count,silver_orders_duplicate_count,silver_orders_future_or_past_date_count,null
        from operational_log.bronze_ingest_safetynet
        where table_name='orders' and ingestion_id=(select ingestion_id from operational_log.ingestion_id);

        RAISE NOTICE 'Full validation for [orders] completed in %', clock_timestamp() - first_time;

end;
$$;



/*Products optimized validation. */
create or replace procedure silver.products_validation_optimized(insert_time interval)
language PLPGSQL
as $$
declare
silver_products_null_pk_count int;
silver_products_null_count int;
silver_products_duplicate_count int;
silver_products_negative_count int;
first_time timestamp := clock_timestamp();
begin

        /*Quarantine */
        with deleted as(
            delete from silver.products_daily
            where nullif(product_id,'') is null or nullif(name,'') is null or nullif(category,'') is NULL
            or nullif(price::text,'') is null or price<0
            returning *,
            CASE
               when nullif(product_id,'') is null then 'missing_pk'
               when nullif(name,'') is null or nullif(category,'') is NULL or nullif(price::text,'') is null then 'missing_required_fields'
               when price<0 then 'negative_price'
               end as reject_reason
        ),
        inserted as(
            insert into operational_log.quarantine(ingestion_id,table_name, reject_reason, raw_row)
            select (select ingestion_id from operational_log.ingestion_id),
            'products',
            reject_reason,
            row_to_json(d)::JSONB
            from deleted d
            returning reject_reason
        )select
        count(*) filter (where reject_reason ='missing_pk'),
        count(*) filter (where reject_reason ='missing_required_fields'),
        count(*) filter (where reject_reason ='negative_price')
        into
        silver_products_null_pk_count,
        silver_products_null_count,
        silver_products_negative_count
        from inserted;

        /*Duplicate count */
        select count(*) into silver_products_duplicate_count from (
        select product_id,
        row_number() over(partition by product_id,name,category,price order by created_at_bronze desc) as rn
        from bronze.products_raw_daily
        ) ranked
        where rn>1;

        raise notice '[products]null pk count: %',silver_products_null_pk_count;
        raise notice '[products]null count: %',silver_products_null_count;
        raise notice '[products]duplicate count: %',silver_products_duplicate_count;
        raise notice '[products]negative count: %',silver_products_negative_count;

        insert into operational_log.products_log
        (ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,silver_daily_insert_executing_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_daily_null_pk_count,silver_daily_required_null_count,silver_daily_duplicate_count,silver_daily_future_past_count,silver_daily_negative_count)
        select ingestion_id,table_name,bronze_daily_row_count,bronze_main_row_count,silver_daily_row_count,silver_main_row_count,insert_time,silver_daily_indexing_time,silver_main_insert_executing_time,silver_main_update_executing_time,total_silver_process_executing_time,bronze_daily_copy_executing_time,bronze_daily_indexing_time,bronze_main_copy_executing_time,total_bronze_process_executing_time,silver_products_null_pk_count,silver_products_null_count,silver_products_duplicate_count,null,silver_products_negative_count
        from operational_log.bronze_ingest_safetynet
        where table_name='products' and ingestion_id=(select ingestion_id from operational_log.ingestion_id);

        RAISE NOTICE 'Full validation for [products] completed in %', clock_timestamp() - first_time;

end;
$$;