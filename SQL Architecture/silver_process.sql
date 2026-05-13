-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@silver

=================
/*
Full cleaning + casting [CUSTOMERS] table
*/

create or replace procedure silver.silver_customer_validation()
language PLPGSQL
as $$

declare 
bronze_customers_row_count int;
silver_customrs_row_count int;
silver_customers_null_pk_count int;
silver_customers_null_count int;
silver_customers_duplicate_count int;
silver_customers_future_past_count int;
first_time timestamp := clock_timestamp();


BEGIN


        /*[customers_daily]:Bronze full row count: */
        select count(*) into bronze_customers_row_count from bronze.customers_raw_daily;



        /*([customers_daily] Duplicate row chk */
        select count(*) into silver_customers_duplicate_count from(
        select customer_id,name,signup_date,count(*) as cnt from silver.customers_daily group by customer_id,name,signup_date having count(*)>1
        ) as a;

       
        delete from silver.customers_daily where ctid in( select ctid from(
        select ctid, row_number()over(partition by customer_id,name,signup_date order by created_at_bronze desc) as cnt from silver.customers_daily
        ) as a where  cnt>1);
        

        
        /*[customers_daily]:Checking for PK null*/
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'customers','missing_pk',row_to_json(d)::JSONB
        from silver.customers_daily d where nullif(customer_id,'') is null;

        select count(*) into silver_customers_null_pk_count from silver.customers_daily where nullif(customer_id,'') is null;

        delete from silver.customers_daily where nullif(customer_id,'') is null;



        /*[CUSTOMERS]:Deleting null and filling/qurantine:*/
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'customers','missing_required_fields',row_to_json(d)::JSONB
        from silver.customers_daily d where nullif(name,'') is null or nullif(signup_date::text,'') is null;

        select count(*) into silver_customers_null_count from silver.customers_daily where nullif(name,'') is null or nullif(signup_date::text,'') is null;

        delete from silver.customers_daily where nullif(name,'') is null or nullif(signup_date::text,'') is null;


        /*[customers_daily]:Future/way past Date */
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'customers','future_or_past_date',row_to_json(d)::JSONB
        from silver.customers_daily d where signup_date > now() or signup_date < '2015-01-01';

        select count(*) into silver_customers_future_past_count from silver.customers_daily where signup_date > now()+interval '1 day' or signup_date < '2015-01-01';

        delete from silver.customers_daily where signup_date > now() or signup_date < '2015-01-01';


        /*[customers_daily]:Count succeed rows */
        select count(*) into silver_customrs_row_count from silver.customers_daily;


        /*display*/
        raise notice '[customers]bronze row count: %',bronze_customers_row_count;
        raise notice '[customers]silver row count: %',silver_customrs_row_count;
        raise notice '[customers]null pk count: %',silver_customers_null_pk_count;
        raise notice '[customers]other null count: %',silver_customers_null_count;
        raise notice '[customers]duplicate count: %',silver_customers_duplicate_count;
        raise notice '[customers]future/past count: %',silver_customers_future_past_count;


        /*Inserting to log table: */
        insert into operational_log.customers_log(ingestion_id,table_name,bronze_row_count,silver_row_count,null_pk_count,other_null_count,duplicate_count,future_past_count,quarantine_count,executing_time)
        values((select ingestion_id from operational_log.ingestion_id),
        'customers',
        bronze_customers_row_count,
        silver_customrs_row_count,
        silver_customers_null_pk_count,
        silver_customers_null_count,
        silver_customers_duplicate_count,
        silver_customers_future_past_count,
        silver_customers_null_pk_count+silver_customers_null_count+silver_customers_duplicate_count+silver_customers_future_past_count,
        null
        );

        RAISE NOTICE 'Full validation for [customers] completed in %', clock_timestamp() - first_time;


/*
[CUSTOMERS] ends
*/
end;
$$;

=================

/*
Full cleaning + casting [ORDERS_ITEMS] table
*/

create or replace procedure silver.silver_order_items_validation()
language PLPGSQL
as $$
DECLARE
bronze_order_items_row_count int;
silver_order_items_row_count int;
silver_order_items_null_pk_count int;
silver_order_items_null_count int;
silver_order_items_duplicate_count int;
first_time timestamp := clock_timestamp();

BEGIN

        /*[ORDERS_ITEMS]:Bronze full row count: */
        select count(*) into bronze_order_items_row_count from bronze.order_items_raw_daily;


        /*([ORDERS_ITEMS] Duplicate row chk */
        select count(*) into silver_order_items_duplicate_count from(
        select order_id,product_id,quantity,unit_price,total,count(*) as cnt from silver.order_items_daily group by order_id,product_id,quantity,unit_price,total having count(*)>1
        ) as a;


        delete from silver.order_items_daily where ctid in( select ctid from(
        select ctid, row_number()over(partition by order_id,product_id,quantity,unit_price,total order by created_at_bronze desc) as cnt from silver.order_items_daily
        ) as a where  cnt>1);


        /*[ORDERS_ITEMS]:Checking for PK null*/
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'order_items','missing_pk',row_to_json(d)::JSONB
        from silver.order_items_daily d where (nullif(order_id,'') is null or nullif(product_id,'') is null); 

        select count(*) into silver_order_items_null_pk_count from silver.order_items_daily where (nullif(order_id,'') is null or nullif(product_id,'') is null);

        delete from silver.order_items_daily where (nullif(order_id,'') is null or nullif(product_id,'') is null);


        /*[ORDERS_ITEMS]:Deleting null/negative values and filling/qurantine:*/
        /*Picking unit price from products:*/
        update silver.order_items_daily b
        set unit_price =  a.price::numeric(10,2) from bronze.products_raw
        a where a.product_id = b.product_id and
        (nullif(b.unit_price::text,'') is null or b.unit_price<=0);


        /*Updating total:*/
        update silver.order_items_daily
        set total = unit_price * quantity
        where (nullif(total::text,'') is null or total<=0) and  quantity>0  and unit_price >0;


        /*Pushing to quarantine: where total is null*/
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'order_items','missing_required_fields',row_to_json(d)::JSONB
        from silver.order_items_daily d where  nullif(total::text,'') is null or  nullif(quantity::text,'') is null ;

        select count(*) into silver_order_items_null_count from silver.order_items_daily where  nullif(total::text,'') is null or  nullif(quantity::text,'') is null ;


        /*deleting where total is null*/
        delete from silver.order_items_daily where  nullif(total::text,'') is null;


        /*Checking negative values:*/
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'order_items','negative_total',row_to_json(d)::JSONB
        from silver.order_items_daily d where total < 0 or quantity < 0;

        delete from silver.order_items_daily where total < 0 or quantity < 0;


        /*[ORDERS_ITEMS]:Count succeed rows */
        select count(*) into silver_order_items_row_count from silver.order_items_daily;


        /*display*/
        raise notice '[order_items]bronze row count: %',bronze_order_items_row_count;
        raise notice '[order_items]silver row count: %',silver_order_items_row_count;
        raise notice '[order_items]null pk count: %',silver_order_items_null_pk_count;
        raise notice '[order_items]other null count: %',silver_order_items_null_count;
        raise notice '[order_items]duplicate count: %',silver_order_items_duplicate_count;


        /*Inserting to log table: */
        insert into operational_log.order_items_log(
        ingestion_id,table_name,bronze_row_count,silver_row_count,null_pk_count,other_null_count,duplicate_count,quarantine_count,executing_time)
        values((select ingestion_id from operational_log.ingestion_id),
        'order_items',
        bronze_order_items_row_count,
        silver_order_items_row_count,
        silver_order_items_null_pk_count,
        silver_order_items_null_count,
        silver_order_items_duplicate_count,
        silver_order_items_null_pk_count+silver_order_items_null_count+silver_order_items_duplicate_count,
        null  
        );

        RAISE NOTICE 'Full validation for [order_items] completed in %', clock_timestamp() - first_time;

END;
$$;

/*
[ORDERS_ITEMS] ends
*/

=================

/*
Full cleaning + casting [ORDERS] table
*/
create or replace procedure silver.silver_orders_validation()
language PLPGSQL
as $$   
DECLARE
bronze_orders_row_count int;
silver_orders_row_count int;
silver_orders_null_pk_count int;
silver_orders_null_count int;
silver_orders_duplicate_count int;
silver_orders_future_past_count int;
first_time timestamp:=clock_timestamp();

BEGIN
        /*[ORDERS]:Bronze full row count: */
        select count(*) into bronze_orders_row_count from bronze.orders_raw_daily;


        /*([ORDERS] Duplicate row chk */
        select count(*) into silver_orders_duplicate_count from(
        select order_id,customer_id,order_date,status,count(*) as cnt from silver.orders_daily group by order_id,customer_id,order_date,status having count(*)>1
        ) as a;


        delete from silver.orders_daily where ctid in( select ctid from(
        select ctid, row_number()over(partition by order_id,customer_id,order_date,status order by created_at_bronze desc) as cnt from silver.orders_daily
        ) as a where  cnt>1);


        /*[ORDERS]:Checking for PK null*/
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'orders','missing_pk',row_to_json(d)::JSONB
        from silver.orders_daily d where nullif(order_id,'') is null or nullif(customer_id,'') is null ;

        select count(*) into silver_orders_null_pk_count from silver.orders_daily where nullif(order_id,'') is null or nullif(customer_id,'') is null;

        delete from silver.orders_daily where nullif(order_id,'') is null or nullif(customer_id,'') is null ;


        /*[ORDERS]:Deleting null/negative values and filling/qurantine:*/
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'orders','missing_required_fields',row_to_json(d)::JSONB
        from silver.orders_daily d where nullif(order_date::text,'') is null or nullif(status,'') is null ;

        select count(*) into silver_orders_null_count from silver.orders_daily where nullif(order_date::text,'') is null or nullif(status,'') is null;

        delete from silver.orders_daily where nullif(order_date::text,'') is null or nullif(status,'') is null;


        /*[ORDERS]:Future or way past dates: */
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'orders','future_or_past_date',row_to_json(d)::JSONB
        from silver.orders_daily d where order_date > now()+interval '1 day' or order_date < '2015-01-01';

        select count(*) into silver_orders_future_past_count from silver.orders_daily where order_date > now()+interval '1 day' or order_date < '2015-01-01';

        delete from silver.orders_daily where order_date > now()+interval '1 day' or order_date < '2015-01-01';


        /*[ORDERS]:Count succeed rows */
        select count(*) into silver_orders_row_count from silver.orders_daily;


        /*display*/
        raise notice '[orders]bronze row count: %',bronze_orders_row_count;
        raise notice '[orders]silver row count: %',silver_orders_row_count;
        raise notice '[orders]null pk count: %',silver_orders_null_pk_count;
        raise notice '[orders]other null count: %',silver_orders_null_count;
        raise notice '[orders]duplicate count: %',silver_orders_duplicate_count;
        raise notice '[orders]future or past date count: %',silver_orders_future_past_count;


        /*Inserting to log table: */
        insert into operational_log.orders_log(
        ingestion_id,table_name,bronze_row_count,silver_row_count,null_pk_count,other_null_count,duplicate_count,future_past_count,quarantine_count,executing_time)
        values((select ingestion_id from operational_log.ingestion_id),
        'orders',
        bronze_orders_row_count,
        silver_orders_row_count,
        silver_orders_null_pk_count,
        silver_orders_null_count,
        silver_orders_duplicate_count,
        silver_orders_null_pk_count+silver_orders_null_count+silver_orders_duplicate_count,
        silver_orders_future_past_count,
        null
        );

        RAISE NOTICE 'Full validation for [orders] completed in %', clock_timestamp() - first_time;

END;
$$;

/*
[ORDERS] ends
*/

=================

/*
Full cleaning + casting [PAYMENTS] table
*/

create or replace procedure silver.silver_payments_validation()
language PLPGSQL
as $$   
DECLARE
bronze_payments_row_count int;
silver_payments_row_count int;
silver_payments_null_pk_count int;
silver_payments_null_count int;
silver_payments_duplicate_count int;
silver_payments_future_past_count int;
first_time timestamp:=clock_timestamp();


BEGIN

        /*[PAYMENTS]:Bronze full row count: */
        select count(*) into bronze_payments_row_count from bronze.payments_raw_daily;


        /*([PAYMENTS] Duplicate row chk */
        select count(*) into silver_payments_duplicate_count from(
        select payment_id,payment_date,method,order_id,order_date,total,count(*) as cnt from silver.payments_daily group by payment_id,payment_date,method,order_id,order_date,total having count(*)>1
        )as a;

        delete from silver.payments_daily where ctid in( select ctid from(
        select ctid, row_number()over(partition by payment_id,payment_date,method,order_id,order_date,total order by created_at_bronze desc) as cnt from silver.payments_daily
        ) as a where  cnt>1);


        /*[PAYMENTS]:Checking for PK null*/
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'payments','missing_pk',row_to_json(d)::JSONB
        from silver.payments_daily d where nullif(payment_id,'') is null or nullif(order_id,'') is null ;

        select count(*) into silver_payments_null_pk_count from silver.payments_daily where nullif(payment_id,'') is null or nullif(order_id,'') is null;

        delete from silver.payments_daily where nullif(payment_id,'') is null or nullif(order_id,'') is null ;  


        /*[ORDERS]:Deleting null/negative values and filling/qurantine:*/
        /*Checking for total/order_date in orders_raw if missing : */

        update silver.payments_daily
        set total = (select sum(a.total) from silver.order_items a where a.order_id = payments_daily.order_id)
        where nullif(total::text,'') is null;

        update silver.payments_daily
        set order_date=(select a.order_date from silver.orders a where a.order_id=payments_daily.order_id)
        where nullif(order_date::text,'') is null;

        select count(*) into silver_payments_null_count from silver.payments_daily where nullif(order_date::text,'') is null or nullif(total::text,'') is null  or nullif(payment_date::text,'') is null;

        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'payments','missing_required_fields',row_to_json(d)::JSONB
        from silver.payments_daily d where nullif(order_date::text,'') is null or nullif(total::text,'') is null  or nullif(payment_date::text,'') is null;

        delete from silver.payments_daily where nullif(total::text,'') is null or  nullif(order_date::text,'') is null or nullif(payment_date::text,'') is null;


        /*Checking negative values in total column: */
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'payments','negative_total',row_to_json(d)::JSONB
        from silver.payments_daily d where total < 0;

        delete from silver.payments_daily where total < 0;


        /*[PAYMENTS]:Future or way past dates: */
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'payments','future_or_past_date',row_to_json(d)::JSONB
        from silver.payments_daily d where payment_date > now()+interval '1 day' or payment_date < '2015-01-01'
        or order_date > now()+interval '1 day' or order_date < '2015-01-01';

        select count(*) into silver_payments_future_past_count from silver.payments_daily where payment_date > now()+interval '1 day' or payment_date < '2015-01-01'
        or order_date > now()+interval '1 day' or order_date < '2015-01-01';

        delete from silver.payments_daily where payment_date > now()+interval '1 day' or payment_date < '2015-01-01'
        or order_date > now()+interval '1 day' or order_date < '2015-01-01';


        /*[PAYMENTS]:Count succeed rows */
        select count(*) into silver_payments_row_count from silver.payments_daily;

        /*Displaying counts: */
        raise notice '[payments]bronze row count: %',bronze_payments_row_count;
        raise notice '[payments]silver row count: %',silver_payments_row_count;
        raise notice '[payments]null pk count: %',silver_payments_null_pk_count;
        raise notice '[payments]other null count: %',silver_payments_null_count;
        raise notice '[payments]duplicate count: %',silver_payments_duplicate_count;
        raise notice '[payments]future or past date count: %',silver_payments_future_past_count;


        /*Inserting to log table: */
        insert into operational_log.payments_log(
        ingestion_id,table_name,bronze_row_count,silver_row_count,null_pk_count,other_null_count,duplicate_count,future_past_count,quarantine_count,executing_time)
        values(
            (select ingestion_id from operational_log.ingestion_id),
            'payments',
            bronze_payments_row_count,
            silver_payments_row_count,
            silver_payments_null_pk_count,
            silver_payments_null_count,
            silver_payments_duplicate_count,
            silver_payments_future_past_count,
            silver_payments_null_pk_count + silver_payments_null_count + silver_payments_duplicate_count + silver_payments_future_past_count,
            null
            
        );
    
        RAISE NOTICE 'Full validation for [payments] completed in %', clock_timestamp() - first_time;

end;
$$;

/*
[ORDERS] ends
*/

=================================

/*
Full cleaning + casting [PRODUCTS] table
*/

create or replace procedure silver.silver_products_validation()
language PLPGSQL
as $$
DECLARE
bronze_products_row_count int;
silver_products_row_count int;
silver_products_null_pk_count int;
silver_products_null_count int;
silver_products_duplicate_count int;
first_time timestamp:= clock_timestamp();

BEGIN

        /*[PRODUCTS]:bronze row count  */
        select count(*) into bronze_products_row_count from bronze.products_raw_daily;
        /*
        Deleting duplicate:
        */
        select count(*) into silver_products_duplicate_count from(
            select product_id,name,category,price,count(*) from silver.products_daily group by product_id,name,category,price having count(*)>1) as a;

        delete from silver.products_daily where ctid in( select ctid from(
        select ctid, row_number()over(partition by product_id,name,category,price order by created_at_bronze desc) as cnt from silver.products_daily
        ) as a where  cnt>1);


        /*
        Checking for PK null:
        */
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'products','missing_pk',row_to_json(d)::JSONB
        from silver.products_daily d where nullif(product_id,'') is null;

        select count(*) into silver_products_null_pk_count from silver.products_daily where nullif(product_id,'') is null;

        delete from silver.products_daily where nullif(product_id,'') is null;


        /*
        Checking for missing/null fields:
        */
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'products','missing_required_fields',row_to_json(d)::JSONB
        from silver.products_daily d where nullif(name,'') is null or nullif(category,'') is null or nullif(price::text,'') is null;

        select count(*) into silver_products_null_count from silver.products_daily where nullif(name,'') is null or nullif(category,'') is null or nullif(price::text,'') is null;

        delete from silver.products_daily where nullif(name,'') is null or nullif(category,'') is null or nullif(price::text,'') is null;

        /*
        Checking negative values:
        */
        insert into operational_log.quarantine(table_name, reject_reason, raw_row)
        select 'products','negative_values',row_to_json(d)::JSONB
        from silver.products_daily d where price < 0;

        delete from silver.products_daily where price < 0;

        /*
        Count succeed rows:
        */
        select count(*) into silver_products_row_count from silver.products_daily;


        /*Displaying counts: */
        raise notice '[products]bronze row count: %',bronze_products_row_count;
        raise notice '[products]silver row count: %',silver_products_row_count;
        raise notice '[products]null pk count: %',silver_products_null_pk_count;
        raise notice '[products]other null count: %',silver_products_null_count;
        raise notice '[products]duplicate count: %',silver_products_duplicate_count;


        /*Inserting to log table: */

        insert into operational_log.products_log(
        ingestion_id,table_name,bronze_row_count,silver_row_count,null_pk_count,other_null_count,duplicate_count,quarantine_count,executing_time)
        values(
            (select ingestion_id from operational_log.ingestion_id),
            'products',
            bronze_products_row_count,
            silver_products_row_count,
            silver_products_null_pk_count,
            silver_products_null_count,
            silver_products_duplicate_count,
            silver_products_null_pk_count + silver_products_null_count + silver_products_duplicate_count,
            null
            
        );
       
        RAISE NOTICE 'Full validation for [products] completed in %', clock_timestamp() - first_time;

end;
$$;

/*
[PRODUCTS] ends
*/

================================
================================ OPTIMIZATION
================================

/* OPTIMIZED PART */

/*Customer optimized validation. */
create or replace procedure silver.customer_validation_optimized()
language PLPGSQL
as $$
DECLARE
bronze_customers_row_count int;
silver_customers_row_count int;
silver_customers_null_pk_count int;
silver_customers_null_count int;
silver_customers_duplicate_count int;
silver_customers_future_past_count int;
first_time timestamp:= clock_timestamp();
BEGIN

            /*Customers*/
            /*Quarantine(PK+required_fields+future_past date)+Count(PK+required_fields+future_past date)*/
            with deleted as(
            delete from silver.customers_daily
            where nullif(customer_id,'') is null or nullif(name,'') is null or nullif(signup_date::text,'') is null OR
            signup_date>now()+interval '1 day' or signup_date<'2015-01-01'
            returning *,
            case
            when nullif(customer_id,'') is null then 'missing_pk'
            when nullif(name,'') is null then 'missing_required_fields'
            when nullif(signup_date::text,'') is null then 'missing_required_fields'
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
            select customer_id,row_number() over(partition by customer_id,name,signup_date order by created_at_bronze) as rnk from bronze.customers_raw_daily
            ) as b where rnk>1;



            /*Counting the number of rows*/
            select count(*) into bronze_customers_row_count from bronze.customers_raw_daily;
            select count(*) into silver_customers_row_count from silver.customers_daily;

            raise notice '[customers]null pk count: %',silver_customers_null_pk_count;
            raise notice '[customers]other null count: %',silver_customers_null_count;
            raise notice '[customers]future or past date count: %',silver_customers_future_past_count;
            raise notice '[customers]duplicate count: %',silver_customers_duplicate_count;
            raise notice '[customers]bronze row count: %',bronze_customers_row_count;
            raise notice '[customers]silver row count: %',silver_customers_row_count;

            insert into operational_log.customers_log(
            ingestion_id,table_name,bronze_row_count,silver_row_count,null_pk_count,other_null_count,duplicate_count,future_past_count,quarantine_count,executing_time)
            values(
            (select ingestion_id from operational_log.ingestion_id),
            'customers',
            bronze_customers_row_count,
            silver_customers_row_count,
            silver_customers_null_pk_count,
            silver_customers_null_count,
            silver_customers_future_past_count,
            silver_customers_duplicate_count,
            silver_customers_null_pk_count + silver_customers_null_count + silver_customers_future_past_count,
            null
            );
            end;
            $$;






/*Payments optimized validation. */
create or replace procedure silver.payments_validation_optimized()
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
            when payment_date>now()+interval '1 day' or payment_date<'2015-01-01' or order_date>now()+interval '1 day' or order_date<'2015-01-01' or payment_date>order_date then 'future_or_past_date'
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




            /*duplicate count */
            select count(*) into silver_payments_duplicate_count from (
            select payment_id,
            row_number() over(partition by payment_id,payment_date,method,order_id,order_date,total order by created_at_bronze desc) as rn
            from bronze.payments_raw_daily
            ) ranked
            where rn>1;
            


            /*row count*/
            select count(*) into bronze_payments_row_count from bronze.payments_raw_daily;
            select count(*) into silver_payments_row_count from silver.payments_daily;


            raise notice '[payments]bronze row count: %',bronze_payments_row_count;
            raise notice '[payments]silver row count: %',silver_payments_row_count;
            raise notice '[payments]null pk count: %',silver_payments_null_pk_count;
            raise notice '[payments]null count: %',silver_payments_null_count;
            raise notice '[payments]duplicate count: %',silver_payments_duplicate_count;
            raise notice '[payments]future or past date count: %',silver_payments_future_past_count;
            raise notice '[payments]negative count: %',silver_payments_negative_count;
            raise notice '[payments]executing time: %',clock_timestamp()-first_time;

            /*insert into operational_log*/
            insert into operational_log.payments_log(
            ingestion_id,table_name,bronze_row_count,silver_row_count,null_pk_count,other_null_count,duplicate_count,future_past_count,quarantine_count,negative_count,executing_time,log_created_at
            )values(
            (select ingestion_id from operational_log.ingestion_id),
            'payments',
            bronze_payments_row_count,
            silver_payments_row_count,
            silver_payments_null_pk_count,
            silver_payments_null_count,
            silver_payments_duplicate_count,
            silver_payments_future_past_count,  
            silver_payments_null_pk_count + silver_payments_null_count + silver_payments_future_past_count+silver_payments_negative_count,
            silver_payments_negative_count,
            clock_timestamp() - first_time,
            null
            );

            RAISE NOTICE 'Full validation for [payments] completed in %', clock_timestamp() - first_time;


end;
$$;


/*Order_items optimized validation. */
create or replace procedure silver.order_items_validation_optimized()
language PLPGSQL
as $$
begin

            /*Qurantine */
            /*null pk count + null count + duplicate count + future or past date count*/
            with deleted as(
            delete from silver.order_items_daily
            where nullif(order_id,'') is null or nullif(product_id,'') is null or nullif(quantity::text,'') is null or quantity<0 or nullif(unit_price::text,'') is null or unit_price<0 or nullif(total::text,'') is null or total<0
            returning *,
            case
            when nullif(order_id,'') is null or nullif(product_id,'') is null  then 'missing_pk'
            when nullif(unit_price::text,'') is null  or nullif(total::text,'') is null or nullif(quantity::text,'') is null then 'missing_required_fields'
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
            count(*) filter (where reject_reason ='negative_quantity')
            from inserted;


            /*




end;
$$;

