-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@silver





=================
/*
Full cleaning + casting [CUSTOMERS] table
*/

create or replace procedure silver.silver_customer_validation()
language PLPGSQL
as $$

declare 
silver_customrs_row_count int;
silver_customers_null_pk_count int;
silver_customers_null_count int;
silver_customers_duplicate_count int;


BEGIN

        /*([customers_daily] Duplicate row chk */
        delete from silver.customers_daily where ctid in( select ctid from(
        select ctid, row_number()over(partition by customer_id,name,signup_date order by created_at_bronze desc) as cnt from bronze.customers_raw_daily
        ) as a where  cnt>1);
        
        select count(*) into silver_customers_duplicate_count from(
        select customer_id,name,signup_date,count(*) as cnt from silver.customers_daily group by customer_id,name,signup_date having count(*)>1
        ) as a;

        



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

        delete from silver.customers_daily where signup_date > now() or signup_date < '2015-01-01';

        /*[customers_daily]:Count succeed rows */
        select count(*) into silver_customrs_row_count from silver.customers_daily;

        /*display*/
        raise notice '[customers_daily]row count: %',silver_customrs_row_count;
        raise notice '[customers_daily]null pk count: %',silver_customers_null_pk_count;
        raise notice '[customers_daily]other null count: %',silver_customers_null_count;
        raise notice '[customers_daily]duplicate count: %',silver_customers_duplicate_count;


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
silver_order_items_row_count int;
silver_order_items_null_pk_count int;
silver_order_items_null_count int;
silver_customers_duplicate_count int;

BEGIN


        /*([ORDERS_ITEMS] Duplicate row chk */
        select count(*) into silver_customers_duplicate_count from(
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
        from silver.order_items_daily d where  nullif(total::text,'') is null or total<=0 or nullif(quantity::text,'') is null or quantity<=0;

        select count(*) into silver_order_items_null_count from silver.order_items_daily where  nullif(total::text,'') is null or total<=0 or nullif(quantity::text,'') is null or quantity<=0;

        /*deleting where total is null*/
        delete from silver.order_items_daily where  nullif(total::text,'') is null or total<=0;


        /*[ORDERS_ITEMS]:Count succeed rows */
        select count(*) into silver_order_items_row_count from silver.order_items_daily;

        /*display*/
        raise notice '[order_items]row count: %',silver_order_items_row_count;
        raise notice '[order_items]null pk count: %',silver_order_items_null_pk_count;
        raise notice '[order_items]other null count: %',silver_order_items_null_count;
        raise notice '[order_items]duplicate count: %',silver_customers_duplicate_count;

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
silver_orders_row_count int;
silver_orders_null_pk_count int;
silver_orders_null_count int;
silver_customers_duplicate_count int;

BEGIN

/*([ORDERS] Duplicate row chk */
select count(*) into silver_customers_duplicate_count from(
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
from silver.orders_daily d where order_date > now() or order_date < '2015-01-01';

delete from silver.orders_daily where order_date > now() or order_date < '2015-01-01';


/*[ORDERS]:Count succeed rows */
select count(*) into silver_orders_row_count from silver.orders_daily;

raise notice '[orders]row count: %',silver_orders_row_count;
raise notice '[orders]null pk count: %',silver_orders_null_pk_count;
raise notice '[orders]other null count: %',silver_orders_null_count;
raise notice '[orders]duplicate count: %',silver_customers_duplicate_count;

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
silver_payments_row_count int;
silver_payments_null_pk_count int;
silver_payments_null_count int;
silver_customers_duplicate_count int;

BEGIN

/*([PAYMENTS] Duplicate row chk */
select count(*) into silver_customers_duplicate_count from(
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

select count(*) into silver_payments_null_count from silver.payments_daily where nullif(order_date::text,'') is null or nullif(total::text,'') is null or total < 0 or nullif(payment_date::text,'') is null;

insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'payments','missing_required_fields',row_to_json(d)::JSONB
from silver.payments_daily d where nullif(order_date::text,'') is null or nullif(total::text,'') is null or total < 0 or nullif(payment_date::text,'') is null;

delete from silver.payments_daily where nullif(total::text,'') is null or total < 0 or nullif(order_date::text,'') is null or nullif(payment_date::text,'') is null;



/*[PAYMENTS]:Future or way past dates: */
insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'payments','future_or_past_date',row_to_json(d)::JSONB
from silver.payments_daily d where payment_date > now()+interval '1 day' or payment_date < '2015-01-01'
or order_date > now()+interval '1 day' or order_date < '2015-01-01';

delete from silver.payments_daily where payment_date > now()+interval '1 day' or payment_date < '2015-01-01'
or order_date > now()+interval '1 day' or order_date < '2015-01-01';


/*[PAYMENTS]:Count succeed rows */
select count(*) into silver_payments_row_count from silver.payments_daily;

raise notice '[payments]row count: %',silver_payments_row_count;
raise notice '[payments]null pk count: %',silver_payments_null_pk_count;
raise notice '[payments]other null count: %',silver_payments_null_count;
raise notice '[payments]duplicate count: %',silver_customers_duplicate_count;

end;
$$;

=================================
/*
Deleting any duplicate data came from bronze layer(Secondary safety net):
*/





#[orders_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)


#[payments_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)


#[products_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from bronze.products_raw_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by product_id,name,category,price order by created_at_bronze desc) as cnt from bronze.products_raw_daily
) as a where  cnt>1);

=================================

/*Qurantine and delete any null/missing PK data came from bronze layer:*/











insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'products','missing_pk',row_to_json(d)::JSONB
from silver.products_daily d where product_id is null or trim(product_id) = '';
delete from silver.products_daily where product_id is null or trim(product_id) = '';


==================================


