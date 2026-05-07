-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@silver



/*
Full cleaning + casting Customers table
*/

#[customers_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from silver.customers_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by customer_id,name,signup_date order by created_at_bronze desc) as cnt from bronze.customers_raw_daily
) as a where  cnt>1);














=================================
/*
Deleting any duplicate data came from bronze layer(Secondary safety net):
*/



#[order_items_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from bronze.order_items_raw_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by order_id,product_id,quantity,unit_price,total order by created_at_bronze desc) as cnt from bronze.order_items_raw_daily
) as a where  cnt>1);

#[orders_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from bronze.orders_raw_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by order_id,customer_id,order_date,status order by created_at_bronze desc) as cnt from bronze.orders_raw_daily
) as a where  cnt>1);

#[payments_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from bronze.payments_raw_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by payment_id,payment_date,method,order_id,order_date,total order by created_at_bronze desc) as cnt from bronze.payments_raw_daily
) as a where  cnt>1);

#[products_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from bronze.products_raw_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by product_id,name,category,price order by created_at_bronze desc) as cnt from bronze.products_raw_daily
) as a where  cnt>1);

=================================

/*Qurantine and delete any null/missing PK data came from bronze layer:*/

insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'customers','missing_pk',row_to_json(d)::JSONB
from silver.customers_daily d where customer_id is null or trim(customer_id)='';
delete from silver.customers_daily where customer_id is null or trim(customer_id)='';


insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'order_items','missing_pk',row_to_json(d)::JSONB
from silver.order_items_daily d where (order_id is null or product_id is null) or (trim(order_id) = '' or trim(product_id) = ''); 
delete from silver.order_items_daily where (order_id is null or product_id is null) or (trim(order_id) = '' or trim(product_id) = '');


insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'orders','missing_pk',row_to_json(d)::JSONB
from silver.orders_daily d where order_id is null or customer_id is null or trim(order_id) = '' or trim(customer_id) = '';
delete from silver.orders_daily where order_id is null or customer_id is null or trim(order_id) = '' or trim(customer_id) = '';


insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'payments','missing_pk',row_to_json(d)::JSONB
from silver.payments_daily d where payment_id is null or order_id is null or trim(payment_id) = '' or trim(order_id) = '';
delete from silver.payments_daily where payment_id is null or order_id is null or trim(payment_id) = '' or trim(order_id) = '';


insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'products','missing_pk',row_to_json(d)::JSONB
from silver.products_daily d where product_id is null or trim(product_id) = '';
delete from silver.products_daily where product_id is null or trim(product_id) = '';


==================================
/*Quarantine and delete any null/missing from required fields:*/

#[CUSTOMERS]:
insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'customers','missing_required_fields',row_to_json(d)::JSONB
from silver.customers_daily d where name is null or signup_date is null or trim(name) = '';
delete from silver.customers_daily where name is null or signup_date is null or trim(name) = '';


#[ORDER_ITEMS]: Updating unit price if available from products + calculating total price if (unit_price * quantity) available, else pushing to quarantine and delete.
#Updating unit price:
update silver.order_items_daily
set unit_price = (select price from silver.products where product_id = silver.order_items_daily.product_id)
where unit_price is null;

#Updating total:
update silver.order_items_daily
set total = unit_price * quantity
where total is null and quantity is not null;

#Pushing to quarantine: where total is null
insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'order_items','missing_required_fields',row_to_json(d)::JSONB
from silver.order_items_daily d where  total is null ;

#deleting where total is null
delete from silver.order_items_daily where  total is null;

#[ORDERS]:
insert into operational_log.quarantine(table_name, reject_reason, raw_row)
select 'orders','missing_required_fields',row_to_json(d)::JSONB
from silver.orders_daily d where order_date is null or status is null ;
delete from silver.orders_daily where order_date is null or status is null;










select * from silver.customers_daily
where customer_id is null;


        create unlogged table silver.customers_daily(customer_id VARCHAR(255),name VARCHAR(255),signup_date date,created_at_bronze timestamp,created_at_silver timestamp default current_timestamp);

        insert into silver.customers_daily(customer_id,name,signup_date,created_at_bronze)
        select distinct on (customer_id)
        trim(customer_id)::varchar(255),
        trim(name)::varchar(255),
        case when nullif(trim(signup_date),'') ~ '^\d{4}-\d{2}-\d{2}$'
        then to_date(trim(signup_date),'YYYY-MM-DD') end,
        created_at_bronze
        from bronze.customers_raw_daily
        order by customer_id, created_at_bronze desc;

        raise notice 'Loaded completed in % min, Creating PK for [CUSTOMERS], both table(daily+main)', clock_timestamp()-first_time;
        first_time:=clock_timestamp();
        if not exists (select 1 from pg_constraint where conname='customer_id_pk')then
        alter table silver.customers
        add constraint customer_id_pk primary key (customer_id,valid_from);
        end if;

        if not exists(select 1 from pg_indexes  where indexname='partial_index_customer')then
        create unique index partial_index_customer on silver.customers(customer_id)
        where is_valid=true;
        end if;

        alter table silver.customers_daily
        add constraint customer_id_pk_daily primary key (customer_id);