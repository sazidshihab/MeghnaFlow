-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@silver

=================================
/*
Deleting any duplicate data came from bronze layer(Secondary safety net):
*/

#[customers_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from silver.customers_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by customer_id,name,signup_date order by created_at_bronze desc) as cnt from silver.customers_daily
) as a where  cnt>1);

#[order_items_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from silver.order_items_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by order_id,product_id,quantity,unit_price,total order by created_at_bronze desc) as cnt from silver.order_items_daily
) as a where  cnt>1);

#[orders_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from silver.orders_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by order_id,customer_id,order_date,status order by created_at_bronze desc) as cnt from silver.orders_daily
) as a where  cnt>1);

#[payments_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from silver.payments_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by payment_id,payment_date,method,order_id,order_date,total order by created_at_bronze desc) as cnt from silver.payments_daily
) as a where  cnt>1);

#[products_daily] Deleting any duplicate data came from bronze layer(Though distinct on ingestion of data on silver layer cuts off the duplicates)
delete from silver.products_daily where ctid in( select ctid from(
select ctid, row_number()over(partition by product_id,name,category,price order by created_at_bronze desc) as cnt from silver.products_daily
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



/*Quarantine and delete any null/missing from required fields:*/





