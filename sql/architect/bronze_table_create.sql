-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@bronze
create or replace procedure bronze.create_tables_bronze()
language PLPGSQL
as $$

BEGIN
        drop table if exists bronze.customers_raw;
        create table bronze.customers_raw
                (
                    customer_id text,
                    name text,
                    signup_date text, 
                    created_at_bronze timestamp default current_timestamp,
                    source_file_name text
                );


        drop table if exists bronze.products_raw;
        create table bronze.products_raw(
                    product_id text ,
                    name text,
                    category text,
                    price text,
                    created_at_bronze timestamp default current_timestamp,
                    source_file_name text
                );


        drop table if exists bronze.orders_raw;
        create table bronze.orders_raw(
                order_id text,
                customer_id text,
                order_date text,
                status text,
                created_at_bronze timestamp default current_timestamp,
                source_file_name text
                );

        drop table if exists bronze.order_items_raw;
        create table bronze.order_items_raw(
                order_id text,
                product_id text,
                quantity text,
                unit_price text,
                total text,
                created_at_bronze timestamp default current_timestamp,
                source_file_name text
                );


        drop table if exists bronze.payments_raw;
        create table bronze.payments_raw(
                    payment_id text ,
                    method text,
                    order_id text,
                    order_date text,
                    total text,
                    payment_date text,
                    created_at_bronze timestamp default current_timestamp,
                    source_file_name text
                );

end;
$$;


call bronze.create_tables_bronze();

/*
Table creation complete. Now we will ingest data into the bronze layer from CSV files.
*/



===============
===============

/*Bronze unlogged tables for daily ingestion*/
Create or replace procedure bronze.uncloged_bronze_tables()
language PLPGSQL
as $$
BEGIN

        create UNLOGGED table bronze.customers_raw_daily(customer_id text, name text, signup_date text, created_at_bronze timestamp default current_timestamp, source_file_name text);
        create UNLOGGED table bronze.products_raw_daily(product_id text, name text, category text, price text, created_at_bronze timestamp default current_timestamp, source_file_name text);
        create UNLOGGED table bronze.orders_raw_daily(order_id text, customer_id text, order_date text, status text, created_at_bronze timestamp default current_timestamp, source_file_name text);
        create UNLOGGED table bronze.order_items_raw_daily(order_id text, product_id text, quantity text, unit_price text, total text, created_at_bronze timestamp default current_timestamp, source_file_name text);
        create UNLOGGED table bronze.payments_raw_daily(payment_id text, method text, order_id text, order_date text, total text, payment_date text, created_at_bronze timestamp default current_timestamp, source_file_name text);
         
end;
$$;

call bronze.uncloged_bronze_tables();



====================
====================
/*
Procedure to drop all table :
*/

create or replace procedure bronze.drop_all_tables_bronze_daily()
language PLPGSQL
as $$

BEGIN

    drop table if exists bronze.customers_raw_daily cascade;
    drop table if exists bronze.products_raw_daily cascade;
    drop table if exists bronze.orders_raw_daily cascade;
    drop table if exists bronze.order_items_raw_daily cascade;
    drop table if exists bronze.payments_raw_daily cascade;

end;
$$;

call bronze.drop_all_tables_bronze_daily();
