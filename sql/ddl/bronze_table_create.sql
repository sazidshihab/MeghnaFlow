-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@bronze

===============================================
--BRONZE MAIN TABLE CREATION-- START
===============================================


create or replace procedure bronze.create_unlogged_bronze_main_tables()
language PLPGSQL
as $$

BEGIN
        drop table if exists bronze.customers_raw;
        create unlogged table bronze.customers_raw
                (
                    customer_id text,
                    name text,
                    signup_date text, 
                    created_at_bronze timestamp default current_timestamp,
                    source_file_id text
                );


        drop table if exists bronze.products_raw;
        create unlogged table bronze.products_raw(
                    product_id text ,
                    name text,
                    category text,
                    price text,
                    created_at_bronze timestamp default current_timestamp,
                    source_file_id text
                );


        drop table if exists bronze.orders_raw;
        create unlogged table bronze.orders_raw(
                order_id text,
                customer_id text,
                order_date text,
                status text,
                created_at_bronze timestamp default current_timestamp,
                source_file_id text
                );

        drop table if exists bronze.order_items_raw;
        create unlogged table bronze.order_items_raw(
                order_id text,
                product_id text,
                quantity text,
                unit_price text,
                total text,
                created_at_bronze timestamp default current_timestamp,
                source_file_id text
                );


        drop table if exists bronze.payments_raw;
        create unlogged table bronze.payments_raw(
                    payment_id text ,
                    method text,
                    order_id text,
                    order_date text,
                    total text,
                    payment_date text,
                    created_at_bronze timestamp default current_timestamp,
                    source_file_id text
                );

end;
$$;

call bronze.create_unlogged_bronze_main_tables();

================================
--BRONZE MAIN TABLE CREATION-- END
================================



----------------------------------------------------------------------------------------------------------



================================================
--BRONZE DAILY (UNLOGGED)TABLE CREATION-- START
================================================

/*Bronze unlogged tables for daily ingestion*/
Create or replace procedure bronze.create_unlogged_bronze_daily_tables()
language PLPGSQL
as $$
BEGIN

        drop table if exists bronze.customers_raw_daily;
        create UNLOGGED table bronze.customers_raw_daily(customer_id text, name text, signup_date text, created_at_bronze timestamp default current_timestamp, source_file_id text);
        
        drop table if exists  bronze.products_raw_daily;
        create UNLOGGED table bronze.products_raw_daily(product_id text, name text, category text, price text, created_at_bronze timestamp default current_timestamp, source_file_id text);
        
        drop table if exists bronze.orders_raw_daily;
        create UNLOGGED table bronze.orders_raw_daily(order_id text, customer_id text, order_date text, status text, created_at_bronze timestamp default current_timestamp, source_file_id text);
        
        drop table if exists bronze.order_items_raw_daily;
        create UNLOGGED table bronze.order_items_raw_daily(order_id text, product_id text, quantity text, unit_price text, total text, created_at_bronze timestamp default current_timestamp, source_file_id text);
        
        drop table if exists bronze.payments_raw_daily;
        create UNLOGGED table bronze.payments_raw_daily(payment_id text, method text, order_id text, order_date text, total text, payment_date text, created_at_bronze timestamp default current_timestamp, source_file_id text);
         
end;
$$;

call bronze.create_unlogged_bronze_daily_tables();


==============================================
--BRONZE DAILY (UNLOGGED)TABLE CREATION-- END
==============================================



----------------------------------------------------------------------------------------------------------



=======================================
--DROP ALL BRONZE DAILY TABLES-- START
=======================================
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


=======================================
--DROP ALL BRONZE DAILY TABLES-- END
=======================================


----------------------------------------------------------------------------------------------------------


=====================================
--DROP ALL BRONZE MAIN TABLES -- START
=====================================

CREATE OR REPLACE PROCEDURE bronze.drop_all_tables_bronze()
LANGUAGE plpgsql
AS $$

BEGIN

    drop table if exists bronze.customers_raw cascade;
    drop table if exists bronze.products_raw cascade;
    drop table if exists bronze.orders_raw cascade;
    drop table if exists bronze.order_items_raw cascade;
    drop table if exists bronze.payments_raw cascade;

end;
$$;


===============================================
--DROP ALL BRONZE MAIN TABLES -- END
===============================================


--------------------------------------------------------------------------------------------------------




===============================================
--TURN OFF AUTOVACUUM/ANALYZE OFF FOR BRONZE RAW TABLE -- START
=================================================


create or replace procedure bronze.bronze_raw_autovacuum_off()
language plpgsql
as $$
BEGIN
        ALTER TABLE bronze.customers_raw  SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
        ALTER TABLE bronze.orders_raw     SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
        ALTER TABLE bronze.order_items_raw SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
        ALTER TABLE bronze.products_raw   SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
        ALTER TABLE bronze.payments_raw   SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
end;
$$;      
call  bronze.bronze_raw_autovacuum_off(); 


===============================================
--TURN OFF AUTOVACUUM/ANALYZE OFF FOR BRONZE RAW TABLE -- END
===============================================



--------------------------------------------------------------------------------------------------------



================================================
--TURN OFF AUTOVACUUM/ANALYZE OFF FOR BRONZE DAILY TABLE -- START
=================================================

create or replace procedure bronze.bronze_daily_autovacuum_off()
language plpgsql
as $$
BEGIN
        ALTER TABLE bronze.customers_raw_daily  SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
        ALTER TABLE bronze.orders_raw_daily     SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
        ALTER TABLE bronze.order_items_raw_daily SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
        ALTER TABLE bronze.products_raw_daily   SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
        ALTER TABLE bronze.payments_raw_daily   SET (autovacuum_enabled = false, toast.autovacuum_enabled = false);
end;
$$;      
call  bronze.bronze_daily_autovacuum_off();