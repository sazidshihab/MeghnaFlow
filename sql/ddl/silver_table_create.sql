

==============================================================
--CREATING SILVER DAILY TABLES (UNLOGGED)-- START--
==============================================================
/*
Creating 5 tables in the silver layer: 
*/

create or replace procedure silver.create_silver_daily_tables()
language PLPGSQL
as $$
BEGIN

        drop table if exists silver.customers_daily;
        create unlogged table silver.customers_daily
        (
                customer_id varchar(255),
                name varchar(255),
                signup_date date ,
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text

        );


        drop table if exists silver.order_items_daily;
        create unlogged table silver.order_items_daily
        (
                order_id VARCHAR(255) ,
                product_id VARCHAR(255) ,
                quantity numeric(10,2),
                unit_price numeric(10,2),
                total numeric(10,2),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text
        );


        drop table if exists silver.orders_daily;
        create unlogged table silver.orders_daily
        (
                order_id VARCHAR(255) ,
                customer_id VARCHAR(255),
                order_date date,
                status VARCHAR(50),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text
        );


        drop table if exists silver.payments_daily;
        create unlogged table silver.payments_daily(
                payment_id varchar(255) ,
                payment_date date ,
                method VARCHAR(50),
                order_id VARCHAR(255),
                order_date date,
                total numeric(10,2),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text
        );


        drop table if exists silver.products_daily;
        create unlogged table silver.products_daily (
                product_id varchar(100) ,
                name VARCHAR(255),
                category VARCHAR(255),
                price numeric(10,2),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text
        );

end;
$$;

call silver.create_silver_daily_tables();


==============================================================
--CREATING SILVER TABLES (UNLOGGED)-- END--
==============================================================



------------------------------------------------------------------------------------------------------------




===============================================================
--CREATING SILVER RAW TABLES (UNLOGGED)-- START--
==============================================================



create or replace PROCEDURE silver.create_silver_main_tables()
language PLPGSQL
as $$
BEGIN   




        drop table if exists silver.customers_raw_p;
        create table silver.customers_raw_p (
                customer_id varchar(255),
                name varchar(255),
                signup_date date ,
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                valid_from TIMESTAMP default current_date not null,
                valid_to TIMESTAMP default '2050-01-01',
                is_valid boolean default true,
                source_file_id text,
                primary key(customer_id,valid_from)
                
        ) partition by range(valid_from);



        drop table if exists silver.products_raw;
        create table silver.products_raw (
                product_id varchar(100) ,
                name VARCHAR(255),
                category VARCHAR(255),
                price numeric(10,2),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text,
                primary key(product_id)
        );



        drop table if exists silver.orders_raw_p cascade;
        create table silver.orders_raw_p (
                order_id VARCHAR(255) ,
                customer_id VARCHAR(255),
                order_date date not null,
                status VARCHAR(50),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text,
                primary key(order_id,order_date)
        ) partition by range(order_date);


        drop table if exists silver.order_items_raw_p cascade;
        create table silver.order_items_raw_p (
                order_id VARCHAR(255) ,
                product_id VARCHAR(255) ,
                quantity numeric(10,2),
                unit_price numeric(10,2),
                total numeric(10,2),
                order_date date not null,
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text,
                primary key(order_id,product_id,order_date)
        ) partition by range(order_date);



        drop table if exists silver.payments_raw_p cascade;
        create table silver.payments_raw_p(
                payment_id varchar(255) ,
                payment_date date not null,
                method VARCHAR(50),
                order_id VARCHAR(255),
                order_date date,
                total numeric(10,2),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp,
                source_file_id text,
                primary key(payment_id,order_id,payment_date)
        ) partition by range(payment_date);


end;
$$;

call silver.create_silver_main_tables();



===============================================================
--CREATING SILVER RAW TABLES (UNLOGGED)-- END--
===============================================================



---------------------------------------------------------------------------------------------------------------




=================================
--DROPING SILVER DAILY TABLES-- START--
================================




create or replace procedure silver.silver_daily_table_drop()
language PLPGSQL
as $$
BEGIN
        drop table if exists silver.customers_daily;
        drop table if exists silver.products_daily;
        drop table if exists silver.orders_daily;
        drop table if exists silver.order_items_daily;
        drop table if exists silver.payments_daily;

END;
$$;

call silver.silver_daily_table_drop();


=================================
--DROPING SILVER DAILY TABLES-- END--
================================
