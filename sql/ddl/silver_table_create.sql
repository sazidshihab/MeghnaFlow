


/*
Creating 5 tables in the silver layer: 
*/

create or replace procedure silver.create_tables_silver()
language PLPGSQL
as $$
BEGIN

        drop table if exists silver.customers cascade;
        create table silver.customers (
            customer_id varchar(255),
            name varchar(255),
            signup_date date ,
            created_at_bronze timestamp ,
            created_at_silver timestamp default current_timestamp,
            valid_from TIMESTAMP default current_date,
            valid_to TIMESTAMP default '2050-01-01',
            is_valid boolean default true
        );

        drop table if exists silver.products cascade;
        create table silver.products (
            product_id varchar(100) ,
            name VARCHAR(255),
            category VARCHAR(255),
            price numeric(10,2),
            created_at_bronze timestamp ,
            created_at_silver timestamp default current_timestamp
        );

        drop table if exists silver.orders cascade;
        create table silver.orders (
            order_id VARCHAR(255) ,
                customer_id VARCHAR(255),
                order_date date,
                status VARCHAR(50),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp

        );


        drop table if exists silver.order_items cascade;
        create table silver.order_items (

                order_id VARCHAR(255) ,
                product_id VARCHAR(255) ,
                quantity numeric(10,2),
                unit_price numeric(10,2),
                total numeric(10,2),
                created_at_bronze timestamp ,
                created_at_silver timestamp default current_timestamp

        );

        drop table if exists silver.payments cascade;
        create table silver.payments(
            payment_id varchar(255) ,
                    payment_date date,
                    method VARCHAR(50),
                    order_id VARCHAR(255),
                    order_date date,
                    total numeric(10,2),
                    created_at_bronze timestamp ,
                    created_at_silver timestamp default current_timestamp

        );

end;
$$;

call silver.create_tables_silver();





=================
=================



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
