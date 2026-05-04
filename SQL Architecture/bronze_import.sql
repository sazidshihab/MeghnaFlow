-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@bronze

/*

Creating tables in bronze:

*/
create or replace procedure create_tables_bronze()
language PLPGSQL
as $$

BEGIN
        drop table if exists bronze.customers_raw;
        create table bronze.customers_raw
                (
                    customer_id varchar(255),
                    name varchar(255),
                    signup_date date, 
                    created_at_bronze timestamp default current_timestamp
                );


        drop table if exists bronze.products_raw;
        create table bronze.products_raw(
                    product_id varchar(100) ,
                    name VARCHAR(255),
                    category VARCHAR(255),
                    price numeric(10,2),
                    created_at_bronze timestamp default current_timestamp
                );


        drop table if exists bronze.orders_raw;
        create table bronze.orders_raw(
                order_id VARCHAR(255),
                customer_id VARCHAR(255),
                order_date date,
                status VARCHAR(50),
                created_at_bronze timestamp default current_timestamp
                );

        drop table if exists bronze.order_items_raw;
        create table bronze.order_items_raw(
                order_id VARCHAR(255) not null,
                product_id VARCHAR(255) not null,
                quantity numeric(10,2),
                unit_price numeric(10,2),
                total numeric(10,2),
                created_at_bronze timestamp default current_timestamp
                );


        drop table if exists bronze.payments_raw;
        create table bronze.payments_raw(
                    payment_id varchar(255) ,
                    method VARCHAR(50),
                    order_id VARCHAR(255),
                    order_date date,
                    total numeric(10,2),
                    payment_date date,
                    created_at_bronze timestamp default current_timestamp
                );

end;
$$;



call create_tables_bronze();


/*
Table creation complete. Now we will ingest data into the bronze layer from CSV files.
*/



=======================================================================================================================
/*
IMPORTING DATA INTO THE BRONZE LAYER (customers_raw)===================================================================
*/
=======================================================================================================================


create or replace procedure bronze_ingest()
LANGUAGE plpgsql

as $$

Begin
        RAISE NOTICE 'Step 1: Starting to ingest Customer data...';
       
        COPY bronze.customers_raw(customer_id, name, signup_date)
        FROM '/Users/sazid/Documents/SQL PDF/Warehouse Project/Demo_warehouse/Data/customers_raw1.csv'
        WITH (FORMAT csv, HEADER true);

        RAISE NOTICE 'Step 1: Customer data ingested successfully.';
        


        /*
        IMPORTING DATA INTO THE BRONZE LAYER (products_raw)
       */
        RAISE NOTICE 'Step 2: Starting to ingest Product data...';

        copy bronze.products_raw(product_id, name, category, price)
        from '/Users/sazid/Documents/SQL PDF/Warehouse Project/Demo_warehouse/Data/products_raw.csv'
        with (format csv, header true);

        RAISE NOTICE 'Step 2: Product data ingested successfully.';



       
          /*
        IMPORTING DATA INTO THE BRONZE LAYER (orders_raw)
        */
        RAISE NOTICE 'Step 3: Starting to ingest Order data...';
 
        copy bronze.orders_raw(order_id, customer_id, order_date, status)
        from '/Users/sazid/Documents/SQL PDF/Warehouse Project/Demo_warehouse/Data/orders_raw.csv'
        with(format csv, header true);

        RAISE NOTICE 'Step 3: Order data ingested successfully.';
 

        /*
        IMPORTING DATA INTO THE BRONZE LAYER (order_items_raw)
        */


        RAISE notice 'Step 4: Starting to ingest Order Item data...';
        
        copy bronze.order_items_raw(order_id, product_id, quantity, unit_price, total)
        from '/Users/sazid/Documents/SQL PDF/Warehouse Project/Demo_warehouse/Data/order_items_raw.csv'
        with(format csv, header true);

        RAISE NOTICE 'Step 4: Order Item data ingested successfully.';



        /*
        IMPORTING DATA INTO THE BRONZE LAYER (payments_raw)
        */
      
        RAISE NOTICE 'Step 5: Starting to ingest Payment data...';
       
        copy bronze.payments_raw(order_id,  total, payment_id,  order_date, payment_date, method)
        from '/Users/sazid/Documents/SQL PDF/Warehouse Project/Demo_warehouse/Data/payments_raw.csv'
        with(format csv, header true);

        RAISE NOTICE 'Step 5: Payment data ingested successfully.';

        
        raise notice 'All data ingested successfully into the Bronze layer!';



        
end;
$$;

call bronze_ingest();

/*
Data ingestion into the bronze layer is complete. We can now proceed to create tables in the silver layer and ingest data from bronze to silver.
*/



/*
Procedure to drop all table :
*/

create or replace procedure drop_all_tables_bronze()
language PLPGSQL
as $$

BEGIN

    drop table if exists bronze.customers_raw cascade;
    drop table if exists bronze.products_raw cascade;
    drop table if exists bronze.orders_raw cascade;
    drop table if exists bronze.order_items_raw cascade;
    drop table if exists bronze.payments_raw cascade;

end;
$$;

call drop_all_tables_bronze();








select datname,pid, (now()-query_start)::time as time_,query as "query/command", state as "state/bytes_total" , backend_type as "backend_type/tuples_processed" from pg_stat_activity where state='active' and query not like '%select datname%' 
union all
select datname, pid,null::time, command as "query/command",bytes_total::text as "state/bytes_total",tuples_processed::text as "backend_type/tuples_processed" from   pg_stat_progress_copy;


SELECT * FROM pg_stat_wal;


select * from pg_stat_activity where state='active';
select * from information_schema.tables where table_name like '%stat%';







CREATE INDEX idx_payments_optimization 
ON bronze.payments_raw (order_id, order_date DESC);

SET force_parallel_mode = off;
SET parallel_tuple_cost = 0.1; -- standard default
SET max_parallel_workers_per_gather = 2; 

select *, row_number()over(partition by order_id order by order_date desc) as rnk from bronze.payments_raw
order by order_id;

select count(*) from bronze.customers_raw;
SELECT count(*) FROM bronze.order_items_raw;
select  count(*) from bronze.orders_raw;
select count(*) from bronze.payments_raw;
select * from bronze.products_raw
where product_id='P0';






/*
copy silver.customers(customer_id,name,signup_date) TO '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/customers/customers.csv' WITH (FORMAT CSV, HEADER);
copy silver.order_items(order_id,product_id,quantity,unit_price,total) TO '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/order_items/order_items.csv' WITH (FORMAT CSV, HEADER);
copy silver.orders(order_id,customer_id,order_date,status) TO '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/orders/orders.csv' WITH (FORMAT CSV, HEADER);
copy silver.payments(payment_id,order_id,total,payment_date,method) TO '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/payments/payments.csv' WITH (FORMAT CSV, HEADER);
copy silver.products(product_id,name,category,price) TO '/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing/products/products.csv' WITH (FORMAT CSV, HEADER);
*/
