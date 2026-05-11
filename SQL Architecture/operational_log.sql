-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@operational_log
create or replace procedure operational_log.quarantine_table_create()
LANGUAGE plpgsql

as $$
BEGIN
    drop table if exists operational_log.quarantine;
    create table operational_log.quarantine(
        id SERIAL  primary key,
        table_name varchar(255),
        reject_reason varchar(255),
        flagged_at timestamp default current_timestamp,
        raw_row JSONB
    );
END;
$$;

call operational_log.quarantine_table_create();



create or replace procedure operational_log.create_table_log()
LANGUAGE plpgsql

as $$
BEGIN

        drop table if exists operational_log.customers_log;
        create table operational_log.customers_log(
        ingestion_id int  primary key,
        table_name varchar(255),
        bronze_row_count int,
        silver_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        quarantine_count int,
        executing_time INTERVAL,
        log_created_at timestamp default current_timestamp
        ) ;

        drop table if exists operational_log.order_items_log;
        create table operational_log.order_items_log(
        ingestion_id int  primary key,
        table_name varchar(255),
        bronze_row_count int,
        silver_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        quarantine_count int,
        executing_time INTERVAL,
        log_created_at timestamp default current_timestamp
        ) ;

        drop table if exists operational_log.orders_log;
        create table operational_log.orders_log(
        ingestion_id int  primary key,
        table_name varchar(255),
        bronze_row_count int,
        silver_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        quarantine_count int,
        executing_time INTERVAL,
        log_created_at timestamp default current_timestamp
        ) ;

        drop table if exists operational_log.payments_log;
        create table operational_log.payments_log(
        ingestion_id int  primary key,
        table_name varchar(255),
        bronze_row_count int,
        silver_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        quarantine_count int,
        executing_time INTERVAL,
        log_created_at timestamp default current_timestamp
        ) ;

        drop table if exists operational_log.products_log;
        create table operational_log.products_log(
        ingestion_id int  primary key,
        table_name varchar(255),
        bronze_row_count int,
        silver_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        quarantine_count int,
        executing_time INTERVAL,
        log_created_at timestamp default current_timestamp
        ) ;

end;
$$;

call operational_log.create_table_log();


create or replace procedure operational_log.ingestion_id_create()
LANGUAGE plpgsql

as $$
BEGIN
    drop table if exists operational_log.ingestion_id;
    create table operational_log.ingestion_id(
        ingestion_id serial  primary key,
        created_at timestamp
    );
END;
$$;

call operational_log.ingestion_id_create();


select * from customers_log;


select raw_row->>'name',raw_row->>'customer_id',raw_row->>'signup_date'  from operational_log.quarantine;