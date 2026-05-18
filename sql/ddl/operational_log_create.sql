-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse@operational_log

==================================================================================
--OPERATIONAL LOG TABLE CREATION((INSERT LOG FROM PYTHON FOR DAILY TABLE))-- START
==================================================================================


/*Creating bronze_raw_daily_ingest_log table*/
CREATE OR REPLACE PROCEDURE operational_log.bronze_raw_daily_ingest_log_table_create()
LANGUAGE plpgsql

as $$
BEGIN
drop table if exists operational_log.bronze_raw_daily_ingest_log;
create table operational_log.bronze_raw_daily_ingest_log(
    ingestion_id serial,
    source_file_id SERIAL primary key,
    source_file_name VARCHAR(255),
    table_name VARCHAR(255),
    row_count int,
    executing_time float8,
    created_at timestamp default current_date
);
end;
$$;


call operational_log.bronze_raw_daily_ingest_log_table_create();


===================================================================================
--OPERATIONAL LOG TABLE CREATION((INSERT LOG FROM PYTHON FOR DAILY TABLE))-- END
===================================================================================



----------------------------------------------------------------------------------------------------------




==================================================================================
--SAFETYNET TABLE CREATION -- START
==================================================================================

/*
Procedure to create bronze_ingest_safetynet table
*/

CREATE OR REPLACE PROCEDURE operational_log.bronze_ingest_safetynet()
LANGUAGE plpgsql
AS 
$$
BEGIN
drop table if exists operational_log.bronze_ingest_safetynet;
create table operational_log.bronze_ingest_safetynet(
    ingestion_id int,
    table_name VARCHAR(255),
    bronze_row_count int,
    silver_daily_row_count int,
    silver_main_row_count int,
    null_pk_count int,
    other_null_count int,
    duplicate_count int,
    future_past_count int,
    negative_count int,
    quarantine_count int,
    silver_daily_insert_executing_time INTERVAL,
    silver_main_insert_executing_time INTERVAL,
    silver_main_update_executing_time INTERVAL,
    total_silver_process_executing_time INTERVAL,
    bronze_daily_copy_executing_time INTERVAL,
    bronze_daily_indexing_time INTERVAL,
    bronze_main_insert_executing_time INTERVAL,
    total_bronze_process_executing_time INTERVAL,
    created_at timestamp default current_date,
    primary key(ingestion_id,table_name,bronze_row_count)
);
end;
$$;


call operational_log.bronze_ingest_safetynet();


==================================================================================
--SAFETYNET TABLE CREATION -- END
==================================================================================



-----------------------------------------------------------------------------------------------------------




=============================================================================
--INSERTING INGESTION_ID ON EACH INGESTION -- START--
=============================================================================

/*Only run below code inside python bronze daily ingest:
--
truncate table operational_log.ingestion_id;
insert into operational_log.ingestion_id(created_at) values(now())
returning ingestion_id;  --This will create a new ingestion ID when script runs to copy all data from dumping zone to bronze daily tables.--

*/
=============================================================================
--INSERTING INGESTION_ID ON EACH INGESTION -- END--
=======================================================================




-----------------------------------------------------------------------------------------------------------




=============================================================================
--CREATING QUARANTINE TABLE -- START--
=============================================================================

create or replace procedure operational_log.quarantine_table_create()
LANGUAGE plpgsql

as $$
BEGIN
    drop table if exists operational_log.quarantine;
    create table operational_log.quarantine(
        id SERIAL  primary key,
        ingestion_id int,
        table_name varchar(255),
        reject_reason varchar(255),
        flagged_at timestamp default current_timestamp,
        raw_row JSONB
    );
END;
$$;

call operational_log.quarantine_table_create();

=============================================================================
--CREATING QUARANTINE TABLE -- END--
=============================================================================



------------------------------------------------------------------------------------------------------------





=============================================================================
--CREATING ULTIMATE LOG TABLES ((BRONZE+SILVER))FOR EACH TABLE -- START--
=============================================================================



create or replace procedure operational_log.create_table_log()
LANGUAGE plpgsql

as $$
BEGIN

        drop table if exists operational_log.customers_log;
        create table operational_log.customers_log(
        ingestion_id int,
        table_name VARCHAR(255),
        bronze_row_count int,
        silver_daily_row_count int,
        silver_main_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        negative_count int,
        quarantine_count int,
        silver_daily_insert_executing_time INTERVAL,
        silver_main_insert_executing_time INTERVAL,
        silver_main_update_executing_time INTERVAL,
        total_silver_process_executing_time INTERVAL,
        bronze_daily_copy_executing_time INTERVAL,
        bronze_daily_indexing_time INTERVAL,
        bronze_main_insert_executing_time INTERVAL,
        total_bronze_process_executing_time INTERVAL,
        log_created_at timestamp default current_timestamp,
        primary key(ingestion_id,table_name,bronze_row_count)
        ) ;

        drop table if exists operational_log.order_items_log;
        create table operational_log.order_items_log(
        ingestion_id int,
        table_name VARCHAR(255),
        bronze_row_count int,
        silver_daily_row_count int,
        silver_main_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        negative_count int,
        quarantine_count int,
        silver_daily_insert_executing_time INTERVAL,
        silver_main_insert_executing_time INTERVAL,
        silver_main_update_executing_time INTERVAL,
        total_silver_process_executing_time INTERVAL,
        bronze_daily_copy_executing_time INTERVAL,
        bronze_daily_indexing_time INTERVAL,
        bronze_main_insert_executing_time INTERVAL,
        total_bronze_process_executing_time INTERVAL,
        log_created_at timestamp default current_timestamp,
        primary key(ingestion_id,table_name,bronze_row_count)
        ) ;

        drop table if exists operational_log.orders_log;
        create table operational_log.orders_log(
        ingestion_id int,
        table_name VARCHAR(255),
        bronze_row_count int,
        silver_daily_row_count int,
        silver_main_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        negative_count int,
        quarantine_count int,
        silver_daily_insert_executing_time INTERVAL,
        silver_main_insert_executing_time INTERVAL,
        silver_main_update_executing_time INTERVAL,
        total_silver_process_executing_time INTERVAL,
        bronze_daily_copy_executing_time INTERVAL,
        bronze_daily_indexing_time INTERVAL,
        bronze_main_insert_executing_time INTERVAL,
        total_bronze_process_executing_time INTERVAL,
        log_created_at timestamp default current_timestamp,
        primary key(ingestion_id,table_name,bronze_row_count)
        ) ;

        drop table if exists operational_log.payments_log;
        create table operational_log.payments_log(
        ingestion_id int,
        table_name VARCHAR(255),
        bronze_row_count int,
        silver_daily_row_count int,
        silver_main_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        negative_count int,
        quarantine_count int,
        silver_daily_insert_executing_time INTERVAL,
        silver_main_insert_executing_time INTERVAL,
        silver_main_update_executing_time INTERVAL,
        total_silver_process_executing_time INTERVAL,
        bronze_daily_copy_executing_time INTERVAL,
        bronze_daily_indexing_time INTERVAL,
        bronze_main_insert_executing_time INTERVAL,
        total_bronze_process_executing_time INTERVAL,
        log_created_at timestamp default current_timestamp,
        primary key(ingestion_id,table_name,bronze_row_count)
        ) ;

        drop table if exists operational_log.products_log;
        create table operational_log.products_log(
        ingestion_id int,
        table_name VARCHAR(255),
        bronze_row_count int,
        silver_daily_row_count int,
        silver_main_row_count int,
        null_pk_count int,
        other_null_count int,
        duplicate_count int,
        future_past_count int,
        negative_count int,
        quarantine_count int,
        silver_daily_insert_executing_time INTERVAL,
        silver_main_insert_executing_time INTERVAL,
        silver_main_update_executing_time INTERVAL,
        total_silver_process_executing_time INTERVAL,
        bronze_daily_copy_executing_time INTERVAL,
        bronze_daily_indexing_time INTERVAL,
        bronze_main_insert_executing_time INTERVAL,
        total_bronze_process_executing_time INTERVAL,
        log_created_at timestamp default current_timestamp,
        primary key(ingestion_id,table_name,bronze_row_count)
        ) ;

end;
$$;

call operational_log.create_table_log();

=============================================================================
--CREATING ULTIMATE LOG TABLES ((BRONZE+SILVER))FOR EACH TABLE -- END--
=============================================================================


-------------------------------------------------------------------------------------------------------------



==============================================================================
--CREATING INGESTION_ID TABLE -- START--
==============================================================================

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

=============================================================================
--CREATING INGESTION_ID TABLE -- END--
==============================================================================