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

select * from operational_log.quarantine ;
select * from operational_log.bronze_ingest_safetynet;


select raw_row->>'name',raw_row->>'customer_id',raw_row->>'signup_date'  from operational_log.quarantine;