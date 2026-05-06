create or replace procedure operational_log.bronze_ingest_safetynet()
language PLPGSQL
as $$
BEGIN
drop table if exists operational_log.bronze_ingest_safetynet;
create table operational_log.bronze_ingest_safetynet(
    file_name varchar(255),
    table_name VARCHAR(255),
    file_path VARCHAR(255),
    created_at timestamp default current_date,
    primary key( table_name, created_at)
);
end;
$$;
call operational_log.bronze_ingest_safetynet();


select * from operational_log.bronze_ingest_safetynet;
