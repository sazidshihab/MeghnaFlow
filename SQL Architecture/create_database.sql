-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse

/*
SELECT pg_terminate_backend(pid) 
FROM pg_stat_activity 
WHERE datname = 'template1' 
  AND pid <> pg_backend_pid();
#to kill template1 database connection, if any
*/


create database "Demo_warehouse";
SELECT current_database();

select schema_name from information_schema.schemata
;

create schema bronze;
create schema silver;
create schema gold;


