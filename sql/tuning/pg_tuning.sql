

====================
--PG tuning START--
===================

/*
Performance tuning : Permanant change.
*/
    ALTER SYSTEM SET shared_buffers = '2GB'; 
    -- Planner & I/O
    ALTER SYSTEM SET random_page_cost = 1.1;
    ALTER SYSTEM SET effective_cache_size = '6GB';
    -- Parallelism
    ALTER SYSTEM SET max_parallel_workers_per_gather = 4;
    -- Write Performance (Heavy Lifting)
    ALTER SYSTEM SET synchronous_commit = OFF;
    ALTER SYSTEM SET max_wal_size = '8GB';
    ALTER SYSTEM SET min_wal_size = '1GB';
    ALTER SYSTEM SET checkpoint_completion_target = 0.9;
    -- Maintenance & Operations
    ALTER SYSTEM SET maintenance_work_mem = '1GB'; /*1GB*/
    ALTER SYSTEM SET work_mem = '512MB'; /*128MB*/
    -- Apply (Note: shared_buffers requires a full DB restart)
    ALTER SYSTEM SET max_parallel_maintenance_workers = 4;
    ALTER SYSTEM SET max_parallel_workers = 12;  

    SELECT pg_reload_conf();

    
====================
--PG tuning END--
===================



========================
--PG Activity and WAL monitoring-- START
========================



select datname,pid, (now()-query_start)::time as time_,query as "query/command", state as "state/bytes_total" , backend_type as "backend_type/tuples_processed" from pg_stat_activity where state='active' and query not like '%select datname%' 
union all
select datname, pid,null::time, 
command as "que
ry/comman
d",bytes_total::text as "state/
bytes_tot
al",tuples_processed::text as "backend_type/tuples_processed" from   pg_stat_progress_copy;


SELECT * FROM pg_stat_wal;


select * from pg_stat_activity where state='active';
select * from information_schema.tables where table_name like '%stat%';

========================================================
--PG Activity and WAL monitoring-- END
========================================================