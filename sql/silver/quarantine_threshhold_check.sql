

create or replace procedure silver.quarantine_threshold_check()

language plpgsql as $$

DECLARE
    pro_table_name text;
    pro_quarantine_count int;
    pro_ingestion_id int;
    bronze_row_count int;
    duplicate_count int;
    ratio numeric(10,2);

BEGIN
      pro_ingestion_id := (select ingestion_id from operational_log.ingestion_id); --select ingestion_id from operational_log.ingestion_id;


      for pro_table_name,pro_quarantine_count IN
      select  table_name,count(*) FROM
      operational_log.quarantine 
      where ingestion_id = pro_ingestion_id
      group by 1

      LOOP

            select bronze_daily_row_count into bronze_row_count
            from operational_log.bronze_ingest_safetynet
            where table_name = pro_table_name
            and ingestion_id = pro_ingestion_id;

            EXECUTE format(
                'SELECT silver_daily_duplicate_count 
                FROM operational_log.%I 
                WHERE ingestion_id = $1', 
                pro_table_name || '_log'
            ) 
            INTO duplicate_count 
            USING pro_ingestion_id;

            ratio := (pro_quarantine_count::numeric + COALESCE(duplicate_count, 0)::numeric) / nullif(bronze_row_count, 0) * 100;

            if ratio > 10 THEN
                    RAISE EXCEPTION 'Quarantine threshold exceeded for table % with ratio %[Quarantine count:%, Ingested count:%]', pro_table_name, ratio, pro_quarantine_count+duplicate_count, bronze_row_count;
            END IF;


            raise notice 'Table: %, Quarantine Count: %, Ingested Count: %, Ratio: %', pro_table_name, pro_quarantine_count+duplicate_count, bronze_row_count, ratio;

      END LOOP;

end;
$$;


call silver.quarantine_threshold_check();



