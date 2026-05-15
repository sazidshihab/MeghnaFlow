# MeghnaFlow Project Notes

Last updated: 2026-05-15

## Current Source Of Truth

Use this project folder as the main source:

`/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/MeghnaFlow/MeghnaFlow`

Important SQL files:

- `SQL Architecture/bronze_import.sql`
- `SQL Architecture/silver_import.sql`
- `SQL Architecture/silver_process.sql`
- `SQL Architecture/operational_log.sql`
- `SQL Architecture/Main_execute.sql`

## Current Project State

MeghnaFlow is an end-to-end PostgreSQL medallion data warehouse project.

Current completed focus:

- Bronze CSV ingestion
- Silver daily staging tables
- Silver main tables
- Optimized validation procedures
- Quarantine logging
- Operational runtime and row-count logging
- Update-then-insert loading logic

Planned next focus:

- Gold layer star schema
- Airflow orchestration
- Data quality checks
- Split table-level procedures for parallel execution

## Silver Optimization Result

Old Bronze-to-Silver runtime:

- `58m 51.1s`

New optimized Bronze-to-Silver runtime:

- `41m 14.3s`

Approximate improvement:

- Saved `17m 36.8s`
- About `29.9%` faster

Main improvements came from:

- Optimized validation procedures
- Set-based `DELETE ... RETURNING` quarantine pattern
- Update-then-insert instead of heavier `ON CONFLICT` flow
- `IS DISTINCT FROM` to avoid unnecessary updates
- `GET DIAGNOSTICS ROW_COUNT` for update/insert logging
- `UNLOGGED` daily tables
- Targeted primary keys/indexes on daily and main tables

## Current Decision

Do not spend much more time micro-optimizing the single sequential Silver procedure.

Small possible gains remain:

- Replace some `count(*)` scans with `GET DIAGNOSTICS ROW_COUNT`
- Add a direct first-load path for empty Silver tables
- Tune indexes only if `EXPLAIN ANALYZE` proves a clear bottleneck

Expected gain from those small SQL changes is likely only around `1-3 minutes`.

The next meaningful performance gain should come from architecture:

- Split `silver.silver_import_full()` into table-level procedures:
  - `silver.load_payments()`
  - `silver.load_order_items()`
  - `silver.load_customers()`
  - `silver.load_orders()`
  - `silver.load_products()`
- Run independent table loads in parallel through separate sessions or Airflow tasks.

## Parallel Execution Note

Running table loads from separate terminal sessions creates real PostgreSQL parallel execution because each call uses a separate DB connection.

Best first parallel test:

- Run heavy tables in parallel:
  - `payments`
  - `order_items`
  - `orders`

Customers and products are small and do not matter much for runtime.

Be careful that parallel procedures do not reset shared `operational_log.ingestion_id` or recreate shared log tables.

## Interview / LinkedIn Story

Suggested wording:

> Optimized the Bronze-to-Silver processing pipeline for 80M+ rows in PostgreSQL, reducing runtime from 58m 51s to 41m 14s, about a 30% improvement. Reworked validation into set-based quarantine logic, replaced heavier upsert flow with update-then-insert loading, added execution and row-count logging, and validated performance on local PostgreSQL.

## Reminder

Old validation procedures are intentionally kept in `silver_process.sql` to demonstrate before/after performance improvement.

