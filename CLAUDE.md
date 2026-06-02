# Warehouse Project — Claude Memory

## Project Overview
Data warehouse pipeline project built on PostgreSQL.
Medallion architecture: Bronze → Silver → Gold layers.
Daily incremental/append pipeline using pg_partman.

## Tech Stack
- Database: PostgreSQL with pg_partman (partition management)
- Orchestration: Airflow (learning)
- Transformation: dbt (upcoming)
- Language: Python for pipeline scripts
- Future: Kafka, Spark, AWS

## Architecture Conventions
- Bronze: raw append-only ingestion, partitioned daily
- Silver: normalized, cleaned, deduplicated
- Gold: aggregated, business-ready
- Use UNLOGGED → INSERT → SET LOGGED pattern for bulk partition loads
- All pipelines are incremental/append — never full refresh

## SQL Style
- Uppercase SQL keywords
- snake_case for table and column names
- Always specify schema prefix (bronze., silver., gold.)

## My Learning Goals
- Currently learning: Airflow, data warehouse design
- Near future: dbt
- Target: Data Engineer job in Bangladesh market

## What I Want From Claude
- Always explain WHY, not just what — I am learning
- Point out industry best practices when relevant
- Flag if my approach differs from production standards
- Keep code clean and interview-portfolio worthy