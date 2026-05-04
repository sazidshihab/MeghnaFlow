# 🌊 MeghnaFlow

**End-to-End Medallion Data Platform | Bronze → Silver → Gold**

MeghnaFlow is a production-style data engineering project built around the Medallion Architecture using PostgreSQL and Apache Airflow. The pipeline ingests 160M+ rows of synthetic transactional data (customers, orders, sales, payments) across five domain tables — progressively refining raw data into a clean, validated, analytics-ready Star Schema, fully automated with incremental load logic, upsert strategies, and an audit control layer.

Covers the full DE/DA spectrum — raw ingestion, deduplication, data quality, business transformations, dimensional modeling, and pipeline orchestration — with zero manual intervention.

---

**Stack:** Python · PostgreSQL · Apache Airflow · SQL  
**Scale:** 160M+ rows · 5 tables · Incremental load · Upsert pipeline · Star Schema
