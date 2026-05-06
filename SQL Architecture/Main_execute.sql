-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse
call bronze.create_tables_bronze(); /* Creating all raw table on bronze for the first time*/

call bronze.bronze_ingest(); /* Ingesting data into the bronze layer from the CSV files,
on this procedure one table contains daily data and raw table contains all time data. */

call silver.create_tables_silver(); /* Creating all raw table on silver for the first time*/

call silver.silver_daily_table_drop();/*Using this step untill creating of gold layer, just dropping daily silver table.*/

call silver.silver_import_full(); /* Ingesting data into the silver layer from the bronze layer (FULL LOAD),
here also daily table contains daily data and raw table contains all time data. */


