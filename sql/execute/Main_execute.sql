-- Active: 1776668343304@@127.0.0.1@5432@Demo_warehouse
call bronze.create_tables_bronze(); /* Creating all raw table on bronze for the first time*/

call bronze.bronze_ingest(); /* Ingesting data into the bronze layer from the CSV files,
on this procedure one table contains daily data and raw table contains all time data. */

call bronze.drop_all_tables_bronze_daily();
/*dropping daily table on bronze layer, using this step until creating of silver layer, just to keep the bronze layer clean and organized.*/

call silver.create_tables_silver(); /* Creating all raw table on silver for the first time*/

call silver.silver_daily_table_drop();/*Using this step untill creating of gold layer, just dropping daily silver table.*/



/*Full silver sequential import*/
call silver.silver_import_full(); /* Ingesting data into the silver layer from the bronze layer (FULL LOAD),
here also daily table contains daily data and raw table contains all time data. */



/*Full silver parallel import*/


--1. Run above below procedures on one session on terminal to execute those 3 sequentially for best performance. --
/*
call silver.parallel_silver_import_payment();
call silver.parallel_silver_import_orders();
call silver.parallel_silver_import_customers_products();
*/

--2. Run this procedure parallelly on different session on terminal.--
/*
call silver.parallel_silver_import_order_items();
*/

--Parallel run saves around 30% time than sequential run for silver import. Time may vary based on the system configuration and data size.

