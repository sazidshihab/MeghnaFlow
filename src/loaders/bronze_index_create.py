import psycopg2
from concurrent.futures import ThreadPoolExecutor
import time





procedures = [
    "bronze.create_bronze_daily_customers_index",
    "bronze.create_bronze_daily_order_items_index",
    "bronze.create_bronze_daily_payments_index",
    "bronze.create_bronze_daily_main_orders_index",
    "bronze.create_bronze_daily_products_index"
]


# VACUUM (ANALYZE) the freshly-COPYed daily tables before indexing so the planner
# has fresh stats and the visibility map is set. That lets the silver DISTINCT ON
# read the (pk, created_at_bronze DESC) index in order via an index-only scan and
# skip its sort.
daily_tables = [
    "bronze.customers_raw_daily",
    "bronze.order_items_raw_daily",
    "bronze.payments_raw_daily",
    "bronze.orders_raw_daily",
    "bronze.products_raw_daily"
]


def vacuum_analyze(table):
    time1 = time.time()
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
    )
    conn.autocommit = True            # VACUUM cannot run inside a transaction block
    try:
        cur = conn.cursor()
        cur.execute(f"VACUUM (ANALYZE) {table};")
        print(f"VACUUM (ANALYZE) {table} done in {time.time()-time1} seconds.")
        cur.close()
    except Exception as e:
        print(f"An error occurred during VACUUM of {table}: {e}")
    finally:
        conn.close()


def run_procedure(name):
    time1=time.time()
    conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="Demo_warehouse",
    user="sazid",
    )

    try: 
        cur = conn.cursor()
        sql = f"""
        call {name}();
        """
        cur.execute(sql)
        print(f"Procedure {name} executed successfully in {time.time()-time1} seconds.")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred: {e}")







def main():
        sql = f"""
        truncate table operational_log.bronze_ingest_safetynet;
        """
        conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
        )
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

        # 1. Refresh stats + visibility map on the freshly-COPYed daily tables.
        with ThreadPoolExecutor(max_workers=5) as executor:
         list(executor.map(vacuum_analyze, daily_tables))
         print("ALL VACUUM (ANALYZE) FINISHED")

        # 2. Build the covering indexes + write the safetynet log.
        with ThreadPoolExecutor(max_workers=5) as executor:
         list(executor.map(run_procedure, procedures))
         print("ALL THREADS FINISHED")


if __name__ == "__main__":
    main()
