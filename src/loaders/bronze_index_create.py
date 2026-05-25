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

        with ThreadPoolExecutor(max_workers=5) as executor:
         list(executor.map(run_procedure, procedures))
         print("ALL THREADS FINISHED")


if __name__ == "__main__":
    main()
