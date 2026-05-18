import psycopg2
from concurrent.futures import ThreadPoolExecutor





procedures = [
    "bronze.brone_daily_to_bronze_main_products",
    "bronze.brone_daily_to_bronze_main_customers",
    "bronze.brone_daily_to_bronze_main_orders",
    "bronze.brone_daily_to_bronze_main_order_items",
    "bronze.brone_daily_to_bronze_main_payments"
]




def run_procedure(name):
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
        print(f"Procedure {name} executed successfully.")
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
         executor.map(run_procedure, procedures)
         print("ALL THREADS FINISHED")


if __name__ == "__main__":
    main()
