import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor


procedures_daily = [
    "silver.ingest_silver_daily_products()",
    "silver.ingest_silver_daily_customers()",
    "silver.ingest_silver_daily_payments()",
    "silver.ingest_silver_daily_order_items()",
    "silver.ingest_silver_daily_orders()"
    ]

procedures_raw =[
      "silver.ingest_silver_raw_products()",
      " silver.ingest_silver_raw_customers()",
      "silver.ingest_silver_raw_payments()",
      "silver.ingest_silver_raw_order_items()",
      "silver.ingest_silver_raw_orders()"
]


def log():
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
    )

    sql = f"""
    select ingestion_id from operational_log.ingestion_id;
    """

    cur = conn.cursor()
    cur.execute(sql)
    ingestion_id = cur.fetchone()[0]

    sql = f"""
    call silver.create_silver_daily_tables();
    """

    cur.execute(sql)

    conn.commit()
    cur.close()
    return ingestion_id


def run_procedures(name):
       conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
    )
       time1=time.time()                              #Start timing after connection — measures only procedure execution

       try:
           cur = conn.cursor()
           sql = f"""
           call {name};
           """
           cur.execute(sql)
           elapsed = time.time() - time1             #Capture immediately after procedure returns
           print(f"Procedure {name} executed successfully.,{elapsed}")
           conn.commit()
           cur.close()
           conn.close()

       except Exception as e:
           print(f"An error occurred: {e}")  



def main():

    time1=time.time()
    ingestion_id = log()
    #Multithreading with 5 threads
    try: 
        with ThreadPoolExecutor(max_workers=5) as executor:
          list(executor.map(run_procedures, procedures_daily))
          print(f"Total time taken: {time.time()-time1}")
    except Exception as e:
        print(f"An error occurred: {e}")

    try:
        time1=time.time()
        with ThreadPoolExecutor(max_workers=5) as executor:
          list(executor.map(run_procedures, procedures_raw))
          print(f"Total time taken: {time.time()-time1}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()                     

      






