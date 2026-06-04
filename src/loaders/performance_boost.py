from concurrent.futures import ThreadPoolExecutor
import time
import psycopg2




def boost_performance(table_name):
    start = time.time()
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
    )
    cur = conn.cursor()
    cur.execute(f"Analyze  {table_name};")
    conn.commit()
    conn.close()
    print(f"Analyze {table_name} completed in {time.time() - start} seconds")



analyze = ["silver.order_items_raw_p","silver.orders_raw_p","silver.payments_raw_p","gold.fact_sales","gold.fact_payments"]


def main():
    start = time.time()
    print("Starting performance boost procedures...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(boost_performance, analyze)
       
    print(f"Performance boost procedures completed in {time.time() - start} seconds")


if __name__ == "__main__":
    main()   