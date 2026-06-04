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
    try:
        cur = conn.cursor()
        cur.execute(f"Analyze  {table_name};")
        conn.commit()
        conn.close()
        print(f"Analyze {table_name} completed in {time.time() - start} seconds")
    except Exception as e:
        print(f"Error analyzing {table_name}: {e}")
        conn.close()
        raise  
    finally:
        conn.close()     



analyze = ["silver.order_items_raw_p","silver.orders_raw_p","silver.payments_raw_p","gold.fact_sales","gold.fact_payments"]


def main():

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
    )

    try:
        cur = conn.cursor()
        cur.execute("""
        drop table if exists bronze.customers_raw_daily;
        drop table if exists bronze.orders_raw_daily;
        drop table if exists bronze.order_items_raw_daily;
        drop table if exists bronze.products_raw_daily;
        drop table if exists bronze.payments_raw_daily;
                                    
        drop table if exists silver.customers_daily;
        drop table if exists silver.orders_daily;
        drop table if exists silver.order_items_daily;
        drop table if exists silver.payments_daily;
        drop table if exists silver.products_daily;                                                        
                                    """)
        conn.commit()
    except Exception as e:
        print(f"Error during table drop: {e}")
        conn.rollback()
        raise
    finally:    
     conn.close()


    start = time.time()
    print("Starting performance boost procedures...")
    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(boost_performance, analyze)
       
    print(f"Performance boost procedures completed in {time.time() - start} seconds")


if __name__ == "__main__":
    main()   