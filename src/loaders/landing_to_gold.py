import psycopg2
import time
from concurrent.futures import ThreadPoolExecutor


procedures_dims = [
    "gold.load_gold_dim_customers()",
    "gold.load_gold_dim_products()",
]

procedures_facts = [
    "gold.load_gold_fact_sales()",
    "gold.load_gold_fact_payments()",
]


def run_procedure(name):
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
    )
    time1 = time.time()
    try:
        cur = conn.cursor()
        cur.execute(f"call {name};")
        elapsed = time.time() - time1
        print(f"Procedure {name} executed successfully. {elapsed:.2f}s")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred in {name}: {e}")
        conn.close()


def main():
    time1 = time.time()

    # Phase 1: dims in parallel (independent of each other)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(run_procedure, procedures_dims))
        print(f"Dims loaded. Elapsed: {time.time() - time1:.2f}s")
    except Exception as e:
        print(f"Dim load error: {e}")

    # Phase 2: facts in parallel (require dims to exist first)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            list(executor.map(run_procedure, procedures_facts))
        print(f"Facts loaded. Total gold time: {time.time() - time1:.2f}s")
    except Exception as e:
        print(f"Fact load error: {e}")


if __name__ == "__main__":
    main()
