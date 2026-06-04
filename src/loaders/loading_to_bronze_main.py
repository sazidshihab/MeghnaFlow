from pathlib import Path
import psycopg2
import re
import time
from concurrent.futures import ThreadPoolExecutor
import csv


def log():                                               #This table only run once, just to get ingestion id and truncate daily table
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
        )
                                                                     #Insert new ingestion id on each new ingestion
    sql= f"""                                                  
    truncate table operational_log.ingestion_id;
    insert into operational_log.ingestion_id(created_at) values(now())
    returning ingestion_id;
    """

    cur = conn.cursor()
    cur.execute(sql)
    ingestion_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    return ingestion_id



EXPECTED_HEADERS = {
    "customers":   ["customer_id", "name", "signup_date"],
    "orders":      ["order_id", "customer_id", "order_date", "status"],
    "order_items": ["order_id", "product_id", "quantity", "unit_price", "total"],
    "payments":    ["payment_id", "method", "order_id", "customer_id", "order_date", "total", "payment_date"],
    "products":    ["product_id", "name", "category", "price"],
}

def validate_headers(csv_file, table_name):
    with open(csv_file, 'r') as f:
        actual = next(csv.reader(f))
    expected = EXPECTED_HEADERS[table_name]
    if actual != expected:
        raise ValueError(
            f"Schema drift in {csv_file.name}: "
            f"expected {expected}, got {actual}"
        )


table_config = {                                                  #Dictionary of table name and its configuration
"customers" :
    {
        "columns":"""

        customer_id, name, signup_date,source_file_id """,
        
        "target_table": "bronze.customers_raw"
    },
"orders" :
    {
        "columns":"""

        order_id, customer_id, order_date, status,source_file_id """,
        "target_table": "bronze.orders_raw"
    },
"order_items" :
    {
        "columns":"""

        order_id, product_id, quantity, unit_price, total,source_file_id """,
        "target_table": "bronze.order_items_raw"
    },
"payments" :
    {
        "columns":"""

        payment_id, method, order_id, customer_id, order_date, total, payment_date,source_file_id """,
        "target_table": "bronze.payments_raw"
    },
"products" :
    {
        "columns":"""

        product_id, name, category, price,source_file_id """,
        "target_table": "bronze.products_raw"
    }

}

def extract_table_name(filename):                                   #Function to extract table name

    filename = re.match(r"^(customers|orders|order_items|payments|products)",filename)
    if filename:                                         

     return filename.group(0)
    else:
     return None
    


def csv_generator(file, table_name, row_counter):
    reader = csv.reader(file)
    next(reader)  # Skip header row
    for row in reader:
        row.append(str(table_name))
        row_counter[0] += 1
        yield ','.join(row) + '\n'


    



def file_loaded(csv_files,ingestion_id):              #Function to copy data from landing to bronze

        table_name = extract_table_name(csv_files.stem)

        config=table_config.get(table_name)


        sql = f"""
        COPY {config['target_table']}({config['columns']})
        FROM STDIN
        WITH (
            FORMAT CSV
        )
        """ 
        
        
                                          #Copying data from landing to bronze

        log_sql= f"""
        insert into operational_log.bronze_ingest_log(ingestion_id,source_file_name,table_name,ingestion_for,row_count,executing_time)
        values(%s,%s,%s,'bronze_raw',0,NULL)
        RETURNING source_file_id;
        """                                   #Inserting ingestion log (row_count updated after copy)



        conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
        )

        try:
            cur = conn.cursor()
            cur.execute(log_sql,(ingestion_id,csv_files.stem,table_name))
            table_id=cur.fetchone()[0]

            row_counter = [0]
            with open(csv_files, 'r') as f:

                generator=csv_generator(f,table_id,row_counter)

                class FileWrapper:
                    def __init__(self,generator):
                        self.generator = generator
                    def read(self, _size=-1):
                        try:
                            chunk =[]
                            for _ in range(150000):
                                chunk.append(next(self.generator))
                        except StopIteration:
                            pass
                        return ''.join(chunk)

                time1 = time.time()
                cur.copy_expert(sql, FileWrapper(generator))
                copy_time = time.time() - time1
                cur.execute(
                    "update operational_log.bronze_ingest_log set executing_time = %s, row_count = %s  where source_file_id = %s",
                    (copy_time, row_counter[0], table_id)
                )
                conn.commit()
                print(f"Data from {csv_files.name} has been loaded into {config['target_table']} in {copy_time} seconds")

            cur.close()
        finally:
            conn.close()


def get_new_files(csv_files):
    conn = psycopg2.connect(host="localhost", port=5432, database="Demo_warehouse", user="sazid")
    cur = conn.cursor()
    cur.execute(
        """
        SELECT source_file_name FROM operational_log.bronze_ingest_log
        WHERE source_file_name = ANY(%s) AND ingestion_for = 'bronze_raw'
        """,
        ([f.stem for f in csv_files],)
    )
    already_ingested = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    return [f for f in csv_files if f.stem not in already_ingested]


def main():

    time1 = time.time()

    landing_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/MeghnaFlow_/Data/Landing')
    csv_files = list(landing_folder.glob("*.csv"))

    new_files = get_new_files(csv_files)

    if not new_files:
        print("All files already ingested. Nothing to do.")
        return

    for csv_file in new_files:                                    #All-or-nothing: validate every file before touching DB
        table_name = extract_table_name(csv_file.stem)
        if table_name:
            validate_headers(csv_file, table_name)

    ingestion_id = log()

    try:
        with ThreadPoolExecutor(max_workers=5) as executor:
            list(executor.map(file_loaded, new_files, [ingestion_id] * len(new_files)))
        print(f"Total time taken: {time.time()-time1}")
    except Exception as e:
        print(f"An error occurred: {e}")
        raise
       



if __name__ == "__main__":
   
    main()
    





