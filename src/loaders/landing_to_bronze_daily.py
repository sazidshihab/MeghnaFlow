from pathlib import Path
import psycopg2
import re
import time
from concurrent.futures import ThreadPoolExecutor
import shutil
import csv
import threading




table_config = {
"customers" :
    {
        "columns":"""

        customer_id, name, signup_date,source_file_name """,
        
        "target_table": "bronze.customers_raw_daily"
    },
"orders" :
    {
        "columns":"""

        order_id, customer_id, order_date, status,source_file_name """,
        "target_table": "bronze.orders_raw_daily"
    },
"order_items" :
    {
        "columns":"""

        order_id, product_id, quantity, unit_price, total,source_file_name """,
        "target_table": "bronze.order_items_raw_daily"
    },
"payments" :
    {
        "columns":"""

        payment_id, method, order_id, order_date, total, payment_date,source_file_name """,
        "target_table": "bronze.payments_raw_daily"
    },
"products" :
    {
        "columns":"""

        product_id, name, category, price,source_file_name """,
        "target_table": "bronze.products_raw_daily"
    }

}

def extract_table_name(filename):

    filename = re.match(r"^(customers|orders|order_items|payments|products)",filename)
    if filename:

     return filename.group(0)
    else:
     return None
    




def csv_generator(file,table_name):
    reader=csv.reader(file)   
    next(reader)  # Skip header row
    for row in reader:
        row.append(table_name)  # Append table name to each row
        yield ','.join(row) + '\n' 
    



def file_loaded(csv_files):
        print(threading.current_thread().name)
        time1=time.time()
        table_name = extract_table_name(csv_files.stem)
        
        config=table_config.get(table_name)


        sql = f"""
        COPY {config['target_table']}({config['columns']})
        FROM STDIN
        WITH (
            FORMAT CSV,
            HEADER TRUE
        )
        """

        conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
        )

        cur = conn.cursor()

        with open(csv_files, 'r') as f:
            generator=csv_generator(f,csv_files.stem)

            class FileWrapper:
                def __init__(self,generator):
                    self.generator = generator
                def read(self, size=-1):
                    try:
                        chunk =[]
                        for _ in range(150000):
                            chunk.append(next(self.generator))
                       
                    except StopIteration:
                        return ""  # Return empty string to signal end of file
                    return ''.join(chunk)    


            cur.copy_expert(sql, FileWrapper(generator))
            conn.commit()
            print(f"Data from {csv_files.name} has been loaded into {config['target_table']}")
            print(f"Time taken to load {csv_files.name}: {time.time()-time1} seconds, clocked at: {time.ctime()}")


        cur.close()
        conn.close()    


def main():
    time1=time.time()
    landing_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing')
    csv_files = list(landing_folder.glob("*.csv"))
    
    try: 
        with ThreadPoolExecutor(max_workers=5) as executor:
          list(executor.map(file_loaded, csv_files))
          print("ALL THREADS FINISHED") 

          #move file from landing to archive after loading
          archive_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Archive')
          for csv_file in csv_files:
              shutil.move(csv_file, archive_folder)

          print("ALL FILES MOVED TO ARCHIVE FOLDER")
          print(f"Total time taken: {time.time()-time1}")
    except Exception as e:
        print(f"An error occurred: {e}")
       



if __name__ == "__main__":
   
    main()
    





