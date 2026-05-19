from pathlib import Path
import psycopg2
import re
import time
from concurrent.futures import ThreadPoolExecutor
import shutil
import csv
import threading


def log(table_names):                                               #This table only run once, just to get ingestion id and truncate daily table
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
        )
                                                                     #Insert new ingestion id on each new ingestion
    sql= f"""                                                  
    select ingestion_id from operational_log.ingestion_id;
    """


    sql_truncate = f"""
    call bronze.create_unlogged_bronze_daily_tables();  
    """                                                             #resetting bronze daily table
    cur = conn.cursor()
    cur.execute(sql_truncate)
    cur.execute(sql)
    ingestion_id = cur.fetchone()[0]

    conn.commit()
    cur.close()
    return ingestion_id



table_config = {                                                  #Dictionary of table name and its configuration
"customers" :
    {
        "columns":"""

        customer_id, name, signup_date,source_file_id """,
        
        "target_table": "bronze.customers_raw_daily"
    },
"orders" :
    {
        "columns":"""

        order_id, customer_id, order_date, status,source_file_id """,
        "target_table": "bronze.orders_raw_daily"
    },
"order_items" :
    {
        "columns":"""

        order_id, product_id, quantity, unit_price, total,source_file_id """,
        "target_table": "bronze.order_items_raw_daily"
    },
"payments" :
    {
        "columns":"""

        payment_id, method, order_id, order_date, total, payment_date,source_file_id """,
        "target_table": "bronze.payments_raw_daily"
    },
"products" :
    {
        "columns":"""

        product_id, name, category, price,source_file_id """,
        "target_table": "bronze.products_raw_daily"
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
        
        time1=time.time()
        table_name = extract_table_name(csv_files.stem)
        
        
        config=table_config.get(table_name)


        sql = f"""                                
        COPY {config['target_table']}({config['columns']})
        FROM STDIN
        WITH (
            FORMAT CSV
        ) 
        """                                   #Copying data from landing to bronze

        log_sql= f"""
        insert into operational_log.bronze_ingest_log(ingestion_id,source_file_name,table_name,ingestion_for,row_count,executing_time)
        values(%s,%s,%s,'bronze_daily',0,NULL)
        RETURNING source_file_id;;
        """                                   #Inserting ingestion log (row_count updated after copy)



        conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
        )

        cur = conn.cursor()                           #Executing log query
        cur.execute(log_sql,(ingestion_id,csv_files.stem,table_name))
        table_id=cur.fetchone()[0]



        row_counter = [0]                             #Counter incremented inside generator — no separate file scan needed
        with open(csv_files, 'r') as f:              #Opening file but not loading to RAM

            generator=csv_generator(f,table_id,row_counter)

            class FileWrapper:                   #Warp Class, so generator returned rows can be send as file to copy quer
                def __init__(self,generator):
                    self.generator = generator
                def read(self, size=-1):          #This method called by copy_expert auto until all rows are copied
                    try:
                        chunk =[]
                        for _ in range(150000):
                            chunk.append(next(self.generator))  #Instead of row by row we are sending a chunk of 150000 rows to copy

                    except StopIteration:
                        pass                                  # Return empty string to signal end of file
                    return ''.join(chunk)


            cur.copy_expert(sql, FileWrapper(generator))       #Executing copy query
            cur.execute(
                "update operational_log.bronze_ingest_log set executing_time = %s, row_count = %s where source_file_id = %s",
                (time.time()-time1, row_counter[0], table_id)
            )
            conn.commit()
            print(f"Data from {csv_files.name} has been loaded into {config['target_table']}")

        cur.close()
        conn.close()    


def main():


    time1=time.time()

    landing_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/MeghnaFlow/Data/Landing')
    csv_files = list(landing_folder.glob("*.csv"))  
    #Listing all files in landing folder

    ingestion_id = log(csv_files)                                 #Calling log function


                                                               #Multithreading with 5 threads
    try: 
        with ThreadPoolExecutor(max_workers=5) as executor:              
          list(executor.map(file_loaded, csv_files, [ingestion_id] * len(csv_files)))
          
          
          print(f"Total time taken: {time.time()-time1}")
          #move file from landing to archive after loading
          archive_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/MeghnaFlow/Data/Archive')
          for csv_file in csv_files:
              shutil.move(csv_file, archive_folder)

          print("ALL FILES MOVED TO ARCHIVE FOLDER")
          
    except Exception as e:
        print(f"An error occurred: {e}")
       



if __name__ == "__main__":
   
    main()
    





