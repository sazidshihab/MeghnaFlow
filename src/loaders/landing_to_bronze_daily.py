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
    truncate table operational_log.ingestion_id;
    insert into operational_log.ingestion_id(created_at) values(now())
    returning ingestion_id;
    """

    for i in table_names:                                             #Truncating daily table before ingestion
        table_name=extract_table_name(i.stem)                         #Getting table name part from path
        config=table_config.get(table_name)                           #Getting actual table name from predefined dictionary

        sql_truncate = f"""
        truncate table {config['target_table']};  
        """                                                             #Truncating table
        cur = conn.cursor()
        cur.execute(sql_truncate)
        conn.commit()
        cur.close()
    
    
    cur = conn.cursor()
    cur.execute(sql)
    ingestion_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
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
    

def count_rows(file_path):                                       #Function to count number of rows for each individual file copied
    row1= 0

    with open(file_path, 'rb') as f:

        return sum(

            chunk.count(b'\n')

            for chunk in iter(lambda: f.read(1024 * 1024), b'')

        ) - 1    




def csv_generator(file,table_name):                    #GENERATOR FUNCTION(it reads data in a chunk of 150000 lines and append table_id to each row)
    reader=csv.reader(file)   
    next(reader)  # Skip header row
    for row in reader:
        row.append(str(table_name))  # Append table name to each row
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
        insert into operational_log.bronze_raw_daily_ingest_log(ingestion_id,source_file_name,table_name,row_count,executing_time)
        values(%s,%s,%s,%s,NULL)
        RETURNING source_file_id;;
        """                                   #Inserting ingestion log
        
    

        conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="Demo_warehouse",
        user="sazid",
        )

        cur = conn.cursor()                           #Executing log query
        cur.execute(log_sql,(ingestion_id,csv_files.stem,table_name,count_rows(csv_files)))
        table_id=cur.fetchone()[0]
        conn.commit()


        with open(csv_files, 'r') as f:              #Opening file but not loading to RAM
    
            generator=csv_generator(f,table_id)       #Calling generator(Which load and yield data in a chunk of 150000 lines with the help of read() method)

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
            cur.execute("update operational_log.bronze_raw_daily_ingest_log set executing_time = %s where source_file_id = %s",(time.time()-time1,table_id)) #Updating execution time
            conn.commit()
            print(f"Data from {csv_files.name} has been loaded into {config['target_table']}")

        cur.close()
        conn.close()    


def main():


    time1=time.time()

    landing_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/Demo_warehouse/Data/Landing')
    csv_files = list(landing_folder.glob("*.csv"))                 #Listing all files in landing folder

    ingestion_id = log(csv_files)                                 #Calling log function
    
    try: 
        with ThreadPoolExecutor(max_workers=5) as executor:              #Multithreading with 5 threads
          list(executor.map(file_loaded, csv_files, [ingestion_id] * len(csv_files)))
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
    





