import csv
import pandas as pd
from pathlib import Path
import re
import psycopg2

dict ={
    "col1" :["1","2","3","4","5"],
    "col2" :["6","7","8","9","10"]
}

dict1 ={
    "val1":{
        "col1" :"""col1,col2,col3 """,
        "col2" :["6","7","8","9","10"]
    },
    "val2":{
        "col3" :["11","12","13","14","15"],
        "col4" :["16","17","18","19","20"]
    }
}



df = pd.DataFrame(dict)

#print(dict1["val1"]["col1"])
print(type(dict1["val1"]["col2"]))



landing_folder = Path('/Users/sazid/Work Station/SQL PDF/Warehouse Project/MeghnaFlow_/Data/Landing')
csv_files = list(landing_folder.glob("*.csv"))

for csv_file in csv_files:
    with open(csv_file, 'r') as f:
        match = re.match(r"^payments|products|customers|orders|order_items", csv_file.stem)
        print(match.group(0))
        print(next(csv.reader(f)))

        conn = psycopg2.connect(
            host="localhost",
            port = 5432,
            database="Demo_warehouse",
            user="sazid"
        )

        conn.cursor().execute(
            "select * from operational_log.bronze_ingest_log;"
        )
        print(conn.cursor().fetchall())
        conn.close()



