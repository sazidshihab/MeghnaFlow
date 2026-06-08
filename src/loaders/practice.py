import csv
from gettext import install

import pandas as pd
from pathlib import Path
import re

import psycopg2

print(psycopg2.__path__)

import numpy as np
txt = np.loadtxt('/Users/sazid/Work Station/SQL PDF/Warehouse Project/MeghnaFlow_/Data/Archive/customers_raw_03_06_25v2.csv', delimiter=',', dtype=str, skiprows=1, usecols=[0,1,2])
#print(txt)

import pandas as pd
df=pd.read_csv('/Users/sazid/Work Station/SQL PDF/Warehouse Project/MeghnaFlow_/Data/Archive/customers_raw_03_06_25v2.csv',nrows=5, delimiter=',', comment='#',na_values=['Zachary James'])
print(df)





blk = [1,5,7,2,2,3,4,5,6,7,8,9]
blk1 = set(blk)

print(blk1.add(65))
print(re.__path__)










blk = [1,2,3,4,5,6,7,8,9]
r=-1


for i in range(int(len(blk)/2)):
    if blk[i]==4:
        print(f"got, iteration:{i+1}")
        break
    elif  blk[r]==4:
        print(f"got, iteration reverse:{abs(r)}")
        break
    r-=1
 
      









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
        #print(match.group(0))
        #print(next(csv.reader(f)))

        conn = psycopg2.connect(
            host="localhost",
            port = 5432,
            database="Demo_warehouse",
            user="sazid"
        )

        cur=conn.cursor()
        cur.execute(
            "select * from operational_log.bronze_ingest_log where ingestion_id=(select ingestion_id from operational_log.ingestion_id);"
        )
        print(cur.fetchall()[0][0])
        conn.close()





### Selft taught Python basic to advance for DE roles ###

##The five core Data types:
        name = "MeghnaFlow"      # str   — text
        row_count = 160_000_000  # int   — whole numbers (note: underscores for readability)
        price = 1234.56          # float — decimals
        is_valid = True          # bool  — True / False
        missing = None           # None  — the absence of a value

        print(type(name))   
        print(isinstance(name,bool))    
        
        #Best practice avoid float, use decimal or int for money values to avoid precision issues.

        from decimal import Decimal
        price = Decimal("1234.56")   # exact — pass it as a string, not a float
        total = price * 3
        print(total)


        discount = None
        #final = 100 - discount   # TypeError: unsupported operand type(s)
        final = 100 - (discount if discount is not None else 0)


## Lists, tuples, dicts, sets — when to use which
        

