from concurrent.futures import process
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





###### Selft taught Python basic to advance for DE roles ######

### The five core Data types: ###
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


### Lists, tuples, dicts, sets — when to use which: ###
        #These four are Python's core "collection" types. Picking the right one is a real DE skill

        #List — ordered, changeable, allows duplicates, allows mixed types
        products = ["Rice", "Wheat", "Lentils", "Rice"]  # duplicates OK
        products.append("Sugar")        # add to end
        products[0] = "Basmati Rice"    # change in place (mutable)
        print(products[1])              # "Wheat" — indexed by position
        print(len(products))            # 5
        #Use a list when order matters and the contents will change — like rows you're accumulating as you read a file.

        #Tuple — ordered, unchangeable, allows duplicates, allows mixed types
        coordinates = (23.81, 90.41)   # Dhaka lat/lng
        # coordinates[0] = 24.0        # TypeError — tuples are immutable
        row = ("Rice", 50, "62.50")    # a fixed record
        #Use a tuple when the group should not change — a database row, a fixed config pair, a coordinate.
        #Immutability is a feature: it signals "this is a fixed unit" and prevents accidental edits. Tuples are also slightly faster and lighter than lists.


        #Dict — key-value pairs, the DE workhorse, — unordered, changeable, no duplicates, allows mixed types
        row = {
        "product": "Rice",
        "quantity": 50,
        "price": "62.50"
        }
        print(row["product"])          # "Rice" — lookup by key, not position
        print(row.get("products"))     # None — safe lookup, no crash if missing
        row["region"] = "Dhaka"        # add a new key
        row["price"] = "60.00"         # update existing key
        print(row)
        #This is the one you'll use most. Every JSON object, every CSV row parsed by csv.DictReader, every API response — they all come to you as dicts.
        #Note .get() vs ["..."]: .get() returns None instead of crashing when the key is missing, which ties straight back to the None-handling you just learned.


        #Set — unordered, unique values only, —  unindexed, no duplicates, allows mixed types
        seen_ids = {101, 102, 103}
        seen_ids.add(102)              # ignored — already there
        print(seen_ids)                # {101, 102, 103} — no duplicates
        print(102 in seen_ids)         # True — membership check is very fast
        #Use a set for deduplication and fast "have I seen this before?" checks.


        #Why the choice matters in DE — the performance trap???
        '''
        This is the part that separates junior from senior thinking.
        Checking membership (x in collection) is dramatically faster in a set than a list:

        # Slow: list membership is O(n) — scans every element
        valid_ids = [1, 2, 3, ..., 1_000_000]
        if user_id in valid_ids:       # checks up to a million items each time
            ...

        # Fast: set membership is O(1) — near-instant hash lookup
        valid_ids = {1, 2, 3, ..., 1_000_000}
        if user_id in valid_ids:       # one lookup regardless of size
        '''

        #Quick References:
        '''
        Type  Ordered  Mutable  Duplicates  Main_DE use
        list      Yes     Yes      Yes      Accumulating rows, sequences
        tuple     Yes     No       Yes      Fixed records, config pairs, composite keys
        dict      No      Yes      No       Key-value data, JSON, CSV rows, Parsed rows, configs
        set       No      Yes      No       Deduplication, membership checks
        '''



### Control flow: if / elif / else, for / while ###

        #Conditionals — the basics, fast:
        quantity = 50
        if quantity > 100:
         tier = "bulk"
        elif quantity > 10:        # only checked if the first was False
         tier = "standard"
        else:
         tier = "small"


        #The DE trap: truthiness vs explicit checks:
        price = 0          # a real, valid price of zero (free sample?)
        if not price:                    # BUG: 0 is falsy, fires as if missing
            print("price is missing")    # wrong — 0 is a real value!

        if price is None:                # CORRECT: only fires for actual missing
            print("price is missing")


        #for loops — and the DE mindset shift:
        # enumerate — when you need the index (e.g. row number for error logs)
        rows = [{"product": "Rice", "qty": 50}, {"product": "Wheat", "qty": 0}]

        for i, row in enumerate(rows, start=1):
            print(f"Row {i}: {row['product']}")

        # zip — walk two lists together
        products = ["Rice", "Wheat"]
        prices   = [62.50, 45.00]
        for p, pr in zip(products, prices):
            print(p, pr)

        # loop a dict
        for key, value in row.items():
            print(key, value)


        #DE mindset:
        '''
        the instinct to reach for a for loop over data is exactly what we discussed last topic. Looping in Python over 160M rows is slow.
        For data in pandas, you use vectorized operations; for data in the warehouse, you use SQL. 
        Python loops are for orchestration-level work (looping over files, tables, API pages — dozens or hundreds of things), not over millions of rows. 
        Loop over the 30 CSV files; don't loop over the 160M rows inside them.
        '''

        #Continue and break:
        for row in rows:
            if row["qty"] == 0:
                continue          # skip this row, move to next
            if row["product"] is None:
                break             # stop the whole loop entirely
            process(row)  

        #while loops — and the one place DEs actually use them:
        #You rarely loop while over data. The real DE use is retry logic and pagination — when you don't know how many iterations ahead of time

        # Paginating an API — you don't know how many pages exist
        '''
        page = 1
        all_data = []
        while True:
            response = fetch_api(page) #Hypothetical function to fetch one page of results
            if not response["results"]:    # no more data
                break
            all_data.extend(response["results"])
            page += 1 
            
            
        The while danger: an infinite loop. Always have a guaranteed exit — a break condition that will be reached, 
        or a counter cap like above. A while True with no reachable break will hang your pipeline forever. 
        This is why the counter pattern (attempts < max_attempts) is the safe default for retries.    
        '''

        #Real DE example of this block:
        '''
        def route_batch(rows: list) -> tuple:
            valid, quarantine = [], []

            for i, row in enumerate(rows, start=1):
                # explicit None checks — not truthiness
                if row.get("product") is None or row.get("price") is None:
                    quarantine.append((i, row, "missing field"))
                    continue                      # skip rest, next row

                if not isinstance(row["price"], (int, float)):
                    quarantine.append((i, row, "bad price type"))
                    continue

                if row["price"] < 0:              # 0 is allowed, negative isn't
                    quarantine.append((i, row, "negative price"))
                    continue

                valid.append(row)

            return valid, quarantine              # tuple of two lists

        '''



######  Functions: args, kwargs, return, scope ######

       




        



        

