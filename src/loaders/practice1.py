from typing import Optional,Any,Union
dict1 : dict[str, dict[str, str | list[str]]] ={
    "val1":{
        "col1" :"""col1,col2,col3 """,
        "col2" :["6","7","8","9","10"]
    },
    "val2":{
        "col3" :["11","12","13","14","15"],
        "col4" :["16","17","18","19","20"]
    }
}


def n1(dict1: dict[str,dict[str,str|list[str]]],val : str, col:str) -> list[str | list[str]] | None:
   found = list(dict1[row][col] for row in dict1 if row is val and col in dict1[row]) 
   return found
print(n1(dict1,'val1','col1'))

#===============================   API Flattening   ===============================================
import pandas as pd
response = {
    "order_id": 5001,
    "customer": {
        "id": 200,
        "name": "Rahim",
        "address": {"city": "Dhaka", "zone": "Banani"}
    },
    "items": [
        {"product": "Rice", "qty": 2, "price": 62.50},
        {"product": "Wheat", "qty": 1, "price": 45.00}
    ]
}

def flatten_order(order: dict) -> list[dict]:
    """One nested order → one flat row per item."""
    customer = order.get("customer", {})
    city = customer.get("address", {}).get("city")

    rows = []
    for item in order.get("items", []):          # safe default empty list
        rows.append({
            "order_id": order.get("order_id"),
            "customer_name": customer.get("name"),
            "city": city,
            "product": item.get("product"),
            "qty": item.get("qty"),
            "price": item.get("price"),
        })
    return rows

print(pd.DataFrame(flatten_order(response)))
# the two nested items become two flat rows, ready for a database table


#===============================   API Calling and Handling   ===============================================
import requests
import json
import pandas as pd
from IPython.display import display

data_all = []
page= 1

url = 'https://api.openbrewerydb.org/v1/breweries'

while True:



    response = requests.get(url, params={"page":page,"per_page":100}, timeout=5)

    print("Status Code: ", response.status_code)

    #print("Headers: ", response.headers)




    # Always check if the request was successful
    if response.status_code == 200:
        data = response.json()

        if not data:
            break
        data_all.extend(data)
    
    else:
        print(f"Error {response.status_code}: Server returned non-JSON text:")
        print(response.text[:200])  # Prints the first 200 characters of the error page
        break
    page+=1

    display(pd.DataFrame(data_all))
