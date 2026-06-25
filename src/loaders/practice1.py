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
from dotenv import load_dotenv
import os
from IPython.display import display

data_all = []
skip= 1

url = 'https://dummyjson.com/auth/login'

load_dotenv()

login_response = requests.post(
    url,
    json={"username": os.environ.get("DUMMY_USERNAME"), 
          "password": os.environ.get("DUMMY_PASSWORD")}
)
token = login_response.json()['accessToken']

headers = {
    "Authorization": f"Bearer {token}"
}

raw_main = []
page =1 


while True:
    skip = (page-1)*100
    response = requests.get("https://dummyjson.com/users",
    headers=headers,
    params={"skip":skip,"limit":100},
    timeout=5)

    response.raise_for_status()

    if not response.json()['users']:
        break
    print(f"Page : {page}")

    if response.status_code != 200:
        print(f"Error {response.status_code}: Server returned non-JSON text:")
        print(response.text[:200])  # Prints the first 200 characters of the error page
        break

    raw_main.extend(response.json()['users'])


    page+=1




display(raw_main)

main =pd.DataFrame(raw_main)

table_main = main[['id','firstName','lastName','maidenName','age','gender','email','phone','username','password','birthDate','image','bloodGroup','height','weight','hair','eyeColor','ip','macAddress','university','ein','ssn','role']]
table_main['hair_color']=table_main['hair'].apply(lambda x: x['color'])
table_main['hair_type'] = table_main['hair'].apply(lambda x: x['type'])
table_main = table_main.drop(columns=['hair'])



table_address =pd.DataFrame(main['address'].to_list())
table_address['id']=main['id']
table_address['lat']=table_address['coordinates'].apply(lambda x: x['lat'])
table_address['lng']=table_address['coordinates'].apply(lambda x: x['lng'])
table_address= table_address.drop(columns=['coordinates'])


table_bank = pd.DataFrame(main['bank'].to_list())
table_bank['id']=main['id']


table_company = pd.DataFrame(main['company'].to_list())
table_company['id']=main['id']


table_company_address =pd.DataFrame(table_company['address'].to_list())
table_company_address['id']=table_company['id']
table_company_address['lat']=table_company_address['coordinates'].apply(lambda x: x['lat'])
table_company_address['lng']=table_company_address['coordinates'].apply(lambda x: x['lng'])
table_company_address= table_company_address.drop(columns=['coordinates'])
table_company = table_company.drop(columns=['address'])



table_crypto = pd.DataFrame(main['crypto'].to_list())
table_crypto['id']=main['id']





display(pd.DataFrame(main))
display(pd.DataFrame(table_main))
display(pd.DataFrame(table_address))
display(pd.DataFrame(table_bank))
display(pd.DataFrame(table_company))
display(pd.DataFrame(table_company_address))
display(pd.DataFrame(table_crypto))










