
import pandas as pd
import numpy as np
from faker import Faker


fake = Faker()

# -----------------------
# CONFIG (adjust size)
# -----------------------
N_CUSTOMERS = 5001
N_PRODUCTS = 3001
N_ORDERS = 200000  # reduce if your Mac struggles

# -----------------------
# 1. PRODUCTS
# -----------------------
food_catalog = {
    "Apple": ("Fruit", 0.50),
    "Banana": ("Fruit", 0.30),
    "Bread": ("Bakery", 4.50),
    "Cheese": ("Dairy", 5.99),
    "Milk": ("Dairy", 3.25),
    "Chicken": ("Meat", 8.50),
    "Rice": ("Pantry", 1.50)
}

product_names = list(food_catalog.keys())

products = pd.DataFrame({
    "product_id": list(range(214000,214000+N_PRODUCTS)),
    "name": np.random.choice(product_names, N_PRODUCTS)
})

products["category"] = products["name"].map(lambda x: food_catalog[x][0])
products["price"] = products["name"].map(lambda x: food_catalog[x][1])

# -----------------------
# 2. CUSTOMERS
# -----------------------
customers = pd.DataFrame({
    "customer_id": list(range(215000,215000+N_CUSTOMERS)),
    "name": [fake.name() for _ in range(N_CUSTOMERS)],
    "signup_date": pd.to_datetime(
        np.random.randint(18000, 20300, N_CUSTOMERS), unit="D"
    )
})

# -----------------------
# 3. ORDERS
# -----------------------
customer_ids = customers["customer_id"].values

orders = pd.DataFrame({
    "order_id": list(range(109500000,109500000+N_ORDERS)),
    "customer_id": np.random.choice(customer_ids, N_ORDERS),
})

orders["order_date"] = pd.to_datetime(
    np.random.randint(20600, 20606, N_ORDERS), unit="D"
)

orders["status"] = np.random.choice(
    ["Completed", "completed", "Pending", "Cancelled"], N_ORDERS
)

# -----------------------
# 4. ORDER ITEMS (EXPLODE STYLE)
# -----------------------
items_per_order = np.random.randint(1, 4, N_ORDERS)
order_ids_repeated = np.repeat(orders["order_id"].values, items_per_order)

N_ITEMS = len(order_ids_repeated)

product_ids = products["product_id"].values

order_items = pd.DataFrame({
    "order_id": order_ids_repeated,
    "product_id": np.random.choice(product_ids, N_ITEMS),
    "quantity": np.random.randint(1, 6, N_ITEMS)
})

# map price
price_map = products.set_index("product_id")["price"]
order_items["unit_price"] = order_items["product_id"].map(price_map)

# introduce messy data
mask = np.random.rand(N_ITEMS) < 0.05
order_items.loc[mask, "unit_price"] = None  # nulls

order_items["total"] = order_items["quantity"] * order_items["unit_price"]

# -----------------------
# 5. PAYMENTS
# -----------------------
payments_base = order_items.groupby("order_id")["total"].sum().reset_index()
payments_base = payments_base.merge(orders[["order_id", "customer_id", "order_date"]], on="order_id")

n = len(payments_base)
payments = pd.DataFrame({
    "payment_id":   list(range(171000000, 171000000+n)),
    "method":       np.random.choice(["card", "Card", "cash", "CASH"], n),
    "order_id":     payments_base["order_id"].values,
    "customer_id":  payments_base["customer_id"].values,
    "order_date":   payments_base["order_date"].values,
    "total":        payments_base["total"].values,
    "payment_date": payments_base["order_date"].values + pd.Timedelta(days=1),
})

# introduce messy payments
mask = np.random.rand(len(payments)) < 0.03
payments.loc[mask, "total"] = None

# -----------------------
# SAVE TO CSV
# -----------------------
products.to_csv("products_raw_03_06_25v11.csv", index=False)
customers.to_csv("customers_raw_03_06_25v11.csv", index=False)
orders.to_csv("orders_raw_03_06_25v11.csv", index=False)
order_items.to_csv("order_items_raw_03_06_25v11.csv", index=False)
payments.to_csv("payments_raw_03_06_25v11.csv", index=False)

print("✅ All datasets generated successfully!")



# %%
