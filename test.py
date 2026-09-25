import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load .env
load_dotenv()

# -----------------------------
# PostgreSQL Connection
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

if DATABASE_URL:
    db_url = DATABASE_URL
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)
else:
    db_url = f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST', 'localhost')}:{int(os.getenv('POSTGRES_PORT', 5432))}/{os.getenv('POSTGRES_DATABASE')}"

engine = create_engine(db_url)
db = engine.raw_connection()
cursor = db.cursor()

# -----------------------------
# Read CSV files
# -----------------------------
products_df = pd.read_csv("electronics_products_100.csv")
orders_df = pd.read_csv("electronics_orders_100.csv")

# -----------------------------
# Insert Products
# -----------------------------
product_sql = """
INSERT INTO products
(product_id, product_name, category, brand, price, stock)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (product_id) DO UPDATE SET
product_name = EXCLUDED.product_name,
category = EXCLUDED.category,
brand = EXCLUDED.brand,
price = EXCLUDED.price,
stock = EXCLUDED.stock
"""

for _, row in products_df.iterrows():
    cursor.execute(
        product_sql,
        (
            row["product_id"],
            row["product_name"],
            row["category"],
            row["brand"],
            float(row["price"]),
            int(row["stock"])
        )
    )

print("Products inserted successfully!")

# -----------------------------
# Insert Orders
# -----------------------------
order_sql = """
INSERT INTO orders
(order_id, customer_id, product_id, quantity, order_date, order_status)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (order_id) DO UPDATE SET
customer_id = EXCLUDED.customer_id,
product_id = EXCLUDED.product_id,
quantity = EXCLUDED.quantity,
order_date = EXCLUDED.order_date,
order_status = EXCLUDED.order_status
"""

for _, row in orders_df.iterrows():
    cursor.execute(
        order_sql,
        (
            row["order_id"],
            row["customer_id"],
            row["product_id"],
            int(row["quantity"]),
            row["order_date"],
            row["order_status"]
        )
    )

print("Orders inserted successfully!")

# -----------------------------
# Save Changes
# -----------------------------
db.commit()

cursor.close()
db.close()

print("Data inserted into PostgreSQL successfully!")