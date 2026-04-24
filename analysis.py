import pandas as pd
import sqlite3 
import matplotlib.pyplot as plt
import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_path = os.path.join(BASE_DIR, "ecommerce.db")

conn = sqlite3.connect("ecommerce.db")

kpis = pd.read_sql("SELECT * FROM gold_kpis", conn)
print ("KPIs do negócio:")
print(kpis)

cities = pd.read_sql("SELECT * FROM gold_orders_kpis", conn)
top_cities = cities.sort_values(by="total_orders", ascending=False).head(10)

print("\nTop 10 cidades com mais pedidos")
print(top_cities)

plt.figure()
plt.bar(top_cities["customer_city"], top_cities["total_orders"])
plt.xticks(rotation=45)
plt.title("Top 10 cidades com mais pedidos")
plt.show()

products = pd.read_sql("SELECT * FROM gold_top_products", conn)
top_products = products.head(10)

print(top_products)


plt.figure()
plt.bar(top_products["products_id"], top_products["total_sold"])
plt.xticks(rotation=90)
plt.title("Top 10 produtos mais vendidos")
plt.show()

conn.close()

