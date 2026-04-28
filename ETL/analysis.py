import pandas as pd
import sqlite3 
import matplotlib.pyplot as plt
import os

# -------------------------
# Caminho do banco
# ------------------------- 
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_path = os.path.join(BASE_DIR, "ecommerce.db")

conn = sqlite3.connect(DB_path)

# -------------------------
# KPIs diários
# -------------------------

kpis = pd.read_sql("SELECT * FROM gold_daily_kpis", conn)

print ("KPIs do negócio:")
print(kpis.head())

#Converter data para datetime
kpis["date"] = pd.to_datetime(kpis["date"])

# -------------------------
# INSIGHT 1: crescimento
# -------------------------
kpis["growth"] = kpis["total_orders"].pct_change()

print("\nCrescimento diário (%): ")
print(kpis[["date", "growth"]].head())

# -------------------------
# Gráfico pedidos por dia
# -------------------------
plt.figure()
plt.plot(kpis["date"], kpis["total_orders"])
plt.xticks(rotation=45)
plt.title("Pedidos por dia")
plt.tight_layout()
plt.show()

# -------------------------
# Gráfico ticket médio
# -------------------------
plt.figure()
plt.plot(kpis["date"], kpis["ticket_medio"])
plt.xticks(rotation=45)
plt.title("Ticket médio ao longo do tempo")
plt.tight_layout()
plt.show()

# -------------------------
# Top cidades
# -------------------------
cities = pd.read_sql("SELECT * FROM gold_orders_by_city", conn)

top_cities = cities.sort_values(
    by="total_orders",
    ascending=False
).head(10)

print("\nTop 10 cidades com mais pedidos: ")
print(top_cities)

# INSIGHT
top_city = top_cities.iloc[0]["customer_city"]
top_city_orders = top_cities.iloc[0]["total_orders"]

print(f"\nCidade com mais pedidos: {top_city}({top_city_orders}pedidos)")

# Gráfico
plt.figure()
plt.bar(top_cities["customer_city"], top_cities["total_orders"])
plt.xticks(rotation=45)
plt.title("Top 10 cidades com mais pedidos")
plt.tight_layout()
plt.show()

# -------------------------
# Top produtos
# -------------------------
products = pd.read_sql("SELECT * FROM gold_top_products", conn)

top_products = products.head(10)

print("\nTop 10 produtos mais vendidos:")
print("top_products")

# INSIGHT
top_product = top_products.iloc[0]["product_id"]
top_sales = top_products.iloc[0]["total_sold"]

print(f"\nProduto mais vendido: {top_product} ({top_sales} vendas)")

# Gráfico
plt.figure()
plt.bar(top_products["product_id"], top_products["total_sold"])
plt.xticks(rotation=90)
plt.title("Top 10 produtos mais vendidos")
plt.tight_layout()
plt.show()

conn.close()































