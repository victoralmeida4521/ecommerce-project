import pandas as pd
print(pd.__version__)
import sqlite3

import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_path = os.path.join(BASE_DIR, "ecommerce.db")


def transform_data_silver_to_gold():
    conn = sqlite3.connect(DB_path)

    #------------------ 
    # GOLD 1: KPIs
    #------------------

    orders = pd.read_sql_query(
        "SELECT * FROM olist_orders_dataset_silver", conn
    )

    orders['order_purchase_timestamp'] = pd.to_datetime(
        orders['order_purchase_timestamp']
    )

    orders['date'] = orders['order_purchase_timestamp'].dt.date

    payments = pd.read_sql_query(
        "SELECT * FROM olist_order_payments_dataset_silver", conn
        
    )

    orders_per_day = (
        orders.groupby("date")["order_id"]
        .nunique()
        .reset_index(name="total_orders")
    )

    payments = pd.read_sql_query(
        "SELECT * FROM olist_order_payments_dataset_silver", conn   
    )

    df = orders.merge(payments, on="order_id", how="left")

    revenue_per_day = (
        df.groupby("date")["payment_value"]
        .sum()
        .reset_index(name="total_revenue")
    )

    gold_daily = orders_per_day.merge(revenue_per_day, on= 'date')
    gold_daily["ticket_medio"] = (
        gold_daily["total_revenue"] / gold_daily["total_orders"]
    )

    gold_daily.to_sql(
        "gold_daily_kpis",
        conn,
        if_exists="replace" , 
        index = False
    )

    #---------------------------- 
    # GOLD 2: Pedidos por cidade
    #----------------------------
    customers = pd.read_sql_query(
        "SELECT * FROM olist_customers_dataset_silver", conn
    )

    df = orders.merge(customers, on="customer_id", how="left")

    orders_by_city = (
        df.groupby("customer_city")
        .size()
        .reset_index(name="total_orders")
    )

    orders_by_city.to_sql(
        "gold_orders_by_city",
        conn,
        if_exists="replace",
        index=False
    )

    #---------------------------- 
    # GOLD 3: Produtos mais vendidos
    #----------------------------

    items = pd.read_sql_query(
        "SELECT * FROM olist_order_items_dataset_silver", conn       
    )

    products_sold = (
        items.groupby("product_id")
        .size()
        .reset_index(name="total_sold")
        .sort_values(by="total_sold", ascending=False)
    )

    products_sold.to_sql("gold_top_products", conn, if_exists="replace", index=False)
    

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
   
     
