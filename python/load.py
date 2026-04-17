import pandas as pd
print(pd.__version__)
import sqlite3

DB_path = 'ecommerce.db'

def transform_data_silver_to_gold():
    conn = sqlite3.connect(DB_path)

    #------------------ 
    # GOLD 1: KPIs
    #------------------

    orders = pd.read_sql_query(
        "SELECT * FROM olist_orders_dataset_silver", conn
    )

    payments = pd.read_sql_query(
        "SELECT * FROM olist_order_payments_dataset_silver", conn
        
    )

    total_orders = orders['order_id'].nunique()
    total_revenue = payments['payment_value'].sum()
    ticket_medio = total_revenue / total_orders 

    kpis = pd.DataFrame({
        "total_orders": [total_orders],
        "total_revenue": [total_revenue],
        "ticket_medio": [ticket_medio]
    })
    kpis.to_sql("gold_kpis", conn, if_exists = "replace", index=False)

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
    
    conn.close()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
   
     
