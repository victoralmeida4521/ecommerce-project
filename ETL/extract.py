import pandas as pd
import os
import sqlite3
from datetime import datetime

import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_path = os.path.join(BASE_DIR, "ecommerce.db")

def extract_data_from_csv():
    conn = sqlite3.connect(DB_path)

    ## listar os arquivos csv
    csv_files = [f for f in os.listdir("C:\\estudos\\ecommerce-project\\data") if f.endswith('.csv')]
    print("Dados listados")
    ## ler os arquivos csv e armazenar no .db
    for csv_file in csv_files:
        df = pd.read_csv(os.path.join("C:\\estudos\\ecommerce-project\\data", csv_file))
        table_name = csv_file.split('.')[0]

        df.to_sql(table_name,con=conn, if_exists='replace', index=False)
        print(f"{csv_file} Dados armazenados no banco de dados.")   

    conn.close()
    return csv_files
