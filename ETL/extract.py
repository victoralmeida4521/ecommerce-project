import pandas as pd
import os
import sqlite3
from datetime import datetime

from config import DB_path, DATA_PATH

def extract_data_from_csv():
    with sqlite3.connect(DB_path) as conn:

        # -------------------------
        # Listar arquivos .csv
        # -------------------------
        csv_files = [f for f in os.listdir(DATA_PATH) if f.endswith('.csv')]
        print("Dados listados")

        # -------------------------
        # ler os arquivos csv e armazenar no .db
        # -------------------------
        for csv_file in csv_files:
            df = pd.read_csv(os.path.join(DATA_PATH, csv_file))
            table_name = csv_file.split('.')[0]

            df.to_sql(table_name,con=conn, if_exists='replace', index=False)
            print(f"{csv_file} Dados armazenados no banco de dados.")   

        
        return csv_files

    if __name__ == "__main__":
        extract_data_from_csv()