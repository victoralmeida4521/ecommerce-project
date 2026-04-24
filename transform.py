import pandas as pd
import sqlite3
from datetime import datetime

import os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_path = os.path.join(BASE_DIR, "ecommerce.db")


def validate_dataframe(df, tablename):
    print(f"\n Validating {tablename}")

    row_count = df.shape[0]
    col_count = df.shape[1]
    null_counts = df.isnull().sum()
    dtypes = df.dtypes

    print(f"Linhas: {row_count}")
    print(f"Colunas: {col_count}")
    print("\n Valores nulos por coluna:")
    print(null_counts)
    print("\n Tipos de dados:")
    print(dtypes)

    return {
        "rows": row_count,
        "cols": col_count,
        "nulls": null_counts,
        "dtypes": dtypes.astype(str).to_dict()
    }

def transform_data_bronze_to_silver():
    conn = sqlite3.connect('ecommerce.db')

    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table'", conn
    )['name']

    tables = [t for t in tables if not t.endswith('_silver') and not t.endswith('_gold') and t != 'audit_bronze_to_silver']

    audit_logs = []

    for table in tables:
        print(f"\nTransformando {table}(bronze para silver)")

        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)

        #Validação 
        validation = validate_dataframe(df, table)

        # Aqui você pode adicionar as transformações necessárias para cada tabela
        transformed = False

        # Remover colunas totalmente nulas
        null_cols = df.columns[df.isnull().all()]
        if len(null_cols) > 0:
           df = df.drop(columns=null_cols)
           transformed = True

        # Nome da tabela silver
        silver_table = f"{table}_silver"

        df.to_sql(silver_table, conn, if_exists='replace', index=False)

        print(f"Tabela salva como {silver_table}")
        print(f"Transformação aplicada{transformed}")

        # Log de auditoria
        audit_logs.append({
            "table": table,
            "silver_table": silver_table,
            "transformed": transformed,
            "timestamp": datetime.now().isoformat(),
            "rows": validation["rows"],
            "cols": validation["cols"]
        })

    # salvar log
    audit_df = pd.DataFrame(audit_logs)
    audit_df.to_sql("audit_bronze_to_silver", conn, if_exists='replace', index=False)

    print("\n Log de auditoria Bronze para Silver salvo na tabela audit_bronze_to_silver")

    conn.close()