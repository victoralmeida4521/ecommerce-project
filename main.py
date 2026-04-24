from extract import extract_data_from_csv
from transform import transform_data_bronze_to_silver
from load import transform_data_silver_to_gold



def main():
    print("ETL iniciado")

    extract_data_from_csv()
    print("Extract OK")

    transform_data_bronze_to_silver()
    print("Transform OK")

    transform_data_silver_to_gold()
    print("Load OK")
    

if __name__ == "__main__":
    main()