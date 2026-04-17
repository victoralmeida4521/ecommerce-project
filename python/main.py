from extract import extract_data_from_csv
from transform import transform_data_bronze_to_silver
from load import transform_data_silver_to_gold

def main():
    extract_data_from_csv()
    transform_data_bronze_to_silver()
    transform_data_silver_to_gold()

if __name__ == "__main__":
    main()