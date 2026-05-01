from ETL.extract import extract_data_from_csv
from ETL.transform import transform_data_bronze_to_silver
from ETL.load import transform_data_silver_to_gold
from ETL.analysis import run_analysis


def main():
    print("=== INÍCIO DO PIPELINE ETL ===")

    try:
        print("\n[1] EXTRAÇÃO")
        extract_data_from_csv()
        print("✔ Extract OK")

        print("\n[2] TRANSFORMAÇÃO (BRONZE → SILVER)")
        transform_data_bronze_to_silver()
        print("✔ Transform OK")

        print("\n[3] TRANSFORMAÇÃO (SILVER → GOLD)")
        transform_data_silver_to_gold()
        print("✔ Load OK")

        print("\n[4] ANÁLISE")
        run_analysis()
        print("✔ Analysis OK")

        print("\n=== PIPELINE FINALIZADO COM SUCESSO ===")

    except Exception as e:
        print(f"\n❌ ERRO NO PIPELINE: {e}")


if __name__ == "__main__":
    main()

    