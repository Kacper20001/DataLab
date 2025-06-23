from core.loader import load_parquet
from core.cleaner import clean_data

if __name__ == "__main__":
    path = "data/raw/yellow_tripdata_2024-01.parquet"
    df_raw = load_parquet(path)

    print(f"Przed czyszczeniem: {len(df_raw)} rekordów")

    df_cleaned = clean_data(df_raw)

    print(f"Po czyszczeniu: {len(df_cleaned)} rekordów")
    print("Przykładowe dane:")
    print(df_cleaned.head(5))