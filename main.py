from core.loader import load_parquet

if __name__ == "__main__":
    path = "data/raw/yellow_tripdata_2024-01.parquet"
    df = load_parquet(path)

    print(f"Liczba rekordów: {len(df)}")
    print("Dostępne kolumny:")
    print(df.columns.tolist())
    print(df.head(5))