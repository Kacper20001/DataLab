from pipeline.taxi_pipeline import TaxiPipeline
from core.streamer import stream_parquet
from core.cleaner import stream_and_clean_data

if __name__ == "__main__":
    path = "data/raw/yellow_tripdata_2024-01.parquet"
    pipeline = TaxiPipeline(path)
    pipeline.run()

    # Test strumieniowego wczytywania
    print("\n--- Test strumieniowego wczytywania ---")
    for i, chunk in enumerate(stream_parquet("data/raw/yellow_tripdata_2024-01.parquet", batch_size=250_000)):
        print(f"Chunk {i + 1}: {len(chunk)} wierszy")
        if i == 2:
            break  # tylko 3 chunki testowo

    print("\n--- Test stream_and_clean_data ---")
    for i, cleaned_chunk in enumerate(stream_and_clean_data(path, chunksize=250_000)):
        print(f"Oczyszczony chunk {i + 1}: {len(cleaned_chunk)} wierszy")
        if i == 2:
            break
