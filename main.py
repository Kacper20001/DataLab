from pipeline.taxi_pipeline import TaxiPipeline
from core.streamer import stream_parquet
from core.cleaner import stream_and_clean_data

if __name__ == "__main__":
    # Ścieżka do dużego zbioru danych Yellow Taxi NYC
    path = "data/raw/yellow_tripdata_2024-01.parquet"

    # Tworzenie i uruchamianie pipeline'u danych
    pipeline = TaxiPipeline(path)
    pipeline.run()

    # Test strumieniowego wczytywania 3 chunków
    print("\n--- Test strumieniowego wczytywania ---")
    for i, chunk in enumerate(stream_parquet(path, batch_size=250_000)):
        print(f"Chunk {i + 1}: {len(chunk)} wierszy")
        if i == 2:
            break

    # Test strumieniowego czyszczenia danych
    print("\n--- Test stream_and_clean_data ---")
    for i, cleaned_chunk in enumerate(stream_and_clean_data(path, chunksize=250_000)):
        print(f"Oczyszczony chunk {i + 1}: {len(cleaned_chunk)} wierszy")
        if i == 2:
            break
