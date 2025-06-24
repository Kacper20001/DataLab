import pyarrow.parquet as pq

def stream_parquet(path: str, batch_size: int = 100_000):
    """
    Strumieniowo wczytuje dane z pliku Parquet w partiach (batchach).

    :param path: Ścieżka do pliku .parquet
    :param batch_size: Liczba wierszy na batch
    :yield: Kolejny fragment danych jako DataFrame
    """
    parquet_file = pq.ParquetFile(path)

    for batch in parquet_file.iter_batches(batch_size=batch_size):
        yield batch.to_pandas()
