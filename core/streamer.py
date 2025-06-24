import pyarrow.parquet as pq
import pyarrow as pa


def stream_parquet(path, chunksize=100_000, columns=None):
    """
    Generator zwracający kolejne chanki z pliku Parquet.

    Parameters:
        path (str): Ścieżka do pliku Parquet
        chunksize (int): Rozmiar chanka (liczba wierszy)
        columns (list[str]): Lista kolumn do załadowania (opcjonalna)

    Yields:
        pd.DataFrame: kolejny fragment danych
    """
    parquet_file = pq.ParquetFile(path)
    for batch in parquet_file.iter_batches(batch_size=chunksize, columns=columns):
        yield pa.Table.from_batches(batches=[batch]).to_pandas()
