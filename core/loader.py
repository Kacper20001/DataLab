import pandas as pd
from decorators.timer import measure_time
from decorators.counter import count_calls
from core.logger import logger


@measure_time
@count_calls
def load_parquet_in_chunks(path: str, chunksize: int = 100_000, columns: list[str] | None = None):
    """
    Generator wczytujący dane z pliku Parquet w chunkach przy użyciu pandas.

    Args:
        path (str): Ścieżka do pliku Parquet.
        chunksize (int): Liczba wierszy na chunk.
        columns (list[str] | None): Lista kolumn do załadowania (opcjonalnie).

    Yields:
        pd.DataFrame: Kolejny fragment danych jako DataFrame.
    """
    try:
        df = pd.read_parquet(path, columns=columns, engine="pyarrow")
        total_rows = len(df)
        logger.info(f"[Loader] Wczytano {total_rows} wierszy z pliku: {path}")

        if total_rows == 0:
            logger.warning(f"[Loader] Brak danych do przetworzenia w pliku {path}")
            return

        for i in range(0, total_rows, chunksize):
            chunk = df.iloc[i:i + chunksize]
            logger.info(f"[Loader] Chunk {i // chunksize + 1} załadowany ({len(chunk)} wierszy)")
            yield chunk
    except Exception as e:
        logger.error(f"[Loader] Błąd podczas wczytywania pliku {path}: {e}")
        return
