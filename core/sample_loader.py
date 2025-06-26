"""
sample_loader.py

Moduł odpowiedzialny za załadowanie niewielkiej próbki danych z pliku .parquet
(np. do testów, eksploracji, wykresów). Próbka pochodzi z pierwszych kilku chunków
i jest czyszczona za pomocą clean_data.
"""

import pandas as pd
from decorators.timer import measure_time
from decorators.counter import count_calls
from core.cleaner import clean_data
from core.loader import load_parquet_in_chunks

@measure_time
@count_calls
def load_sample_for_visualization(path: str, chunksize: int = 100_000, max_chunks: int = 2) -> pd.DataFrame:
    """
    Ładuje i oczyszcza dane z pierwszych N chunków jako próbkę do wizualizacji.

    Args:
        path (str): Ścieżka do pliku .parquet.
        chunksize (int): Liczba wierszy na chunk.
        max_chunks (int): Maksymalna liczba chunków do załadowania.

    Returns:
        pd.DataFrame: Połączona i oczyszczona próbka danych.
    """
    chunks = []
    for i, chunk in enumerate(load_parquet_in_chunks(path, chunksize=chunksize)):
        cleaned = clean_data(chunk)
        chunks.append(cleaned)
        if i + 1 >= max_chunks:
            break

    if not chunks:
        return pd.DataFrame()  # fallback na pusty DataFrame

    return pd.concat(chunks, ignore_index=True)
