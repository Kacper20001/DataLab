"""
cleaner.py

Moduł odpowiedzialny za czyszczenie i walidację danych w formie chunków.

Zawiera funkcję clean_data, która:
- filtruje dane za pomocą walidatora
- loguje liczbę rekordów przed i po przetworzeniu
- mierzy zużycie pamięci RAM

W przypadku błędu zwraca pusty DataFrame, aby nie przerywać całego procesu.
"""

import pandas as pd
import psutil
from decorators.timer import measure_time
from decorators.counter import count_calls
from validation.validation_runner import validate_chunk
from core.logger import logger

NEEDED_COLUMNS = [
    "passenger_count", "trip_distance", "tip_amount", "total_amount",
    "fare_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"
]

@measure_time
@count_calls
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Czyści i waliduje pojedynczy chunk danych.

    Args:
        df (pd.DataFrame): Chunk danych do przetworzenia.

    Returns:
        pd.DataFrame: Przefiltrowany i zweryfikowany chunk. W razie błędu — pusty DataFrame.
    """
    try:
        initial_len = len(df)
        cleaned_df = validate_chunk(df)
        cleaned_len = len(cleaned_df)
        mem = psutil.Process().memory_info().rss / 1024 ** 2
        logger.info(
            f"[Cleaner] Chunk: {initial_len} → {cleaned_len} rekordów po walidacji | RAM: {mem:.2f} MB"
        )
        return cleaned_df
    except Exception as e:
        logger.error(f"[Cleaner] Błąd podczas czyszczenia chunku: {e}")
        return pd.DataFrame()  # Nie przerywamy całego procesu przy multiprocessing
