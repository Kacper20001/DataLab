import pandas as pd
from typing import List
from decorators.timer import measure_time
from decorators.counter import count_calls


# Lista wymaganych kolumn z danych NYC Yellow Taxi
REQUIRED_COLUMNS: List[str] = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount",
    "payment_type"
]

@measure_time
@count_calls
def load_parquet(path: str) -> pd.DataFrame:
    """
    Wczytuje dane z pliku Parquet i sprawdza, czy zawierają wymagane kolumny.

    :param path: Ścieżka do pliku .parquet
    :return: DataFrame zawierający dane o przejazdach NYC Yellow Taxi
    :raises ValueError: jeśli plik jest uszkodzony lub brakuje wymaganych kolumn
    """
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        raise ValueError(f"Błąd podczas wczytywania pliku Parquet: {e}")

    missing = []
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            missing.append(col)
    if missing:
        raise ValueError(f"Brakuje wymaganych kolumn w zbiorze danych: {missing}")

    return df