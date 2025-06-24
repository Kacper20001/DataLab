from core.streamer import stream_parquet
from decorators.timer import measure_time
from decorators.counter import count_calls
from validation.run import run_all_validations
import pandas as pd
import psutil

NEEDED_COLUMNS = [
    "passenger_count", "trip_distance", "tip_amount",
    "total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"
]


@measure_time
@count_calls
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    return run_all_validations(df)

@measure_time
@count_calls
def stream_and_clean_data(path: str, chunksize: int = 100_000):
    """
    Strumieniowo wczytuje i czyści dane z pliku Parquet.

    :param path: Ścieżka do pliku .parquet
    :param chunksize: Liczba wierszy na chunk
    :yield: Oczyszczony DataFrame (chunk)
    """
    for chunk in stream_parquet(path, chunksize=chunksize, columns=NEEDED_COLUMNS):
        cleaned = clean_data(chunk)
        mem = psutil.Process().memory_info().rss / 1024 ** 2
        print(f"[Cleaner] Aktualne zużycie pamięci: {mem:.2f} MB")
        yield cleaned