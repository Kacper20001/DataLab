from core.streamer import stream_parquet
from decorators.timer import measure_time
from decorators.counter import count_calls
import pandas as pd

@measure_time
@count_calls
def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Czyści dane NYC Taxi z podstawowych błędów i anomalii.

    :param df: Surowy DataFrame z danymi o przejazdach
    :return: Odfiltrowany DataFrame
    """

    # Tylko kursy z dodatnią liczbą pasażerów
    df = df[df["passenger_count"] > 0]

    # Tylko kursy z dodatnim dystansem
    df = df[df["trip_distance"] > 0]

    # Tylko kursy z dodatnią kwotą za przejazd
    df = df[df["fare_amount"] >= 0]
    df = df[df["total_amount"] >= 0]

    # Daty – czas zakończenia musi być późniejszy niż rozpoczęcia
    df = df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

    # Usuwanie duplikatów (jeśli występują)
    df = df.drop_duplicates()

    # Reset indeksu po odfiltrowaniu
    df.reset_index(drop=True, inplace=True)

    return df

@measure_time
@count_calls
def stream_and_clean_data(path: str, chunksize: int = 100_000):
    """
    Strumieniowo wczytuje i czyści dane z pliku Parquet.

    :param path: Ścieżka do pliku .parquet
    :param chunksize: Liczba wierszy na chunk
    :yield: Oczyszczony DataFrame (chunk)
    """
    for chunk in stream_parquet(path, chunksize):
        cleaned = clean_data(chunk)
        yield cleaned