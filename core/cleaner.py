import pandas as pd


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