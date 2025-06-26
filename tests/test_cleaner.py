"""
test_cleaner.py

Zestaw testów jednostkowych dla funkcji `clean_data` z modułu core.cleaner.

Testy sprawdzają:
- czy poprawne dane przechodzą walidację,
- czy błędne dane są usuwane (ujemne wartości, zera, błędne daty),
- czy obsługiwane są wartości NaN,
- czy pusty DataFrame nie powoduje błędów.
"""

import pandas as pd
from core.cleaner import clean_data

def test_clean_data_valid_row_passes():
    """
    Poprawny rekord powinien przejść walidację bez zmian.
    """
    df = pd.DataFrame([{
        "trip_distance": 2.5,
        "fare_amount": 12.0,
        "passenger_count": 1,
        "tip_amount": 3.0,
        "total_amount": 15.0,
        "tpep_pickup_datetime": pd.to_datetime("2024-01-01 10:00"),
        "tpep_dropoff_datetime": pd.to_datetime("2024-01-01 10:30")
    }])

    cleaned = clean_data(df)

    assert len(cleaned) == 1
    row = cleaned.iloc[0]
    assert row["trip_distance"] >= 0
    assert row["fare_amount"] > 0
    assert row["passenger_count"] > 0
    assert row["total_amount"] >= 0
    assert row["tpep_dropoff_datetime"] > row["tpep_pickup_datetime"]

def test_clean_data_removes_invalid_rows():
    """
    Rekordy z błędnymi wartościami powinny zostać usunięte.
    Zostanie tylko jeden poprawny rekord.
    """
    df = pd.DataFrame([
        {
            "trip_distance": -1.0,
            "fare_amount": 5.0,
            "passenger_count": 1,
            "tip_amount": 1.0,
            "total_amount": 6.0,
            "tpep_pickup_datetime": pd.to_datetime("2024-01-01 10:00"),
            "tpep_dropoff_datetime": pd.to_datetime("2024-01-01 10:30")
        },
        {
            "trip_distance": 2.0,
            "fare_amount": 0.0,
            "passenger_count": 1,
            "tip_amount": 1.0,
            "total_amount": 5.0,
            "tpep_pickup_datetime": pd.to_datetime("2024-01-01 10:00"),
            "tpep_dropoff_datetime": pd.to_datetime("2024-01-01 10:30")
        },
        {
            "trip_distance": 1.5,
            "fare_amount": 10.0,
            "passenger_count": 0,
            "tip_amount": 2.0,
            "total_amount": 12.0,
            "tpep_pickup_datetime": pd.to_datetime("2024-01-01 10:00"),
            "tpep_dropoff_datetime": pd.to_datetime("2024-01-01 10:30")
        },
        {
            "trip_distance": 3.0,
            "fare_amount": 8.0,
            "passenger_count": 1,
            "tip_amount": 2.0,
            "total_amount": 10.0,
            "tpep_pickup_datetime": pd.to_datetime("2024-01-01 12:00"),
            "tpep_dropoff_datetime": pd.to_datetime("2024-01-01 11:00")
        },
        {
            "trip_distance": 1.0,
            "fare_amount": 5.0,
            "passenger_count": 1,
            "tip_amount": 1.0,
            "total_amount": 6.0,
            "tpep_pickup_datetime": pd.to_datetime("2024-01-02 09:00"),
            "tpep_dropoff_datetime": pd.to_datetime("2024-01-02 09:30")
        }
    ])

    cleaned = clean_data(df)

    assert len(cleaned) == 1, f"Powinien zostać tylko 1 poprawny rekord, a dostałem {len(cleaned)}"
    pickup = cleaned.iloc[0]["tpep_pickup_datetime"]
    assert pickup == pd.to_datetime("2024-01-02 09:00")

def test_clean_data_handles_nan_values():
    """
    Rekord z wartością NaN powinien zostać usunięty.
    """
    df = pd.DataFrame([{
        "trip_distance": 2.0,
        "fare_amount": None,
        "passenger_count": 1,
        "tip_amount": 1.0,
        "total_amount": 10.0,
        "tpep_pickup_datetime": pd.to_datetime("2024-01-01 10:00"),
        "tpep_dropoff_datetime": pd.to_datetime("2024-01-01 10:30")
    }])

    cleaned = clean_data(df)
    assert cleaned.empty, "Rekord z NaN powinien zostać wyrzucony"

def test_clean_data_empty_df_returns_empty():
    """
    Pusty DataFrame powinien zwrócić pusty wynik (bez błędów).
    """
    df = pd.DataFrame(columns=[
        "trip_distance", "fare_amount", "passenger_count",
        "tip_amount", "total_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"
    ])
    cleaned = clean_data(df)
    assert cleaned.empty, "Pusty DataFrame powinien zwrócić pusty wynik"
