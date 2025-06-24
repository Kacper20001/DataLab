import pandas as pd
from core.cleaner import clean_data

def test_clean_data_filters_invalid_records():
    df = pd.DataFrame({
        "trip_distance": [1.0, -2.0, 0.0, 3.5],
        "fare_amount": [5.0, -10.0, 0.0, 8.0],
        "passenger_count": [1, 0, 2, 1],
        "tip_amount": [0.5, 1.0, 2.0, 1.5],
        "total_amount": [10.0, -5.0, 5.0, 15.0],
        "tpep_pickup_datetime": pd.to_datetime([
            "2024-01-01 10:00", "2024-01-02 12:00", "2024-01-03 14:00", "2024-01-04 10:00"
        ]),
        "tpep_dropoff_datetime": pd.to_datetime([
            "2024-01-01 11:00", "2024-01-02 11:00", "2024-01-03 13:00", "2024-01-04 11:00"
        ])
    })

    cleaned = clean_data(df)

    # Oczekujemy tylko ostatniego rekordu
    assert len(cleaned) == 2, f"Oczekiwano 2 poprawnych wierszy, a otrzymano {len(cleaned)}"

    row = cleaned.iloc[0]
    assert row["trip_distance"] > 0
    assert row["fare_amount"] >= 0
    assert row["passenger_count"] > 0
    assert row["total_amount"] >= 0
    assert row["tpep_dropoff_datetime"] > row["tpep_pickup_datetime"]
