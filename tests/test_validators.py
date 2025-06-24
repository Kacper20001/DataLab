import pandas as pd
from validation.run import run_all_validations

def test_validators_pass_on_clean_data():
    df = pd.DataFrame({
        "trip_distance": [1.0, 2.0],
        "fare_amount": [10.0, 20.0],
        "total_amount": [15.0, 25.0],
        "passenger_count": [1, 2],
        "tip_amount": [1.5, 2.0],
        "tpep_pickup_datetime": pd.to_datetime(["2024-01-01 00:00", "2024-01-02 00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2024-01-01 01:00", "2024-01-02 01:00"])
    })

    validated_df = run_all_validations(df)

    assert len(validated_df) == 2, f"Oczekiwano 2 poprawnych wierszy, ale jest {len(validated_df)}"

