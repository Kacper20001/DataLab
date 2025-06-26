"""
test_validators.py

Testy jednostkowe dla funkcji `run_all_validations` z modułu `validation_runner`.

Sprawdzane przypadki:
- poprawna walidacja czystych danych,
- odrzucanie niepoprawnych lub niekompletnych wierszy,
- sprawdzanie poprawności kolumn i wartości null,
- wykrywanie złych zakresów czasu i duplikatów.
"""

import pandas as pd
import pytest
from validation.validation_runner import run_all_validations

def test_validators_pass_on_clean_data():
    """
    Dane spełniające wszystkie kryteria powinny przejść walidację bez zmian.
    """
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
    assert len(validated_df) == 2

def test_validators_remove_invalid_rows():
    """
    Rzędy z błędnymi wartościami (ujemne, zero, złe daty) powinny zostać usunięte.
    """
    df = pd.DataFrame({
        "trip_distance": [1.0, -5.0],
        "fare_amount": [10.0, 0.0],
        "total_amount": [15.0, -1.0],
        "passenger_count": [1, 0],
        "tip_amount": [1.5, -3.0],
        "tpep_pickup_datetime": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 02:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2024-01-01 01:00", "2024-01-01 01:30"])
    })

    validated_df = run_all_validations(df)
    assert len(validated_df) == 1

def test_validator_raises_on_missing_columns():
    """
    Brak wymaganych kolumn powinien skutkować wyjątkiem ValueError.
    """
    df = pd.DataFrame({
        "trip_distance": [1.0],
        "fare_amount": [10.0]
    })

    with pytest.raises(ValueError, match="Brakuje wymaganych kolumn"):
        run_all_validations(df)

def test_validator_removes_rows_with_missing_values():
    """
    Rekordy zawierające NaN powinny zostać odrzucone.
    """
    df = pd.DataFrame({
        "trip_distance": [1.0, None],
        "fare_amount": [10.0, 5.0],
        "total_amount": [15.0, 10.0],
        "passenger_count": [1, 1],
        "tip_amount": [1.5, 2.0],
        "tpep_pickup_datetime": pd.to_datetime(["2024-01-01 00:00", "2024-01-02 00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2024-01-01 01:00", "2024-01-02 01:00"])
    })

    validated_df = run_all_validations(df)
    assert len(validated_df) == 1

def test_trip_duration_filtering():
    """
    Kursy z czasem trwania <= 0 lub > 24h powinny zostać odrzucone.
    """
    df = pd.DataFrame({
        "trip_distance": [1.0, 1.0],
        "fare_amount": [10.0, 10.0],
        "total_amount": [15.0, 15.0],
        "passenger_count": [1, 1],
        "tip_amount": [1.0, 1.0],
        "tpep_pickup_datetime": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2024-01-01 00:00", "2024-01-02 01:00"])  # 0s i 25h
    })

    validated_df = run_all_validations(df)
    assert validated_df.empty

def test_duplicates_are_removed():
    """
    Duplikaty (identyczne wiersze) powinny zostać usunięte.
    """
    df = pd.DataFrame({
        "trip_distance": [1.0, 1.0],
        "fare_amount": [10.0, 10.0],
        "total_amount": [15.0, 15.0],
        "passenger_count": [1, 1],
        "tip_amount": [1.0, 1.0],
        "tpep_pickup_datetime": pd.to_datetime(["2024-01-01 00:00", "2024-01-01 00:00"]),
        "tpep_dropoff_datetime": pd.to_datetime(["2024-01-01 01:00", "2024-01-01 01:00"])
    })

    validated_df = run_all_validations(df)
    assert len(validated_df) == 1
