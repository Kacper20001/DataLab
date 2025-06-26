"""
Moduł `validators.py` zawiera implementacje konkretnych walidatorów dziedziczących po `BaseValidator`.

Każdy walidator realizuje jedną konkretną zasadę filtracji danych, np.:
- dodatnie wartości liczbowe,
- brak duplikatów,
- poprawność czasów rozpoczęcia i zakończenia kursu,
- brak brakujących danych.

Walidatory mogą być stosowane niezależnie lub jako sekwencja w `validation_runner.py`.
"""

import pandas as pd
from validation.base import BaseValidator

class PositivePassengerCountValidator(BaseValidator):
    """
    Przepuszcza tylko rekordy z liczbą pasażerów > 0.
    """
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["passenger_count"] > 0]

class PositiveDistanceValidator(BaseValidator):
    """
    Filtruje rekordy z dodatnią długością trasy (> 0 mil).
    """
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["trip_distance"] > 0]

class PositiveFareValidator(BaseValidator):
    """
    Akceptuje tylko rekordy, gdzie fare_amount i total_amount są dodatnie (> 0).
    """
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[(df["fare_amount"] > 0) & (df["total_amount"] > 0)]

class ValidDateRangeValidator(BaseValidator):
    """
    Usuwa rekordy, gdzie data zakończenia kursu jest wcześniejsza niż data rozpoczęcia.
    """
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

class DropDuplicatesValidator(BaseValidator):
    """
    Usuwa zduplikowane wiersze w DataFrame.
    """
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates()

class PositiveTipValidator(BaseValidator):
    """
    Usuwa rekordy z ujemną wartością napiwku (tip_amount >= 0).
    """
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["tip_amount"] >= 0]

class ColumnExistenceValidator(BaseValidator):
    """
    Sprawdza, czy wszystkie wymagane kolumny istnieją w DataFrame.

    :param required_columns: Lista wymaganych nazw kolumn
    :raises ValueError: Gdy brakuje którejkolwiek kolumny
    """
    def __init__(self, required_columns: list[str]):
        self.required_columns = required_columns

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        missing = [col for col in self.required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Brakuje wymaganych kolumn: {missing}")
        return df

class NoMissingValuesValidator(BaseValidator):
    """
    Usuwa rekordy zawierające jakiekolwiek wartości NaN.
    """
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna()

class TripDurationValidator(BaseValidator):
    """
    Filtruje rekordy, gdzie czas trwania przejazdu jest ≤ 0 lub > 24h.

    Zakładamy, że kurs nie powinien trwać dłużej niż 86400 sekund (24 godziny).
    """
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        duration = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
        return df[(duration > 0) & (duration < 86400)]
