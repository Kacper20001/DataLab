"""
Moduł `validation_runner` odpowiada za centralne zarządzanie walidacją danych wejściowych.

Zawiera funkcje, które uruchamiają sekwencję walidatorów (implementujących klasę `BaseValidator`)
na danych typu DataFrame, filtrując niepoprawne rekordy i przygotowując dane do dalszego przetwarzania.

Używany m.in. w pipeline, czyszczeniu i testach.
"""

import pandas as pd
from validation.validators import (
    ColumnExistenceValidator,
    NoMissingValuesValidator,
    TripDurationValidator,
    PositivePassengerCountValidator,
    PositiveDistanceValidator,
    PositiveFareValidator,
    PositiveTipValidator,
    ValidDateRangeValidator,
    DropDuplicatesValidator
)

REQUIRED_COLUMNS = [
    "passenger_count", "trip_distance", "tip_amount", "total_amount",
    "fare_amount", "tpep_pickup_datetime", "tpep_dropoff_datetime"
]

def validate_chunk(df: pd.DataFrame) -> pd.DataFrame:
    """
    Przepuszcza dany DataFrame przez zestaw walidatorów.

    Każdy walidator sprawdza określone zasady poprawności:
    - obecność wymaganych kolumn
    - brak wartości NaN
    - dodatnie wartości liczbowe
    - poprawność dat i czasu trwania przejazdu
    - usunięcie duplikatów

    :param df: Surowy DataFrame do walidacji
    :return: Oczyszczony i zweryfikowany DataFrame
    """
    validators = [
        ColumnExistenceValidator(REQUIRED_COLUMNS),
        NoMissingValuesValidator(),
        PositivePassengerCountValidator(),
        PositiveDistanceValidator(),
        PositiveFareValidator(),
        PositiveTipValidator(),
        ValidDateRangeValidator(),
        TripDurationValidator(),
        DropDuplicatesValidator()
    ]

    for validator in validators:
        df = validator.validate(df)

    return df.reset_index(drop=True)

def run_all_validations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Alias dla validate_chunk – stosowany w pipeline i testach.

    :param df: DataFrame do walidacji
    :return: Zweryfikowany i przefiltrowany DataFrame
    """
    return validate_chunk(df)
