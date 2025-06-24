import pandas as pd
from validation.base import BaseValidator

class PositivePassengerCountValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        # Akceptujemy tylko rekordy z pasażerami > 0
        return df[df["passenger_count"] > 0]

class PositiveDistanceValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        # Dystans musi być dodatni
        return df[df["trip_distance"] > 0]

class PositiveFareValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        # fare_amount i total_amount muszą być > 0, nie >= 0 (bo 0 to śmieć)
        return df[(df["fare_amount"] > 0) & (df["total_amount"] > 0)]

class ValidDateRangeValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        # dropoff musi być po pickup — logiczne
        return df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

class DropDuplicatesValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        # Czyścimy duplikaty (dokładne kopie wierszy)
        return df.drop_duplicates()
