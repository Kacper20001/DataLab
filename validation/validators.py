import pandas as pd
from validation.base import BaseValidator

class PositivePassengerCountValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["passenger_count"] > 0]

class PositiveDistanceValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["trip_distance"] > 0]

class PositiveFareValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[(df["fare_amount"] >= 0) & (df["total_amount"] >= 0)]

class ValidDateRangeValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df["tpep_dropoff_datetime"] > df["tpep_pickup_datetime"]]

class DropDuplicatesValidator(BaseValidator):
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates()
