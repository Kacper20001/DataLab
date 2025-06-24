import pandas as pd
from validation.validators import (
    PositivePassengerCountValidator,
    PositiveDistanceValidator,
    PositiveFareValidator,
    ValidDateRangeValidator,
    DropDuplicatesValidator
)


def run_all_validations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wykonuje wszystkie walidacje sekwencyjnie.
    """
    validators = [
        PositivePassengerCountValidator(),
        PositiveDistanceValidator(),
        PositiveFareValidator(),
        ValidDateRangeValidator(),
        DropDuplicatesValidator()
    ]

    for validator in validators:
        df = validator.validate(df)

    df.reset_index(drop=True, inplace=True)
    return df
