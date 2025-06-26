from abc import ABC, abstractmethod
import pandas as pd

class BaseValidator(ABC):
    """
    Abstrakcyjna klasa bazowa dla wszystkich walidatorów danych.

    Każda klasa dziedzicząca musi zaimplementować metodę `validate`, która
    przyjmuje DataFrame i zwraca przefiltrowany DataFrame zgodnie z daną regułą.
    """

    @abstractmethod
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Waliduje dane i zwraca DataFrame po filtracji.

        :param df: DataFrame do przefiltrowania
        :return: Przefiltrowany DataFrame
        """
        pass
