from abc import ABC, abstractmethod
import pandas as pd

class BaseValidator(ABC):
    """
    Bazowa klasa dla wszystkich walidatorów.
    """
    @abstractmethod
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Waliduje dane i zwraca DataFrame po filtracji.
        """
        pass
