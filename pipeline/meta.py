"""
meta.py

Definicja metaklasy PipelineMeta, służącej do automatycznego rejestrowania kroków
pipeline'u na podstawie metod oznaczonych atrybutem `_is_step = True`.

Metaklasa tworzy listę `_steps`, która zawiera nazwę każdej oznaczonej metody,
a następnie przekazuje ją do klasy bazowej pipeline'u.
"""

class PipelineMeta(type):
    """
    Metaklasa do automatycznego rejestrowania kroków pipeline'u.

    Każda metoda w klasie potomnej oznaczona atrybutem `_is_step = True`
    zostanie automatycznie dodana do listy `_steps`, która definiuje
    kolejność wykonywania kroków w pipeline.
    """

    def __new__(cls, name: str, bases: tuple, dct: dict):
        steps = [key for key, value in dct.items() if hasattr(value, "_is_step")]
        dct["_steps"] = steps
        return super().__new__(cls, name, bases, dct)
