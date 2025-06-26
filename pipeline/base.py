"""
base.py

Moduł definiuje klasę bazową dla pipeline’ów przetwarzania danych.

Klasa BasePipeline wykorzystuje metaklasę PipelineMeta do automatycznej rejestracji metod
pipeline’u (tzw. kroków przetwarzania), które następnie są wykonywane w ustalonej kolejności
przy użyciu metody `run()`.
"""

from pipeline.meta import PipelineMeta


class BasePipeline(metaclass=PipelineMeta):
    """
    Klasa bazowa dla każdego pipeline'u przetwarzającego dane.

    Wykorzystuje metaklasę PipelineMeta do dynamicznej rejestracji kroków.
    Zapewnia metodę `run()`, która wykonuje wszystkie zarejestrowane kroki
    pipeline’u w kolejności ich definicji w klasie dziedziczącej.
    """

    def run(self) -> None:
        """
        Wykonuje wszystkie zarejestrowane kroki pipeline’u.
        Każdy krok to metoda oznaczona przez metaklasę PipelineMeta.
        """
        for step_name in self._steps:
            step = getattr(self, step_name)
            print(f"\n[Pipeline] Wykonuję krok: {step_name}")
            step()
