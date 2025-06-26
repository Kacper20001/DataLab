"""
analyzer.py

Moduł odpowiedzialny za analizę danych z pliku .parquet w dwóch trybach:
- parallel_analysis: analiza z użyciem multiprocessing.Pool
- streaming_global_analysis: analiza sekwencyjna chunków + walidacja

Funkcje obliczają sumaryczne metryki (dystans, napiwki, pasażerowie itp.)
i zapisują podsumowanie do pliku tekstowego. Obsługuje błędy, loguje zdarzenia
i wykorzystuje dekoratory pomiaru czasu i zliczania wywołań.
"""

import os
import logging
import pandas as pd
from multiprocessing import Pool, cpu_count
from decorators.timer import measure_time
from decorators.counter import count_calls
from core.loader import load_parquet_in_chunks
from validation.validation_runner import run_all_validations

logger = logging.getLogger(__name__)


def analyze_chunk(df: pd.DataFrame) -> dict:
    """
    Analizuje pojedynczy fragment danych (chunk) i zwraca metryki.

    Args:
        df (pd.DataFrame): Fragment danych do analizy.

    Returns:
        dict: Słownik z wynikami (liczba wierszy, sumy wartości, liczba długich kursów).
    """
    try:
        return {
            "rows": len(df),
            "distance": df["trip_distance"].sum(),
            "tip": df["tip_amount"].sum(),
            "amount": df["total_amount"].sum(),
            "passengers": df["passenger_count"].sum(),
            "long_trips": (df["trip_distance"] > 10).sum()
        }
    except Exception as e:
        logger.exception("Błąd podczas analizy chunku: %s", e)
        return {
            "rows": 0, "distance": 0.0, "tip": 0.0, "amount": 0.0,
            "passengers": 0, "long_trips": 0
        }


@measure_time
@count_calls
def parallel_analysis(path: str, chunksize: int = 100_000) -> dict:
    """
    Wykonuje równoległą analizę danych z pliku .parquet z użyciem multiprocessing.Pool.

    Args:
        path (str): Ścieżka do pliku .parquet.
        chunksize (int): Liczba wierszy na chunk.

    Returns:
        dict: Podsumowanie analizowanych danych (zapisane też do pliku).
    """
    os.makedirs("data/output", exist_ok=True)
    total = {
        "rows": 0, "distance": 0.0, "tip": 0.0,
        "amount": 0.0, "passengers": 0, "long_trips": 0
    }

    try:
        with Pool(cpu_count()) as pool:
            results = pool.map(analyze_chunk, load_parquet_in_chunks(path, chunksize))

        for result in results:
            for key in total:
                total[key] += result[key]

        average_fare = total["amount"] / total["rows"] if total["rows"] > 0 else 0

        summary = {
            "Liczba rekordów": total["rows"],
            "Średnia długość trasy (mile)": round(total["distance"] / total["rows"], 2),
            "Średni napiwek ($)": round(total["tip"] / total["rows"], 2),
            "Łączna kwota opłat ($)": round(total["amount"], 2),
            "Średnia opłata za kurs ($)": round(average_fare, 2),
            "Liczba pasażerów (łącznie)": int(total["passengers"]),
            "Średnia liczba pasażerów na kurs": round(total["passengers"] / total["rows"], 2),
            "Liczba długich kursów (>10 mil)": total["long_trips"]
        }

        _save_summary(summary, "data/output/parallel_summary.txt")
        logger.info("Analiza równoległa zakończona. Wynik zapisany.")
        return summary

    except Exception as e:
        logger.exception("Błąd podczas analizy równoległej: %s", e)
        return {}


@measure_time
@count_calls
def streaming_global_analysis(path: str, chunksize: int = 100_000) -> dict:
    """
    Wykonuje analizę danych chunk po chunku z walidacją, bez multiprocessing.

    Args:
        path (str): Ścieżka do pliku .parquet.
        chunksize (int): Liczba wierszy na chunk.

    Returns:
        dict: Podsumowanie analizowanych danych (zapisane też do pliku).
    """
    os.makedirs("data/output", exist_ok=True)
    total = {
        "rows": 0, "distance": 0.0, "tip": 0.0,
        "amount": 0.0, "passengers": 0, "long_trips": 0
    }

    try:
        for chunk in load_parquet_in_chunks(path, chunksize):
            chunk = run_all_validations(chunk)

            result = analyze_chunk(chunk)
            for key in total:
                total[key] += result[key]

        average_fare = total["amount"] / total["rows"] if total["rows"] > 0 else 0

        summary = {
            "Liczba rekordów": total["rows"],
            "Średnia długość trasy (mile)": round(total["distance"] / total["rows"], 2),
            "Średni napiwek ($)": round(total["tip"] / total["rows"], 2),
            "Łączna kwota opłat ($)": round(total["amount"], 2),
            "Średnia opłata za kurs ($)": round(average_fare, 2),
            "Liczba pasażerów (łącznie)": int(total["passengers"]),
            "Średnia liczba pasażerów na kurs": round(total["passengers"] / total["rows"], 2),
            "Liczba długich kursów (>10 mil)": total["long_trips"]
        }

        _save_summary(summary, "data/output/streaming_summary.txt")
        logger.info("Analiza streamingowa zakończona. Wynik zapisany.")
        return summary

    except Exception as e:
        logger.exception("Błąd podczas analizy streamingowej: %s", e)
        return {}


def _save_summary(summary: dict, path: str) -> None:
    """
    Zapisuje słownik wyników analizy do pliku tekstowego.

    Args:
        summary (dict): Dane do zapisania.
        path (str): Ścieżka zapisu.
    """
    try:
        with open(path, "w", encoding="utf-8") as f:
            for k, v in summary.items():
                f.write(f"{k}: {v}\n")
        logger.debug(f"Wynik analizy zapisany do {path}")
    except Exception as e:
        logger.exception("Nie udało się zapisać pliku z wynikami: %s", e)
