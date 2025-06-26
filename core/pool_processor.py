"""
pool_processor.py

Moduł wykonujący równoległą analizę danych z pliku .parquet z wykorzystaniem multiprocessing.Pool.

Zawiera funkcje:
- analyze_chunk: analizuje pojedynczy fragment danych (sumy, długie trasy itp.)
- aggregate_results: sumuje wyniki z chunków
- save_summary_by_vendor: tworzy raport per VendorID
- save_anomalies_report: wykrywa podejrzane rekordy (tip > total)
- parallel_analysis: główna funkcja analizy równoległej

Zapisuje wszystkie raporty do katalogu 'data/output'.
"""

import os
import pandas as pd
from multiprocessing import Pool, cpu_count
from decorators.timer import measure_time
from decorators.counter import count_calls
from core.loader import load_parquet_in_chunks
from core.logger import logger

REQUIRED_COLUMNS = [
    "passenger_count", "trip_distance", "tip_amount", "total_amount", "VendorID"
]


def analyze_chunk(df: pd.DataFrame) -> dict:
    """
    Analizuje chunk danych, obliczając sumaryczne metryki.

    Args:
        df (pd.DataFrame): Fragment danych.

    Returns:
        dict: Wyniki analizy (lub zera w razie błędu).
    """
    try:
        if not all(col in df.columns for col in REQUIRED_COLUMNS):
            missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
            raise ValueError(f"Brakuje kolumn: {missing}")

        return {
            "rows": len(df),
            "distance": df["trip_distance"].sum(),
            "tip": df["tip_amount"].sum(),
            "amount": df["total_amount"].sum(),
            "passengers": df["passenger_count"].sum(),
            "long_trips": (df["trip_distance"] > 10).sum()
        }

    except Exception as e:
        logger.warning(f"Błąd w analyze_chunk: {e}")
        return {
            "rows": 0, "distance": 0.0, "tip": 0.0,
            "amount": 0.0, "passengers": 0, "long_trips": 0
        }


def aggregate_results(results: list[dict]) -> dict:
    """
    Sumuje dane ze wszystkich chunków.

    Args:
        results (list[dict]): Lista wyników z analyze_chunk.

    Returns:
        dict: Podsumowanie statystyk globalnych.
    """
    total = {
        "rows": 0, "distance": 0.0, "tip": 0.0,
        "amount": 0.0, "passengers": 0, "long_trips": 0
    }

    for result in results:
        for key in total:
            total[key] += result[key]

    rows = total["rows"]
    return {
        "Liczba rekordów": rows,
        "Średnia długość trasy (mile)": round(total["distance"] / rows, 2) if rows else 0,
        "Średni napiwek ($)": round(total["tip"] / rows, 2) if rows else 0,
        "Łączna kwota opłat ($)": round(total["amount"], 2),
        "Średnia opłata za kurs ($)": round(total["amount"] / rows, 2) if rows else 0,
        "Liczba pasażerów (łącznie)": int(total["passengers"]),
        "Średnia liczba pasażerów na kurs": round(total["passengers"] / rows, 2) if rows else 0,
        "Liczba długich kursów (>10 mil)": total["long_trips"]
    }


def save_summary_by_vendor(df: pd.DataFrame, output_path="data/output/summary_by_vendor.txt"):
    """
    Tworzy raport średnich i maksymalnych wartości dla każdego VendorID.

    Args:
        df (pd.DataFrame): Dane wejściowe.
        output_path (str): Ścieżka zapisu pliku.
    """
    if 'VendorID' not in df.columns:
        logger.warning("Brak kolumny 'VendorID', pominięto raport per VendorID.")
        return

    grouped = df.groupby("VendorID").agg({
        "fare_amount": ["mean", "max"],
        "tip_amount": ["mean", "max"],
        "trip_distance": ["mean", "max"]
    })

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("Podsumowanie per VendorID:\n")
        f.write(grouped.to_string())
        f.write("\n")


def save_anomalies_report(df: pd.DataFrame, output_path="data/output/anomalies_report.txt"):
    """
    Wyszukuje i zapisuje rekordy, w których tip_amount > total_amount.

    Args:
        df (pd.DataFrame): Dane wejściowe.
        output_path (str): Ścieżka zapisu raportu.
    """
    if not all(col in df.columns for col in ["tip_amount", "total_amount"]):
        logger.warning("Brakuje kolumn tip_amount lub total_amount, pominięto raport anomalii.")
        return

    anomalies = df[df["tip_amount"] > df["total_amount"]]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("Anomalie: tip_amount > total_amount\n")
        f.write(f"Liczba podejrzanych rekordów: {len(anomalies)}\n\n")
        if not anomalies.empty:
            f.write(anomalies[["VendorID", "fare_amount", "tip_amount", "total_amount"]].head(10).to_string())


@measure_time
@count_calls
def parallel_analysis(path: str, chunksize: int = 100_000) -> dict:
    """
    Główna funkcja analizy danych z wykorzystaniem multiprocessing.

    Args:
        path (str): Ścieżka do pliku .parquet.
        chunksize (int): Liczba wierszy na chunk.

    Returns:
        dict: Podsumowanie wyników analizy (lub pusty słownik przy błędzie).
    """
    os.makedirs("data/output", exist_ok=True)
    logger.info(f"Start analizy równoległej ({cpu_count()} CPU)...")

    try:
        chunks = list(load_parquet_in_chunks(path, chunksize))
        logger.info(f"Załadowano {len(chunks)} chunków.")

        with Pool(cpu_count()) as pool:
            results = pool.map(analyze_chunk, chunks)

        summary = aggregate_results(results)

        with open("data/output/parallel_summary.txt", "w", encoding="utf-8") as f:
            for k, v in summary.items():
                f.write(f"{k}: {v}\n")

        # Łączenie wszystkich chunków do raportów szczegółowych
        full_df = pd.concat(chunks, ignore_index=True)

        save_summary_by_vendor(full_df)
        save_anomalies_report(full_df)

        logger.info("Analiza zakończona sukcesem. Raporty zapisane.")
        return summary

    except Exception as e:
        logger.error(f"Błąd podczas analizy multiprocessing: {e}")
        return {}
