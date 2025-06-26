"""
Moduł `main.py` to punkt startowy aplikacji DataLab.

Sprawdza obecność wymaganych plików wyjściowych (wykresy, raporty, pliki profilujące).
Jeśli ich brakuje, uruchamia `TaxiPipeline` do przetwarzania danych.
Następnie odpala interfejs Streamlit (`streamlit_app.py`).
"""

import os
import subprocess
from pipeline.taxi_pipeline import TaxiPipeline
from core.logger import logger

# Ścieżki do plików i folderów
RAW_DATA_PATH = "data/raw/yellow_tripdata_2024-01.parquet"
OUTPUT_DIR = "data/output"
PROFILING_DIR = "data/profiling"

# Lista plików, które muszą istnieć po przetworzeniu
NEEDED_FILES = [
    # Wykresy
    "passenger_vs_distance_heatmap.png",
    "tip_amount_boxplot.png",
    "tip_vs_total_scatter.png",
    "trip_distance_hist_filtered.png",
    "tip_by_passenger_count_filtered.png",
    "memory_usage_plot.png",

    # Raporty tekstowe
    "parallel_summary.txt",
    "vendor_analysis.txt",
    "anomalies_report.txt",

    # Profilowanie CPU i RAM
    "cpu_profile.prof",
    "memory_profile.memlog"
]

def is_output_complete() -> bool:
    """
    Sprawdza, czy wszystkie wymagane pliki wyjściowe już istnieją.

    :return: True jeśli wszystkie pliki obecne, False w przeciwnym razie.
    """
    return all(
        os.path.exists(
            os.path.join(OUTPUT_DIR if f.endswith((".png", ".txt")) else PROFILING_DIR, f)
        ) for f in NEEDED_FILES
    )

def generate_outputs() -> None:
    """
    Uruchamia pipeline `TaxiPipeline`, jeśli plik źródłowy istnieje.
    Generuje wykresy, raporty oraz pliki profilowania.
    """
    if not os.path.exists(RAW_DATA_PATH):
        logger.error(f"Nie znaleziono pliku danych: {RAW_DATA_PATH}")
        return

    logger.info("Uruchamiam pipeline...")
    pipeline = TaxiPipeline(RAW_DATA_PATH)
    pipeline.run()
    logger.info("Pipeline zakończony pomyślnie.")

def main() -> None:
    """
    Główna funkcja:
    – sprawdza, czy dane wyjściowe już istnieją,
    – w razie potrzeby uruchamia pipeline,
    – odpala aplikację Streamlit.
    """
    logger.info("Uruchamianie aplikacji DataLab")

    if not is_output_complete():
        logger.warning("Brakuje plików wyjściowych – wykonuję pipeline.")
        generate_outputs()
    else:
        logger.info("Pliki już istnieją – pomijam pipeline.")

    logger.info("Odpalam Streamlit...")
    subprocess.run(["streamlit", "run", "streamlit_app.py"], check=True)

if __name__ == "__main__":
    main()
