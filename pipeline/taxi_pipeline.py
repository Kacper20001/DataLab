"""
taxi_pipeline.py

Definicja klasy TaxiPipeline – konkretnej implementacji pipeline’u przetwarzającego
dane NYC Yellow Taxi przy użyciu multiprocessing. Pipeline obejmuje:
- podgląd danych wejściowych,
- analizę z profilowaniem CPU/pamięci,
- generowanie wykresów,
- tworzenie raportów.

Pipeline korzysta z klasy bazowej BasePipeline i metaklasy PipelineMeta.
"""

import logging
import os
import pandas as pd

from core.visualizer import visualize_data, plot_memory_usage
from core.sample_loader import load_sample_for_visualization
from core.profiling.profiler import profile_memory, profile_cpu
from core.pool_processor import parallel_analysis, save_summary_by_vendor, save_anomalies_report
from core.loader import load_parquet_in_chunks
from decorators.counter import count_calls
from decorators.timer import measure_time
from pipeline.base import BasePipeline

def step(func):
    """
    Dekorator oznaczający metodę jako krok pipeline'u.
    Dodaje atrybut `_is_step`, dzięki czemu metaklasa PipelineMeta automatycznie
    rozpozna metodę jako krok do wykonania.
    """
    func._is_step = True
    return func


class TaxiPipeline(BasePipeline):
    """
    Pipeline do przetwarzania danych NYC Yellow Taxi z użyciem multiprocessing.
    Składa się z kroków: podgląd danych, analiza równoległa, wizualizacja, raporty.
    """

    def __init__(self, file_path: str):
        """
        Inicjalizuje pipeline z podaną ścieżką do pliku .parquet.

        Args:
            file_path (str): Ścieżka do danych wejściowych.
        """
        self.file_path = file_path

    @step
    @measure_time
    @count_calls
    def preview_parallel_data(self):
        """
        Wczytuje pierwszy chunk danych i wypisuje jego podgląd w konsoli.
        Służy jako szybka kontrola zawartości danych przed analizą.
        """
        for i, chunk in enumerate(load_parquet_in_chunks(self.file_path, chunksize=100_000)):
            print(f"[Chunk {i + 1}] Preview:")
            print(chunk.head())
            break

    @step
    @measure_time
    @count_calls
    def analyze_parallel(self):
        """
        Wykonuje analizę danych z równoległym przetwarzaniem.
        Profiluje CPU i pamięć, a następnie generuje wykres zużycia pamięci.
        """
        logger = logging.getLogger(__name__)
        logger.info("Profilowanie CPU i pamięci...")

        def analysis_task():
            parallel_analysis(self.file_path)

        # Profilowanie CPU i pamięci w jednej sesji
        profile_cpu(lambda: profile_memory(analysis_task))

        # Wizualizacja zużycia pamięci
        memlog_path = "data/profiling/memory_profile.memlog"
        if os.path.exists(memlog_path):
            plot_memory_usage(memlog_path)
        else:
            logger.warning("Nie znaleziono memory_profile.memlog – pominięto wykres pamięci.")

    @step
    @measure_time
    @count_calls
    def visualize(self):
        """
        Wczytuje próbkę danych (2 chunki), oczyszcza ją i generuje wykresy.
        Służy jako szybka wizualna kontrola jakości i rozkładów danych.
        """
        df_sample = load_sample_for_visualization(
            self.file_path,
            chunksize=100_000,
            max_chunks=2
        )
        visualize_data(df_sample)

    @step
    @measure_time
    @count_calls
    def generate_reports(self):
        """
        Generuje raporty tekstowe:
        - `summary_by_vendor.txt` – statystyki według VendorID,
        - `anomalies_report.txt` – podejrzane napiwki większe niż całkowita kwota.
        """
        df = pd.read_parquet(self.file_path)
        save_summary_by_vendor(df)
        save_anomalies_report(df)

    def run(self):
        """
        Ręczne uruchomienie wszystkich kroków pipeline'u.
        Metoda służy do testów lub wywołania spoza mechanizmu metaklasy.
        """
        self.preview_parallel_data()
        self.analyze_parallel()
        self.visualize()
        self.generate_reports()
