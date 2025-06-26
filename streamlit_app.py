"""
Moduł `streamlit_app.py` uruchamia interfejs webowy dla projektu analizy danych NYC Yellow Taxi.

Wykorzystuje bibliotekę Streamlit do wizualizacji wyników przetwarzania danych:
– uruchomienie pipeline'u (`TaxiPipeline`)
– podgląd danych surowych
– prezentacja wykresów i raportów
– przegląd logów

Plik może być uruchamiany samodzielnie jako aplikacja frontendowa.
"""

import streamlit as st
import os
import pandas as pd
from core.logger import logger as app_logger
from pipeline.taxi_pipeline import TaxiPipeline

# Stałe ścieżki i pliki
OUTPUT_DIR = "data/output"
RAW_DATA_PATH = "data/raw/yellow_tripdata_2024-01.parquet"
LOG_FILE_PATH = "data/logs/app.log"

# Wykresy do wyświetlenia
PLOTS = {
    "Liczba pasażerów a długość przejazdu": "passenger_vs_distance_heatmap.png",
    "Napiwki – rozrzut wartości": "tip_amount_boxplot.png",
    "Napiwki vs całkowita cena": "tip_vs_total_scatter.png",
    "Długość przejazdu – histogram": "trip_distance_hist_filtered.png",
    "Napiwki a liczba pasażerów": "tip_by_passenger_count_filtered.png",
    "Zużycie pamięci podczas analizy": "memory_usage_plot.png"
}

# Raporty tekstowe
TEXT_REPORTS = {
    "Podsumowanie analizy równoległej": "parallel_summary.txt",
    "Raport anomalii": "anomalies_report.txt",
    "Raport podsumowujący przewoźników": "summary_by_vendor.txt"
}

def show_image(file_name: str) -> None:
    """
    Wyświetla wykres PNG z katalogu `data/output`.

    :param file_name: Nazwa pliku wykresu (np. "trip_distance_hist.png")
    """
    full_path = os.path.join(OUTPUT_DIR, file_name)
    if os.path.exists(full_path):
        st.image(full_path, use_column_width=True)
    else:
        st.warning(f"Brak wykresu: {file_name}")
        app_logger.warning(f"[Streamlit] Brak pliku graficznego: {full_path}")

def show_text(file_name: str) -> None:
    """
    Wyświetla zawartość pliku tekstowego (np. raportu).

    :param file_name: Nazwa pliku z raportem (np. "summary.txt")
    """
    full_path = os.path.join(OUTPUT_DIR, file_name)
    if os.path.exists(full_path):
        with open(full_path, "r", encoding="utf-8") as f:
            st.text(f.read())
    else:
        st.warning(f"Brak raportu: {file_name}")
        app_logger.warning(f"[Streamlit] Brak pliku tekstowego: {full_path}")

def show_logs() -> None:
    """
    Wyświetla ostatnie 100 linii z logów aplikacji (`app.log`).
    """
    if os.path.exists(LOG_FILE_PATH):
        with open(LOG_FILE_PATH, "r", encoding="utf-8") as log_file:
            logs = log_file.readlines()[-100:]
            st.text("".join(logs))
    else:
        st.error("Brak pliku logów.")
        app_logger.warning("[Streamlit] Brak pliku logów.")

def preview_raw_data() -> None:
    """
    Wczytuje pierwsze 10 rekordów z pliku Parquet i wyświetla jako DataFrame.
    """
    if os.path.exists(RAW_DATA_PATH):
        try:
            df = pd.read_parquet(RAW_DATA_PATH, engine="pyarrow")
            st.dataframe(df.head(10))
        except Exception as e:
            st.error(f"Nie udało się załadować danych: {e}")
            app_logger.exception("[Streamlit] Błąd przy wczytywaniu Parquet")
    else:
        st.error("Brak pliku danych wejściowych.")
        app_logger.warning("[Streamlit] Brak pliku wejściowego: Parquet")

def main() -> None:
    """
    Główna funkcja uruchamiająca interfejs Streamlit.
    Obsługuje zakładki, uruchomienie pipeline'u, ładowanie danych, wykresy i raporty.
    """
    st.set_page_config(layout="wide", page_title="DataLab – NYC Taxi Analysis")
    st.title("DataLab – Interaktywna analiza przejazdów taksówkami w NYC")

    tab = st.sidebar.radio("Co chcesz zobaczyć?", [
        "Wykresy",
        "Raporty tekstowe",
        "Wszystkie wykresy",
        "Podgląd danych",
        "Debug / Logi aplikacji"
    ])

    if st.sidebar.button("Odśwież / Uruchom analizę"):
        st.info("Uruchamiam pipeline analizy danych...")
        try:
            pipeline = TaxiPipeline(file_path=RAW_DATA_PATH)
            pipeline.run()
            st.success("Pipeline zakończony powodzeniem.")
        except Exception as e:
            st.error(f"Wystąpił błąd: {e}")
            app_logger.exception("[Streamlit] Błąd podczas uruchamiania pipeline")

    if tab == "Wykresy":
        st.header("Pojedyncze wizualizacje")
        available_plots = [k for k, v in PLOTS.items() if os.path.exists(os.path.join(OUTPUT_DIR, v))]
        if available_plots:
            selected_plot = st.selectbox("Wybierz wykres:", available_plots)
            st.subheader(selected_plot)
            show_image(PLOTS[selected_plot])
        else:
            st.error("Brak dostępnych wykresów. Uruchom analizę danych.")
        st.divider()

    elif tab == "Raporty tekstowe":
        st.header("Raporty z analizy")
        available_reports = [k for k, v in TEXT_REPORTS.items() if os.path.exists(os.path.join(OUTPUT_DIR, v))]
        if available_reports:
            selected_report = st.selectbox("Wybierz raport:", available_reports)
            st.subheader(selected_report)
            show_text(TEXT_REPORTS[selected_report])
        else:
            st.error("Brak dostępnych raportów. Uruchom analizę.")
        st.divider()

    elif tab == "Wszystkie wykresy":
        st.header("Dashboard – Wszystkie dostępne wykresy")
        for name, filename in PLOTS.items():
            full_path = os.path.join(OUTPUT_DIR, filename)
            if os.path.exists(full_path):
                st.subheader(name)
                show_image(filename)
            else:
                st.info(f"Pominięto: {name} (brak pliku)")
        st.divider()

    elif tab == "Podgląd danych":
        st.header("Podgląd danych źródłowych")
        preview_raw_data()
        st.divider()

    elif tab == "Debug / Logi aplikacji":
        st.header("Logi aplikacji")
        show_logs()
        st.divider()

    st.caption("Dane: NYC Yellow Taxi | Projekt: DataLab")

if __name__ == "__main__":
    main()
