"""
test_pool_processor.py

Testy jednostkowe dla funkcji `parallel_analysis` z modułu `core.pool_processor`.

Sprawdzane przypadki:
- poprawna analiza pliku `.parquet` z danymi (czy generuje wynik i pliki wyjściowe),
- obsługa błędnej/niewłaściwej ścieżki (czy zwraca pusty słownik).
"""

import os
from core.pool_processor import parallel_analysis

def test_parallel_analysis_runs():
    """
    Testuje pełną analizę równoległą na istniejącym pliku danych.
    Sprawdza strukturę słownika wynikowego oraz istnienie plików wynikowych.
    """
    path = "data/raw/yellow_tripdata_2024-01.parquet"
    assert os.path.exists(path), f"Plik nie istnieje: {path}"

    result = parallel_analysis(path, chunksize=100_000)

    assert isinstance(result, dict), "Wynik powinien być słownikiem"

    expected_keys = [
        "Liczba rekordów",
        "Średnia długość trasy (mile)",
        "Średni napiwek ($)",
        "Łączna kwota opłat ($)",
        "Średnia opłata za kurs ($)",
        "Liczba pasażerów (łącznie)",
        "Średnia liczba pasażerów na kurs",
        "Liczba długich kursów (>10 mil)"
    ]

    for key in expected_keys:
        assert key in result, f"Brakuje klucza: {key} w wyniku analizy"

    assert result["Liczba rekordów"] > 0
    assert result["Liczba pasażerów (łącznie)"] > 0
    assert result["Łączna kwota opłat ($)"] > 0.0

    # Sprawdź czy pliki wynikowe się utworzyły
    assert os.path.exists("data/output/parallel_summary.txt"), "Brak pliku podsumowania"
    assert os.path.exists("data/output/summary_by_vendor.txt"), "Brak pliku per VendorID"
    assert os.path.exists("data/output/anomalies_report.txt"), "Brak pliku z anomaliami"

def test_parallel_analysis_invalid_path():
    """
    Dla nieistniejącej ścieżki funkcja powinna zwrócić pusty słownik.
    """
    path = "data/raw/nonexistent_file.parquet"
    result = parallel_analysis(path, chunksize=100_000)

    assert result == {}, "Dla nieistniejącego pliku wynik powinien być pustym słownikiem"
