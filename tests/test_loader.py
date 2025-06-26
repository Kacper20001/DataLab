"""
test_loader.py

Testy jednostkowe dla funkcji `load_parquet_in_chunks` z modułu `core.loader`.

Sprawdzane przypadki:
- poprawne wczytywanie danych z dużego pliku Parquet w chunkach,
- obsługa nieistniejącej ścieżki (zwraca pustą listę zamiast wyjątku),
- poprawne zachowanie przy pustym pliku/parquet mockowanym do pustego DataFrame.
"""

import os
import pandas as pd
from core.loader import load_parquet_in_chunks

def test_load_parquet_in_chunks_reads_data():
    """
    Funkcja powinna poprawnie wczytać dane w chunkach z istniejącego pliku Parquet.
    """
    path = "data/raw/yellow_tripdata_2024-01.parquet"
    assert os.path.exists(path), f"Plik nie istnieje: {path}"

    chunks = list(load_parquet_in_chunks(path, chunksize=100_000))

    assert len(chunks) > 0, "Nie wczytano żadnych chunków"

    for chunk in chunks:
        assert isinstance(chunk, pd.DataFrame)
        assert not chunk.empty
        assert len(chunk) > 0

def test_load_parquet_in_chunks_invalid_path():
    """
    Dla nieistniejącej ścieżki powinien zostać zwrócony pusty wynik (lista).
    """
    invalid_path = "data/raw/fake_file.parquet"
    chunks = list(load_parquet_in_chunks(invalid_path, chunksize=100_000))

    assert chunks == [], "Dla nieistniejącego pliku powinien być pusty wynik"

def test_load_parquet_in_chunks_empty_file(monkeypatch):
    """
    Gdy `read_parquet` zwraca pusty DataFrame (symulacja pustego pliku),
    funkcja powinna zwrócić pustą listę.
    """
    def fake_parquet(*args, **kwargs):
        return pd.DataFrame()

    monkeypatch.setattr(pd, "read_parquet", fake_parquet)

    chunks = list(load_parquet_in_chunks("fake_path.parquet", chunksize=10_000))

    assert chunks == [], "Pusty plik powinien dawać pusty wynik"
