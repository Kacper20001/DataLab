from core.analyzer import streaming_global_analysis

def test_streaming_global_analysis_runs():
    result = streaming_global_analysis("data/raw/yellow_tripdata_2024-01.parquet")

    # Sprawdzamy, czy wynik jest słownikiem
    assert isinstance(result, dict)

    # Sprawdzamy, czy zawiera oczekiwane klucze
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

    # Opcjonalnie: sprawdzenie czy wartości są logiczne
    assert result["Liczba rekordów"] > 0
    assert result["Liczba pasażerów (łącznie)"] > 0
    assert result["Łączna kwota opłat ($)"] > 0.0
