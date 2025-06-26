"""
test_decorators.py

Test jednostkowy dla dekoratorów `@count_calls` i `@measure_time`.

Sprawdza:
- czy dekorowana funkcja zwraca poprawny wynik,
- czy licznik wywołań działa poprawnie,
- czy na wyjściu pojawiają się komunikaty z dekoratorów (czas i liczba wywołań).
"""

import time
from decorators.counter import count_calls
from decorators.timer import measure_time

# Globalna zmienna do sprawdzenia działania dekoratora count_calls
call_counter = {"count": 0}

@count_calls
@measure_time
def dummy_function():
    """
    Przykładowa funkcja testowa dekorowana przez count_calls i measure_time.
    """
    call_counter["count"] += 1
    time.sleep(0.01)  # sztuczne opóźnienie do pomiaru czasu
    return sum(range(1000))

def test_count_calls_and_timer(capsys):
    """
    Testuje dekoratory count_calls i measure_time na przykładzie dummy_function.
    Sprawdza wynik działania funkcji oraz obecność odpowiednich komunikatów.
    """
    result = dummy_function()
    captured = capsys.readouterr()

    # Czy funkcja coś zwraca?
    assert isinstance(result, int)

    # Czy licznik działa?
    assert call_counter["count"] == 1

    # Czy dekoratory wypisały odpowiednie logi
    assert "[Timer]" in captured.out
    assert "wykonała się w" in captured.out or "ms" in captured.out.lower()

    assert "[Counter]" in captured.out
    assert "wywołana 1 raz" in captured.out
