"""
counter.py

Dekorator `count_calls` zlicza i wypisuje do konsoli liczbę wywołań danej funkcji.
Przydatny do monitorowania działania funkcji w czasie rzeczywistego działania programu.
"""

from functools import wraps

def count_calls(func):
    """
    Dekorator zliczający liczbę wywołań funkcji.
    Przy każdym wywołaniu wypisuje liczbę dotychczasowych uruchomień.

    Args:
        func (Callable): Funkcja, którą dekorujemy.

    Returns:
        Callable: Owinięta funkcja z licznikiem wywołań.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        wrapper.calls += 1
        print(f"[Counter] Funkcja '{func.__name__}' wywołana {wrapper.calls} raz(y)")
        return func(*args, **kwargs)

    wrapper.calls = 0
    return wrapper
