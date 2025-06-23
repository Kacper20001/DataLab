import time
from functools import wraps

def measure_time(func):
    """
    Dekorator mierzący czas wykonania funkcji.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        wynik = func(*args, **kwargs)
        end = time.time()
        print(f"[Pomiar czasu] Funkcja '{func.__name__}' wykonała się w {end - start:.2f} sekundy")
        return wynik
    return wrapper