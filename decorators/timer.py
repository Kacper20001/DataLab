"""
timer.py

Dekorator `measure_time` służy do mierzenia czasu wykonania funkcji.
Czas wykonywania jest wypisywany na konsolę w milisekundach (ms).
"""

import time
from functools import wraps

def measure_time(func):
    """
    Dekorator mierzący czas wykonania funkcji i wypisujący go w ms.

    Args:
        func (Callable): Funkcja, którą chcemy profilować czasowo.

    Returns:
        Callable: Funkcja opakowana pomiarem czasu.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = (time.time() - start) * 1000
        print(f"[Timer] Funkcja '{func.__name__}' wykonała się w {duration:.2f} ms")
        return result

    return wrapper
