"""
profiler.py

Moduł odpowiedzialny za profilowanie wydajności kodu.

Zawiera dwie funkcje:
- profile_cpu: tworzy profil CPU działania przekazanej funkcji (z użyciem cProfile)
- profile_memory: mierzy użycie pamięci funkcji (z użyciem memory_profiler)

Wyniki profilowania są zapisywane do katalogu 'data/profiling'.
"""

import os
import cProfile
import logging
from memory_profiler import memory_usage

logger = logging.getLogger(__name__)

def profile_cpu(func, filename="cpu_profile.prof"):
    """
    Profiluje zużycie CPU przez przekazaną funkcję i zapisuje wynik do pliku .prof.

    Args:
        func (Callable): Funkcja do profilowania.
        filename (str): Nazwa pliku wynikowego (.prof) zapisywanego w 'data/profiling'.

    Raises:
        Exception: Błąd podczas profilowania — logowany i przekazywany dalej.
    """
    os.makedirs("data/profiling", exist_ok=True)
    output_path = os.path.join("data", "profiling", filename)

    logger.info("Rozpoczynam profilowanie CPU.")
    profiler = cProfile.Profile()

    try:
        profiler.enable()
        func()
        profiler.disable()
        profiler.dump_stats(output_path)
        logger.info(f"Profil CPU zapisany do: {output_path}")
    except Exception as e:
        logger.exception("Błąd podczas profilowania CPU:")
        raise e


def profile_memory(func, filename="memory_profile.memlog"):
    """
    Profiluje zużycie pamięci przez przekazaną funkcję i zapisuje dane do pliku tekstowego.

    Args:
        func (Callable): Funkcja do profilowania.
        filename (str): Nazwa pliku wynikowego (.memlog) zapisywanego w 'data/profiling'.

    Raises:
        Exception: Błąd podczas profilowania — logowany i przekazywany dalej.
    """
    os.makedirs("data/profiling", exist_ok=True)
    output_path = os.path.join("data", "profiling", filename)

    logger.info("Rozpoczynam profilowanie pamięci.")
    try:
        mem_usage = memory_usage(func, interval=0.1, timeout=None)
        with open(output_path, "w") as f:
            for usage in mem_usage:
                f.write(f"{usage}\n")
        logger.info(f"Profil pamięci zapisany do: {output_path}")
    except Exception as e:
        logger.exception("Błąd podczas profilowania pamięci:")
        raise e
