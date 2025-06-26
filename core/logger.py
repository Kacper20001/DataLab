"""
logger.py

Konfiguruje logger aplikacyjny zapisywany do pliku i wyświetlany na konsoli.

Domyślnie logi trafiają do 'data/logs/app.log'.
Logger tworzony funkcją setup_logger może być importowany w całym projekcie.
"""

import logging
import os

def setup_logger(name: str, log_file: str = "data/logs/app.log", level=logging.INFO) -> logging.Logger:
    """
    Tworzy i konfiguruje logger z obsługą zapisu do pliku oraz wyjścia na konsolę.

    Args:
        name (str): Nazwa loggera (np. 'DataLab').
        log_file (str): Ścieżka do pliku logów.
        level (int): Poziom logowania (np. logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: Skonfigurowany logger.
    """
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    log = logging.getLogger(name)
    log.setLevel(level)
    log.propagate = False  # Zapobiega podwójnemu logowaniu

    if not log.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(level)

        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(level)

        formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s', "%Y-%m-%d %H:%M:%S")
        ch.setFormatter(formatter)
        fh.setFormatter(formatter)

        log.addHandler(ch)
        log.addHandler(fh)

    return log

# Główny logger aplikacji
logger = setup_logger("DataLab")
