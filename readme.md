# ️ DataLab – NYC Yellow Taxi Data Processing & Visualization

**DataLab** to zaawansowana aplikacja do analizy danych przejazdów **NYC Yellow Taxi**, zaprojektowana w języku Python. Projekt umożliwia przetwarzanie bardzo dużych zbiorów danych (powyżej 2.9 mln rekordów) z wykorzystaniem **równoległego przetwarzania** (`multiprocessing.Pool`), walidacji i czyszczenia danych, profilowania wydajności oraz dynamicznej wizualizacji wyników w aplikacji webowej zbudowanej przy użyciu **Streamlit**.

## Funkcjonalności

- Wczytywanie danych z pliku `.parquet` chunkami (np. po 100k rekordów)
- Równoległe przetwarzanie danych z wykorzystaniem wielu rdzeni CPU
- Czyszczenie danych i walidacja: filtrowanie błędnych rekordów, braków, wartości ujemnych i nielogicznych
- Generowanie raportów tekstowych:
  - `parallel_summary.txt` – podsumowanie statystyczne
  - `anomalies_report.txt` – wykryte anomalie (np. napiwki większe niż całkowity koszt)
  - `summary_by_vendor.txt` – statystyki według firm taksówkowych (VendorID)
- Wizualizacje danych:
  - histogramy długości tras
  - scatterploty (np. napiwek vs koszt)
  - boxploty napiwków
  - heatmapy (np. liczba pasażerów vs dystans)
  - wykresy zużycia pamięci
- Interaktywny interfejs użytkownika: przegląd raportów, wykresów, danych i logów

## Technologie i narzędzia

- Python 3.11+
- Pandas, PyArrow – analiza danych i obsługa `.parquet`
- Streamlit – interfejs użytkownika
- Multiprocessing – równoległe przetwarzanie danych
- Matplotlib, Seaborn – wizualizacja danych
- Logging – rejestrowanie działania systemu
- memory_profiler, cProfile – profilowanie pamięci i CPU
- Pytest – testy jednostkowe
- Własne dekoratory: `@measure_time`, `@count_calls`

## Struktura projektu

```
DataLab/
├── core/                # Moduły logiki aplikacji: loading, cleaning, analysis, visualizacja
├── decorators/          # Dekoratory pomiaru czasu i liczby wywołań
├── pipeline/            # Pipeline główny oraz metaklasy
├── validation/          # System walidatorów danych
├── data/                # Dane surowe, wyniki, wykresy i logi
├── tests/               # Testy jednostkowe
├── main.py              # Główne wejście do aplikacji (pipeline + UI)
├── streamlit_app.py     # Interfejs aplikacji (Streamlit)
└── requirements.txt     # Lista zależności
```

## Instrukcja uruchomienia

1. Klonuj repozytorium:

```bash
git clone https://github.com/Kacper20001/DataLab.git
cd DataLab
```

2. Utwórz środowisko wirtualne i aktywuj:

```bash
python -m venv venv
source venv/bin/activate  # Windows: .\venv\Scripts\activate
```

3. Zainstaluj zależności:

```bash
pip install -r requirements.txt
```

4. Umieść plik danych:

W katalogu `data/raw/` umieść plik:

```
yellow_tripdata_2024-01.parquet
```

5. Uruchom aplikację:

```bash
python main.py
```

Aplikacja Streamlit otworzy się pod adresem:

```
http://localhost:8501
```

## Testy

Aby uruchomić testy jednostkowe (dla walidatorów, loadera, cleanera itp.), wykonaj:

```bash
pytest
```

## Autor

**Kacper Kulig**  
Projekt realizowany w ramach pracy inżynierskiej – temat: *„Przetwarzanie dużych zbiorów danych z użyciem multiprocessing.Pool”*  
Repozytorium: [https://github.com/Kacper20001/DataLab](https://github.com/Kacper20001/DataLab)