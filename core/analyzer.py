from decorators.counter import count_calls
from decorators.timer import measure_time

@count_calls
@measure_time
def analyze_data(df):
    """
    Analizuje podstawowe statystyki z danych NYC Yellow Taxi.
    """
    results = {
        "Liczba rekordów": len(df),
        "Średnia długość trasy (mile)": round(df["trip_distance"].mean(), 2),
        "Średni napiwek ($)": round(df["tip_amount"].mean(), 2),
        "Łączna kwota opłat ($)": round(df["total_amount"].sum(), 2),
        "Liczba pasażerów (łącznie)": int(df["passenger_count"].sum()),
        "Średnia liczba pasażerów na kurs": round(df["passenger_count"].mean(), 2),
    }

    output_path = "data/output/summary.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        for k, v in results.items():
            f.write(f"{k}: {v}\n")

    print("\n[Analyzer] Wyniki analizy zapisane do:", output_path)