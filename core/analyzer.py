from core.cleaner import stream_and_clean_data
from decorators.timer import measure_time
from decorators.counter import count_calls
import os

@measure_time
@count_calls
def streaming_global_analysis(path: str, chunksize: int = 100_000):
    """
    Strumieniowa analiza danych NYC Yellow Taxi z agregacją globalną.
    """
    total_rows = 0
    total_distance = 0.0
    total_tip = 0.0
    total_amount = 0.0
    total_passengers = 0
    long_trips = 0

    for i, chunk in enumerate(stream_and_clean_data(path, chunksize=chunksize)):
        total_rows += len(chunk)
        total_distance += chunk["trip_distance"].sum()
        total_tip += chunk["tip_amount"].sum()
        total_amount += chunk["total_amount"].sum()
        total_passengers += chunk["passenger_count"].sum()
        long_trips += (chunk["trip_distance"] > 10).sum()

        print(f"[Chunk {i+1}] Wierszy: {len(chunk)}")

    if total_rows == 0:
        print("[Analyzer] Brak danych do analizy.")
        return {}

    average_fare = total_amount / total_rows if total_rows > 0 else 0

    results = {
        "Liczba rekordów": total_rows,
        "Średnia długość trasy (mile)": round(total_distance / total_rows, 2),
        "Średni napiwek ($)": round(total_tip / total_rows, 2),
        "Łączna kwota opłat ($)": round(total_amount, 2),
        "Średnia opłata za kurs ($)": round(average_fare, 2),
        "Liczba pasażerów (łącznie)": int(total_passengers),
        "Średnia liczba pasażerów na kurs": round(total_passengers / total_rows, 2),
        "Liczba długich kursów (>10 mil)": long_trips,
    }

    os.makedirs("data/output", exist_ok=True)
    output_path = "data/output/stream_summary.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        for k, v in results.items():
            f.write(f"{k}: {v}\n")

    print(f"\n[Analyzer] Strumieniowa analiza zakończona. Wyniki zapisane w {output_path}")
    return results
