from multiprocessing import Pool, cpu_count
import pandas as pd
from core.cleaner import clean_data
from core.streamer import stream_parquet
import os


def process_chunk(chunk: pd.DataFrame) -> dict:
    """Przetwarza pojedynczy chunk danych i zwraca statystyki."""
    print(f"[Process] Start")
    cleaned = clean_data(chunk)

    mean_distance = cleaned['trip_distance'].mean()
    total_tip = cleaned['tip_amount'].sum()
    long_trips = (cleaned['trip_distance'] > 10).sum()

    print(f"[Process] Done")
    return {
        'mean_distance': mean_distance,
        'total_tip': total_tip,
        'long_trips': long_trips
    }


def run_multiprocessing(path: str, chunksize: int = 250_000):
    """Uruchamia multiprocessing przy użyciu Pool i zapisuje wynik do pliku."""
    print("[Multiprocessing] Start")

    chunks = list(stream_parquet(path, chunksize=chunksize))
    with Pool(processes=cpu_count()) as pool:
        results = pool.map(process_chunk, chunks)

    global_summary = {
        'mean_distance': sum(r['mean_distance'] for r in results) / len(results),
        'total_tip': sum(r['total_tip'] for r in results),
        'long_trips': sum(r['long_trips'] for r in results)
    }

    print("\n=== Podsumowanie ogólne ===")
    print(f"Średnia odległość (globalnie): {global_summary['mean_distance']:.2f} mil")
    print(f"Łączny napiwek (wszystko): ${global_summary['total_tip']:.2f}")
    print(f"Łączna liczba długich kursów: {global_summary['long_trips']}")

    os.makedirs("data/output", exist_ok=True)
    with open("data/output/pool_summary.txt", "w", encoding="utf-8") as f:
        f.write("=== Podsumowanie ogólne ===\n")
        f.write(f"Średnia odległość (globalnie): {global_summary['mean_distance']:.2f} mil\n")
        f.write(f"Łączny napiwek (wszystko): ${global_summary['total_tip']:.2f}\n")
        f.write(f"Łączna liczba długich kursów: {global_summary['long_trips']}\n")

    print("[Multiprocessing] Wyniki zapisane do: data/output/pool_summary.txt")
