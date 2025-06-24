import numpy as np
import multiprocessing as mp
from decorators.counter import count_calls
from decorators.timer import measure_time


@count_calls
@measure_time
def process_chunk(df_chunk, shared_results, lock, idx):
    with lock:
        print(f"[Process-{idx}] Start")

    avg_distance = df_chunk["trip_distance"].mean()
    total_tips = df_chunk["tip_amount"].sum()
    long_trips = (df_chunk["trip_distance"] > 10).sum()

    shared_results[f"process_{idx}"] = {
        "idx": idx,
        "avg_distance": avg_distance,
        "total_tips": total_tips,
        "long_trips": long_trips
    }

    with lock:
        print(f"[Process-{idx}] Done")


@count_calls
@measure_time
def run_multiprocessing(df, num_processes=4):
    df_chunks = np.array_split(df, num_processes)
    manager = mp.Manager()
    shared_results = manager.dict()
    lock = mp.Lock()
    processes = []

    for idx, chunk in enumerate(df_chunks):
        p = mp.Process(target=process_chunk, args=(chunk, shared_results, lock, idx))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    # Konwersja do listy słowników
    return list(shared_results.values())