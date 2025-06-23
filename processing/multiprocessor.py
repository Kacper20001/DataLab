import numpy as np
import multiprocessing as mp
from decorators.counter import count_calls
from decorators.timer import measure_time


@count_calls
@measure_time
def process_chunk(df_chunk, result_queue, lock, idx):
    with lock:
        print(f"[Process-{idx}] Start")

    avg_distance = df_chunk["trip_distance"].mean()
    total_tips = df_chunk["tip_amount"].sum()
    long_trips = (df_chunk["trip_distance"] > 10).sum()

    result = {
        "idx": idx,
        "avg_distance": avg_distance,
        "total_tips": total_tips,
        "long_trips": long_trips
    }

    result_queue.put(result)

    with lock:
        print(f"[Process-{idx}] Done")


@count_calls
@measure_time
def run_multiprocessing(df, num_processes=4):
    df_chunks = np.array_split(df, num_processes)
    result_queue = mp.Queue()
    lock = mp.Lock()
    processes = []

    for idx, chunk in enumerate(df_chunks):
        p = mp.Process(target=process_chunk, args=(chunk, result_queue, lock, idx))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    return results
