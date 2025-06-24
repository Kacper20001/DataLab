from core.loader import load_parquet
from core.cleaner import clean_data
from core.visualizer import visualize_data
from processing.multiprocessor import run_multiprocessing
from decorators.counter import count_calls
from decorators.timer import measure_time
from pipeline.base import BasePipeline
import os
from core.analyzer import streaming_global_analysis



def step(func):
    func._is_step = True
    return func


class TaxiPipeline(BasePipeline):
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None

    @step
    @measure_time
    @count_calls
    def load(self):
        self.df = load_parquet(self.file_path)

    @step
    @measure_time
    @count_calls
    def clean(self):
        self.df = clean_data(self.df)

    @step
    @measure_time
    @count_calls
    def streaming_analyze(self):
        streaming_global_analysis(self.file_path)

    @step
    @measure_time
    @count_calls
    def visualize(self):
        visualize_data(self.df)

    @step
    @measure_time
    @count_calls
    def parallel_analysis(self):
        results = run_multiprocessing(self.df)

        os.makedirs("data/output", exist_ok=True)
        summary_path = "data/output/multiprocessing_summary.txt"

        total_tips = sum(r["total_tips"] for r in results)
        total_long_trips = sum(r["long_trips"] for r in results)
        avg_distance_overall = sum(r["avg_distance"] for r in results) / len(results)

        with open(summary_path, "w") as f:
            for r in sorted(results, key=lambda x: x['idx']):
                output = (
                    f"Proces {r['idx']}:\n"
                    f"  Średnia odległość: {r['avg_distance']:.2f} mil\n"
                    f"  Łączny napiwek: ${r['total_tips']:.2f}\n"
                    f"  Liczba długich kursów (>10 mil): {r['long_trips']}\n\n"
                )
                print(output, end='')  # wypisujemy na konsolę
                f.write(output)

            summary = (
                "=== Podsumowanie ogólne ===\n"
                f"Średnia odległość (globalnie): {avg_distance_overall:.2f} mil\n"
                f"Łączny napiwek (wszystko): ${total_tips:.2f}\n"
                f"Łączna liczba długich kursów: {total_long_trips}\n"
            )
            print(summary)
            f.write(summary)

        print(f"[Multiprocessing] Wyniki zapisane do: {summary_path}")
