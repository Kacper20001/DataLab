from core.visualizer import visualize_data
from core.analyzer import streaming_global_analysis
from core.parallel import run_multiprocessing
from core.cleaner import stream_and_clean_data
from core.sample_loader import load_sample_for_visualization
from decorators.counter import count_calls
from decorators.timer import measure_time
from pipeline.base import BasePipeline


def step(func):
    func._is_step = True
    return func


class TaxiPipeline(BasePipeline):
    def __init__(self, file_path):
        self.file_path = file_path

    @step
    @measure_time
    @count_calls
    def stream_and_preview(self):
        """
        Wczytuje pierwszy chunk danych i pokazuje jego podgląd.
        """
        for i, chunk in enumerate(stream_and_clean_data(self.file_path, chunksize=100_000)):
            print(f"[Chunk {i + 1}] Preview:")
            print(chunk.head())
            break

    @step
    @measure_time
    @count_calls
    def streaming_analyze(self):
        """
        Wykonuje globalną analizę danych w trybie strumieniowym.
        """
        streaming_global_analysis(self.file_path)

    @step
    @measure_time
    @count_calls
    def visualize(self):
        """
        Tworzy wykresy na podstawie próbki danych (2 chunki).
        """
        df_sample = load_sample_for_visualization(
            self.file_path,
            chunksize=100_000,
            max_chunks=2
        )
        visualize_data(df_sample)

    @step
    @measure_time
    @count_calls
    def parallel_analysis(self):
        run_multiprocessing(self.file_path, chunksize=250_000)
