from core.loader import load_parquet
from core.cleaner import clean_data
from core.visualizer import visualize_data
from core.analyzer import streaming_global_analysis
from core.parallel import run_multiprocessing
from decorators.counter import count_calls
from decorators.timer import measure_time
from pipeline.base import BasePipeline

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
        run_multiprocessing(self.file_path, batch_size=250_000)
