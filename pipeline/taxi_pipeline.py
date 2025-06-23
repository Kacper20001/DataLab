from core.loader import load_parquet
from core.cleaner import clean_data
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
