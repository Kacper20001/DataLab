# do sprawdzenia nazw kolumn
import pandas as pd
from core.streamer import stream_parquet

def test_stream_parquet_reads_chunks():
    chunks = list(stream_parquet("data/raw/yellow_tripdata_2024-01.parquet", chunksize=100_000))
    assert isinstance(chunks[0], pd.DataFrame)
    assert len(chunks) > 0
