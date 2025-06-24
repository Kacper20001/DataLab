import pandas as pd
from core.streamer import stream_parquet

def load_sample_for_visualization(path: str, chunksize: int = 100_000, max_chunks: int = 2) -> pd.DataFrame:
    """
    Ładuje próbkę danych z pierwszych N chunków do wizualizacji.

    :param path: Ścieżka do pliku .parquet
    :param chunksize: Liczba wierszy na chunk
    :param max_chunks: Ile chunków maksymalnie wczytać
    :return: Złączony DataFrame jako próbka
    """
    chunks = []
    for i, chunk in enumerate(stream_parquet(path, chunksize=chunksize)):
        chunks.append(chunk)
        if i + 1 >= max_chunks:
            break
    return pd.concat(chunks, ignore_index=True)
