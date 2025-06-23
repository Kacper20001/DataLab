from pipeline.taxi_pipeline import TaxiPipeline

if __name__ == "__main__":
    path = "data/raw/yellow_tripdata_2024-01.parquet"
    pipeline = TaxiPipeline(path)
    pipeline.run()
