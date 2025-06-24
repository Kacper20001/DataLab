from pipeline.taxi_pipeline import TaxiPipeline

pipeline = TaxiPipeline("data/raw/yellow_tripdata_2024-01.parquet")
pipeline.run()