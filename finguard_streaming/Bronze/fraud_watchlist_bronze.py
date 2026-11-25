from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

@dp.table(
    name="finguard.bronze.fraud_watchlist_bronze",
    comment="stream dataframe fraud whatchlist"
)
def fraud_watchlist_bronze() -> DataFrame:
        input_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format","json")
        .option("cloudFiles.schemaLocation","/Volumes/finguard/source/fraud_watchlist/schema/")
        .option("cloudFiles.inferColumnTypes","True")
        .load("/Volumes/finguard/source/fraud_watchlist/source/")
    )
    
        transformed_df = input_stream.select(
        F.col("watchlist_id"),
        F.col("watch_type"),
        F.col("entity_id"),
        F.col("risk_level"),
        F.col("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("status"),
        F.col("effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("country"),
        F.col("city"),
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("file_path"),
        F.current_timestamp().alias("ingest_timestamp")
    )
       
        return transformed_df
    