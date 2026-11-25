from pyspark import pipelines as db
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

@db.table(
    name="finguard.silver.fraud_watchlist_silver",
    comment="stream dataframe fraud whatchlist"
)
def fraud_watchlist_bronze() -> DataFrame:
        input_stream = spark.readStream.table("finguard.bronze.fraud_watchlist_bronze")
    
        transformed_df = input_stream.select(
        F.col("watchlist_id"),
        F.col("watch_type"),
        F.col("entity_id"),
        F.col("risk_level"),
        F.col("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("status"),
        F.to_date(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("country"),
        F.col("city"),
        F.col("ingest_timestamp").alias("bronze_ingest_timestamp"),
        F.col("file_path").alias("source_file"),
        F.current_timestamp().alias("silver_ingest_timestamp")
    )
       
        return transformed_df
    