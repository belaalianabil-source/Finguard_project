# Databricks notebook source
source_path = "/Volumes/finguard/source/fraud_watchlist/source/"

# COMMAND ----------

dbutils.fs.ls(source_path)


# COMMAND ----------

input_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format","json")
    .option("cloudFiles.schemaLocation","/Volumes/finguard/source/fraud_watchlist/schema/")
    .option("cloudFiles.inferColumnTypes","True")
    .load(source_path)
)

# COMMAND ----------

from pyspark.sql import functions as F
transformed_df = input_stream.select(
    "*",
    F.col("_metadata.file_path").alias("file_path"),
    F.current_timestamp().alias("ingest_timestamp")
)
    

# COMMAND ----------

stream_query = (
   transformed_df.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/Volumes/finguard/source/fraud_watchlist/checkpoint/")
  .trigger(availableNow=True)
  .toTable("finguard.bronze.watchlist_batch_test2")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.watchlist_batch_test2

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.silver.fraud_watchlist_silver