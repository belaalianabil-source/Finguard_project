# Databricks notebook source
#bootstrap_server='pkc-619z3.us-east1.gcp.confluent.cloud:9092'
#api_key='WI7A22YEEPEPVUUP'
#api_secret='cfltSvcKdn+wvc9V2gAvYX4n+xBaV6+VfUxo74AlJLXF3fpCkkfCu+gibP2dxA6w'
#topic='credit_card_transactions'

# COMMAND ----------

import json

# Retrieve the secret stored in Databricks
kafka_connection_json = dbutils.secrets.get(
    scope="finguard-scope",
    key="kafka_connection_details"
)

# Convert the JSON string into a Python dictionary
kafka_config = json.loads(kafka_connection_json)

# Extract Kafka connection details
bootstrap_server = kafka_config["bootstrap_servers"]
api_key = kafka_config["api_key"]
api_secret = kafka_config["api_secret"]
topic = kafka_config["topic"]

# Display only non-sensitive information
print("Bootstrap server:", bootstrap_server)
print("Topic:", topic)
print("Kafka configuration loaded successfully.")

# COMMAND ----------

jaas_config = (
    f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    f'username="{api_key}" '
    f'password="{api_secret}";'
)

# COMMAND ----------

df = (
    spark.read.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets", "earliest")
        .load()
)

# COMMAND ----------

df.count()

# COMMAND ----------

df.show()

# COMMAND ----------

from pyspark.sql.functions import col
prased_batch = df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

prased_batch.display()

# COMMAND ----------

prased_batch.write.saveAsTable('finguard.bronze.transactions_btach_test')

# COMMAND ----------

df = (
    spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_server)
        .option("subscribe", topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("startingOffsets", "earliest")
        .load()
)

# COMMAND ----------

from pyspark.sql.functions import *
parsed_streaming_df = df.select(
    col("key").cast("string"),
    col("value").cast("string"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp"),
    col("timestampType")
)

# COMMAND ----------

streaming_query=(
    parsed_streaming_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            "/Volumes/finguard/source/transactions/checkpoint/"
        )
        .trigger(availableNow=True)
        .toTable("finguard.bronze.transactions_streaming_test")
)
print(f"the query it's started with query id : {streaming_query.id}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from finguard.bronze.transactions_streaming_test