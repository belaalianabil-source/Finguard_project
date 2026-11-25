from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as f
import json

@dp.table(
    name='finguard.bronze.transactions',
    comment = 'transactions raw stream data from kafka'

)
def transactions() -> DataFrame:
      # Read from Kafka
      kafka_connection_json = dbutils.secrets.get(
          scope="finguard-scope",
          key="kafka_connection_details"
      )
      kafka_config = json.loads(kafka_connection_json)
      bootstrap_server = kafka_config["bootstrap_servers"]
      api_key = kafka_config["api_key"]
      api_secret = kafka_config["api_secret"]
      topic = kafka_config["topic"]

      jaas_config = (
          f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
          f'username="{api_key}" '
          f'password="{api_secret}";'
        )
       
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
      
      # transform dataframe
      parsed_streaming_df = df.select(
          col("key").cast("string"),
          col("value").cast("string"),
          col("topic"),
          col("partition"),
          col("offset"),
          col("timestamp"),
          col("timestampType"),
          f.current_timestamp().alias("ingesttime")
      )
      return parsed_streaming_df