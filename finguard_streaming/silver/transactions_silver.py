from pyspark import pipelines as db
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as f
from pyspark.sql.types import *
import json

@db.table(
    name='finguard.silver.transactions',
    comment = 'parsed and cleaned transactions data'
)
@db.expect_or_drop('valid_transaction_id', 'transaction_id IS NOT NULL')
@db.expect_or_drop('valid_customer_id', 'customer_id IS NOT NULL')
@db.expect_or_drop('valid_merchant_id', 'merchant_id IS NOT NULL')
@db.expect('valid_amount', 'amount > 0')

def transactions_silver() -> DataFrame:
    db_bronze = spark.readStream.table('finguard.bronze.transactions')
    schema = f.StructType([
        StructField('transaction_id', StringType(), True),
        StructField('customer_id', StringType(), True),
        StructField('card_number', StringType(), True),
        StructField('merchant_id', StringType(), True),
        StructField('merchant_name', StringType(), True),
        StructField('merchant_category', StringType(), True),
        StructField('amount', DoubleType(), True),
        StructField('currency', StringType(), True),
        StructField('transaction_type', StringType(), True),
        StructField('payment_channel', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('city', StringType(), True),
        StructField('country', StringType(), True),
        StructField('transaction_timestamp', TimestampType(), True),
        StructField('is_international', BooleanType(), True),
        StructField('status', StringType(), True),
    ])

    transformed_df = db_bronze.select(
        f.from_json(col('value'), schema).alias('data'),
        f.col('topic').alias('kafka_topic'),
        f.col('partition').alias('kafka_partition'),
        f.col('offset').alias('kafka_offset'),
        f.col('timestamp').alias('kafka_timestamp'),
        f.col('ingesttime')

    ).select(
        f.col('data.*'),
        f.col('kafka_topic'),
        f.col('kafka_partition'),
        f.col('kafka_offset'),
        f.col('kafka_timestamp'),
        f.col('ingesttime'),
        f.current_timestamp().alias('silver_timestamp')
    )

    return transformed_df
  
  