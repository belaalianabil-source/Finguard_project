import pyspark.sql.functions as F
from pyspark import pipelines as dp
from pyspark.sql import DataFrame


@dp.table(
    name = 'finguard.gold.transactions_count_by_minute'
    ,comment = 'Number of transactions per minute'
)

def transactions_count_by_minute() -> DataFrame:

    transactions = spark.readStream.table('finguard.silver.transactions')
    
    transactions_with_watermark=transactions.withWatermark("transaction_timestamp", "5 minutes")

    transaction_count_df=transactions_with_watermark.groupBy(
        F.window("transaction_timestamp","1 minute")
    ).agg(

    F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )
    return transaction_count_df

