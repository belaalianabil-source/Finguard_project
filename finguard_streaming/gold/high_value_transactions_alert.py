from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark import pipelines as dp

@dp.table(
    name = 'finguard.gold.high_value_alerts_stream',
    comment = 'Alert details where transaciton has been performed with value higher than what is configured by customer'
)

def transactions()->DataFrame:
    customer = spark.read.table('finguard.silver.customers_transactions')
    transactions = spark.readStream.table('finguard.silver.transactions')
    
    joined_df = (
        transactions.join(customer, transactions.customer_id == customer.customer_id, "left")
                          .filter(transactions.amount > customer.transaction_limit)
                          .select(
                                F.concat_ws("-",F.lit('alert'),F.col("transaction_id")).alias("alert_id"),
                                F.lit("HIGH_VALUE_TRANSACTION").alias("alert_type"),
                                F.current_timestamp().alias("alert_timestamp"),
                                transactions.transaction_id,
                                transactions.customer_id,
                                customer.email.alias("customer_email"),
                                F.concat_ws(" ",F.col("first_name"),F.col("last_name")).alias("customer_name"),
                                transactions.amount.alias("transaction_amount"),
                                customer.transaction_limit,
                                transactions.currency,
                                transactions.merchant_name,
                                transactions.merchant_category,
                                transactions.transaction_type,
                                transactions.payment_channel,
                                transactions.city,
                                transactions.country,
                                transactions.is_international,
                                transactions.transaction_timestamp,
                                transactions.status 


                          )
    )
                
    return joined_df
    