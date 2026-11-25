from pyspark import pipelines as db
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F


@db.table(
    name ='finguard.silver.customers_transactions',
    comment ='parsed and clean customers transactions data'
)
@db.expect_or_drop('valid customer_id', 'customer_id IS NOT NULL')

def Transactions() -> DataFrame:
    df_bronze = spark.readStream.table('finguard.bronze.customers')
    
    transformed_df = df_bronze.select(
        F.col('customer_id'),
        F.col('first_name'),
        F.col('last_name'),
        F.col('gender'),
        F.col('age'),
        F.col('city'),
        F.col('state'),
        F.col('country'),
        F.col('annual_income'),
        F.col('customer_segment'),
        F.to_date(F.col('account_open_date'), 'yyyy-mm-dd').alias('account_open_date'),
        F.col('risk_score'),
        F.col('preferred_spending_min'),
        F.col('preferred_spending_max'),
        F.col('preferred_city'),
        F.col('preferred_country'),
        F.col('trusted_device_id'),
        F.col('card_number'),
        F.col('card_type'),
        F.col('email'),
        F.col('transaction_limit'),
        F.col('update_timestamp'),
        F.current_timestamp().alias('silver_timestamp')
    )

    return transformed_df
