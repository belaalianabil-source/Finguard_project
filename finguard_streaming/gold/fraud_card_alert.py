from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql import DataFrame


@dp.table(
    name='finguard.gold.fraud_card_alert',
    comment="Alert details where transaciton has been performed with value higher than what is configured by customer"
)

def fraud_card_alert() -> DataFrame:
    
    costumers_silver_df = spark.read.table('finguard.silver.customers_transactions')
    watchlist_silver_df = spark.readStream.table('finguard.silver.fraud_watchlist_silver')
    transactions_silver_df = spark.readStream.table('finguard.silver.transactions')

    transactions_withWatermark = transactions_silver_df.withWatermark("transaction_timestamp", "5 minutes")
    watchlist_withWatermark = watchlist_silver_df.withWatermark("silver_ingest_timestamp", "5 minutes")

    fraud_detected = (
         transactions_withWatermark.join(watchlist_withWatermark,transactions_withWatermark.card_number == watchlist_withWatermark.entity_id , 'inner')
         .join(costumers_silver_df,transactions_withWatermark.customer_id == costumers_silver_df.customer_id , 'left')
         .select(
             # Alert identification
             F.concat_ws("-", F.lit("FRAUD"), transactions_withWatermark.transaction_id, watchlist_withWatermark.watchlist_id).alias("alert_id"),
             F.lit("FRAUD_WATCHLIST_MATCH").alias("alert_type"),
             F.current_timestamp().alias("alert_timestamp"),

             # Transaction details
             transactions_withWatermark.transaction_id.alias("transaction_id"),
             transactions_withWatermark.transaction_timestamp.alias("transaction_timestamp"),
             transactions_withWatermark.card_number.alias("card_number"),
             transactions_withWatermark.amount.alias("transaction_amount"),
             transactions_withWatermark.currency.alias("currency"),
             transactions_withWatermark.merchant_name.alias("merchant_name"),
             transactions_withWatermark.merchant_category.alias("merchant_category"),
             transactions_withWatermark.country.alias("transaction_country"),
             transactions_withWatermark.transaction_type.alias("transaction_type"),

             # Extra transaction details for the fraud alert email
             transactions_withWatermark.amount.alias("amount"),
             transactions_withWatermark.payment_channel.alias("payment_channel"),
             transactions_withWatermark.city.alias("transaction_city"),
             transactions_withWatermark.is_international.alias("is_international"),
             transactions_withWatermark.status.alias("transaction_status"),

             # Watchlist details
             watchlist_withWatermark.watchlist_id.alias("watchlist_id"),
             watchlist_withWatermark.entity_id.alias("watchlist_entity_id"),
             watchlist_withWatermark.watch_type.alias("watchlist_entity_type"),
             watchlist_withWatermark.risk_level.alias("watchlist_risk_level"),
             watchlist_withWatermark.reason_description.alias("watchlist_reason"),
             watchlist_withWatermark.effective_from.alias("watchlist_effective_from"),

             # Extra watchlist details for the fraud alert email
             watchlist_withWatermark.watch_type.alias("watch_type"),
             watchlist_withWatermark.risk_level.alias("risk_level"),
             watchlist_withWatermark.action.alias("action"),
             watchlist_withWatermark.reason_description.alias("reason_description"),
             watchlist_withWatermark.reported_by.alias("reported_by"),
             watchlist_withWatermark.reported_source.alias("reported_source"),
             watchlist_withWatermark.city.alias("watchlist_city"),
             watchlist_withWatermark.country.alias("watchlist_country"),

             # Customer details
             transactions_withWatermark.customer_id.alias("customer_id"),
             F.concat_ws(",", costumers_silver_df.first_name, costumers_silver_df.last_name).alias("customer_name"),
             costumers_silver_df.email.alias("customer_email"),

         )
         
    )
    return fraud_detected
    
    

    


    

