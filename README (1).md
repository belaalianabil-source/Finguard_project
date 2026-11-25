# FinGuard: Real-Time Fraud Detection Pipeline

A real-time credit card transaction monitoring system built on **Databricks Lakeflow Declarative Pipelines** (PySpark). It streams card transactions from **Kafka (Confluent Cloud)**, matches them against a fraud watchlist and each customer's spending limit, and sends **email alerts** to affected customers.

## Features

- **Streaming ingestion** of card transactions from Kafka and of watchlist files with Auto Loader
- **Medallion architecture** (Bronze → Silver → Gold) in Unity Catalog
- **Fraud watchlist matching** using a stream-stream join between transactions and watchlist entries
- **High-value transaction alerts** when an amount exceeds the customer's configured limit
- **Real-time metrics**: transactions per minute (tumbling window) and a 5-minute sliding window refreshed every minute
- **Email notifications** through a `foreach_batch_sink` and Gmail SMTP
- **Data quality rules** using pipeline expectations
- **Secure credentials** stored in a Databricks secret scope

## Architecture

```
Kafka (Confluent)  ──►  bronze.transactions  ──►  silver.transactions ──┬──►  gold.high_value_alerts_stream ──► email alert
                                                                         ├──►  gold.fraud_card_alert ──────────► email alert
Customers          ──►  bronze.customers     ──►  silver.customers_transactions ─┘ (joined as a lookup)
Watchlist JSON     ──►  bronze.fraud_watchlist_bronze ──► silver.fraud_watchlist_silver ──┘
                                                                         ├──►  gold.transactions_count_by_minute
                                                                         └──►  gold.transactions_count_by_minute_sliding_window
```

All tables live in the Unity Catalog catalog `finguard`.

## Project Structure

```
.
├── notebooks/                                   # Setup and testing notebooks
│   ├── 1_kfka_streaming_test.py                 # Test the Kafka connection and streaming read
│   ├── 2_secret_scope.py                        # Create the secret scope and store credentials
│   ├── 3_sending_email.py                       # Test Gmail SMTP sending
│   ├── 4_test_transformation_watchList.py       # Test watchlist ingestion with Auto Loader
│   └── fraud_watchlist_data_generato.py         # Simulate a watchlist feed from the CSV
├── data/
│   └── fraud_watchlist.csv                      # Sample fraud watchlist
└── pipeline/
    ├── bronze/
    │   ├── transactions_bronze.py               # Kafka → bronze
    │   └── fraud_watchlist_bronze.py            # Auto Loader JSON → bronze
    ├── silver/
    │   ├── transactions_silver.py               # Parse Kafka JSON, quality checks
    │   ├── customers_silver.py                  # Clean customer profiles
    │   └── fraud_watchlist_silver.py            # Clean watchlist records
    └── gold/
        ├── high_value_transactions_alert.py     # Amount > customer limit
        ├── fraud_card_alert.py                  # Card matches watchlist
        ├── transactions_count_by_minute.py      # 1-minute tumbling window
        ├── transaciton_count_by_minute_sliding_window.py  # 5-minute window, 1-minute slide
        ├── hight-value-trnasactions-email-alert.py        # Email sink: high-value alerts
        └── fraud_card_email_alert.py                      # Email sink: fraud alerts
```

## Tables

| Layer | Table | Description |
|---|---|---|
| Bronze | `finguard.bronze.transactions` | Raw Kafka messages (key, value, topic, partition, offset, timestamp) |
| Bronze | `finguard.bronze.fraud_watchlist_bronze` | Watchlist JSON files loaded with Auto Loader, with source file and ingest time |
| Silver | `finguard.silver.transactions` | Kafka JSON parsed into typed columns (amount, merchant, card, location, status, etc.) |
| Silver | `finguard.silver.customers_transactions` | Clean customer profile with card number, email and transaction limit |
| Silver | `finguard.silver.fraud_watchlist_silver` | Clean watchlist with parsed `effective_from` date |
| Gold | `finguard.gold.high_value_alerts_stream` | Transactions above the customer's limit |
| Gold | `finguard.gold.fraud_card_alert` | Transactions whose card number matches a watchlist entity |
| Gold | `finguard.gold.transactions_count_by_minute` | Transaction count per 1-minute window |
| Gold | `finguard.gold.transactions_count_by_minute_sliding_window` | Transaction count over a 5-minute window sliding every minute |

## Data Quality

| Table | Drop rows when | Warn when |
|---|---|---|
| `silver.transactions` | `transaction_id`, `customer_id` or `merchant_id` is null | `amount` ≤ 0 |
| `silver.customers_transactions` | `customer_id` is null | – |

## Prerequisites

- A Databricks workspace with Unity Catalog and serverless or classic compute
- A Kafka topic with transaction events, for example on Confluent Cloud
- A Gmail account with an **App Password** for sending alerts
- Unity Catalog objects: catalog `finguard`, schemas `bronze`, `silver`, `gold` and `source`, and volumes under `/Volumes/finguard/source/`
- A `finguard.bronze.customers` table with customer data

## Setup

1. **Create a secret scope** named `finguard-scope` (for example with the Databricks CLI or `2_secret_scope.py`).
2. **Store your credentials as secrets.** Never commit real values to Git.
   - `kafka_connection_details`: a JSON string with `bootstrap_servers`, `topic`, `api_key` and `api_secret`
   - `gmail_api_key`: your Gmail App Password
3. **Test your connections** with the notebooks `1_kfka_streaming_test.py` and `3_sending_email.py`.
4. **Create a pipeline** in Databricks and add the files under `pipeline/` as its source code.
5. **Set the target catalog** to `finguard` and start the pipeline.
6. **Simulate watchlist updates** by running `fraud_watchlist_data_generato.py`. It writes one JSON record from `fraud_watchlist.csv` every 5 seconds into `/Volumes/finguard/source/fraud_watchlist/source/`.

## Alerts

| Alert | Trigger | Email contains |
|---|---|---|
| High value transaction | `amount > transaction_limit` | Transaction details, the customer's limit, and the action to take |
| Fraud watchlist match | Card number equals a watchlist `entity_id` | Transaction details, risk level, watchlist reason, and the fraud hotline steps |

## Tech Stack

- Databricks Lakeflow Declarative Pipelines (`pyspark.pipelines`)
- PySpark Structured Streaming (watermarks, windows, stream-stream joins)
- Auto Loader (`cloudFiles`)
- Apache Kafka (Confluent Cloud)
- Unity Catalog and Databricks Secrets
- Python `smtplib` (Gmail SMTP)

## Security

- Credentials must only be stored in Databricks secrets, never in code, notebooks or comments.
- If a key or password was ever committed, **rotate it immediately**. Removing it in a later commit does not remove it from Git history.

## License

Add your license here (for example, MIT).
