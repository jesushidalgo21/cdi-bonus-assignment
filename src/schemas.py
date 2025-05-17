from pyspark.sql.types import *

wallet_history_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("balance", DoubleType(), True),
    StructField("balance_previous", DoubleType(), True),
    StructField("immovable_balance", DoubleType(), True),
    StructField("last_movement_ts", TimestampType(), True),
    StructField("total_deposits", DoubleType(), True),
    StructField("total_withdrawals", DoubleType(), True),
    StructField("transactions_count", IntegerType(), True),
    StructField("partition_date", StringType(), True),
])

cdi_rates_schema = StructType([
    StructField("cdi_rate", DoubleType(), True),
    StructField("partition_date", StringType(), True),
])

interest_payments_schema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    StructField("eligible_balance", DecimalType(10, 2), nullable=False),
    StructField("interest_rate", DecimalType(5, 4), nullable=False),
    StructField("interest_amount", DecimalType(10, 2), nullable=False),
    StructField("is_paid", BooleanType(), nullable=False),
    StructField("payment_date", TimestampType(), nullable=True),
    StructField("partition_date", StringType(), nullable=False)
])

cdc_data_schema = StructType([
    StructField("account_id", StringType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("transaction_type", StringType(), nullable=False),
    StructField("amount", IntegerType(), nullable=False),
    StructField("event_time", TimestampType(), nullable=False),
])

audit_log_schema = StructType([
    StructField("job_id", StringType(), nullable=False),
    StructField("job_timestamp", TimestampType(), nullable=False),
    StructField("user_id", StringType(), nullable=False),
    StructField("account_id", StringType(), nullable=False),
    StructField("qualified", BooleanType(), nullable=False),
    StructField("reason", StringType(), nullable=True),
    StructField("relevant_balance", DoubleType(), nullable=True),
    StructField("calculated_interest", DoubleType(), nullable=True),
    StructField("interest_rate", DoubleType(), nullable=True),
    StructField("process_status", StringType(), nullable=False),
    StructField("error_description", StringType(), nullable=True),
    StructField("partition_date", StringType(), nullable=False)
])