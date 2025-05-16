from pyspark.sql.types import *

wallet_history_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("balance", DoubleType(), True),
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