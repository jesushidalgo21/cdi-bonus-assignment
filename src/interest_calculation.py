from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round as spark_round, when, lit

def calculate_interest(wallet_history: DataFrame, cdi_rates: DataFrame) -> DataFrame:
    # Join y renombrado
    cdi_rates = cdi_rates.withColumnRenamed("partition_date", "cdi_date")
    joined = wallet_history.join(
        cdi_rates,
        wallet_history.partition_date == cdi_rates.cdi_date,
        "inner"
    )

    # Calcular interés solo para balances > $100 y sin movimientos en 24h
    interest_df = joined.withColumn(
        "eligible_balance",
        when(col("balance") > 100, col("balance")).otherwise(0.0)
    ).withColumn(
        "interest_amount",
        spark_round(col("eligible_balance") * col("cdi_rate"), 8)
    ).withColumn(
        "is_paid", lit(False)  # Inicialmente no pagado
    ).withColumn(
        "payment_date", lit(None).cast("timestamp")
    ).select(
        "user_id",
        "account_id",
        "eligible_balance",
        col("cdi_rate").alias("interest_rate"),
        "interest_amount",
        "is_paid",
        "payment_date",
        "partition_date"
    )

    return interest_df.filter(col("interest_amount") > 0)