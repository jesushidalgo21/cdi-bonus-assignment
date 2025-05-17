from datetime import datetime, timedelta
from pyspark.sql import Window, DataFrame
from pyspark.sql.functions import (
    to_date, to_timestamp, col, when, sum as spark_sum, abs as spark_abs,
    last, count, lit, lag, greatest
)
from src.validation import CDCDataValidator
from src.utils import get_postgres_connection, load_config
from src.logger import Logger
from src.spark_manager import SparkSessionManager
from pyspark.sql.types import *

logger = Logger().get_logger()
spark = SparkSessionManager().get_spark_session("WalletHistoryOptimized")

class WalletHistoryGenerator:
    def __init__(self):
        pass

    def _read_and_prepare_data(self, cdc_path: str) -> DataFrame:
        logger.info(f"Leyendo y preparando datos desde: {cdc_path}")
        df = spark.read.parquet(cdc_path)

        is_valid, errors = CDCDataValidator(df).validate()
        if not is_valid:
            raise ValueError("\n".join(errors))

        return df.withColumn(
            "normalized_amount",
            when(col("transaction_type").isin("WITHDRAWAL", "TRANSFER_OUT"), -spark_abs((col("amount"))))
            .otherwise(spark_abs(col("amount")))
        ).withColumn("event_ts", to_timestamp(col("event_time")))

    def _compute_running_balances(self, df: DataFrame) -> DataFrame:
        logger.info("Calculando running balances")
        df = df.repartition("account_id")
        balance_window = Window.partitionBy("account_id").orderBy("event_ts").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        return df.withColumn("running_balance", spark_sum("normalized_amount").over(balance_window))

    def _generate_daily_balances(self, df_balances: DataFrame, start_date: str, end_date: str) -> DataFrame:
        logger.info(f"Generando balances diarios desde {start_date} hasta {end_date}")

        # 1. Generar rango de fechas y producto cartesiano con cuentas
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        dates_range = [(start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
                    for i in range((end_dt - start_dt).days + 1)]
        dates_df = spark.createDataFrame([(d,) for d in dates_range], ["partition_date"])
        accounts_df = df_balances.select("account_id", "user_id").distinct()
        full_date_account_df = accounts_df.crossJoin(dates_df)

        # 2. Calcular balances diarios y columnas auxiliares
        eod_balances = df_balances.groupBy("account_id", "partition_date") \
            .agg(
                last("running_balance").alias("balance"),
                last("event_ts").alias("last_movement_ts"),
                spark_sum(when(col("normalized_amount") > 0, col("normalized_amount"))).alias("total_deposits"),
                spark_sum(when(col("normalized_amount") < 0, col("normalized_amount"))).alias("total_withdrawals")
            )

        window_lag = Window.partitionBy("account_id").orderBy("partition_date")
        eod_balances_with_lag = eod_balances.withColumn(
            "balance_previous", lag("balance", 1).over(window_lag)
        ).fillna(0.0, subset=["balance_previous"])

        # 3. Calcular retiros acumulados por día
        withdrawals_daily = df_balances.withColumn(
            "withdrawal",
            when(col("normalized_amount") < 0, -col("normalized_amount")).otherwise(0.0)
        ).groupBy("account_id", "partition_date").agg(
            spark_sum("withdrawal").alias("withdrawals_sum")
        )

        # 4. Unir y calcular immovable_balance (Spark puro)
        daily_balances = eod_balances_with_lag.withColumn(
            "immovable_balance",
            greatest(
                col("balance_previous") - greatest(spark_abs(col("total_withdrawals")) - col("total_deposits"), lit(0.0)),
                lit(0.0)
            )
        )

        # 5. Calcular cantidad de transacciones diarias
        daily_transaction_counts = df_balances.groupBy("account_id", to_date("event_ts").alias("partition_date")) \
            .agg(count("*").alias("transactions_count"))

        # 6. Unir todo al producto cartesiano para asegurar fechas sin movimientos
        final_df = full_date_account_df.join(daily_balances, ["account_id", "partition_date"], "left") \
            .join(daily_transaction_counts, ["account_id", "partition_date"], "left") \
            .fillna({
                "balance": 0.0,
                "balance_previous": 0.0,
                "immovable_balance": 0.0,
                "total_deposits": 0.0,
                "total_withdrawals": 0.0,
                "transactions_count": 0
            }) \
            .select(
                "account_id", "user_id", "balance", "balance_previous",
                "immovable_balance", "last_movement_ts", "total_deposits", "total_withdrawals",
                "transactions_count", "partition_date"
            )

        return final_df

    def load_wallet_history(self, cdc_path: str, start_date: str, end_date: str):
        df_prepared = None
        df_balances = None
        final_df = None

        try:
            logger.info(f"Iniciando generación de wallet history de {start_date} a {end_date}")
            df_prepared = self._read_and_prepare_data(cdc_path)
            df_balances = self._compute_running_balances(df_prepared)
            df_balances = df_balances.withColumn("partition_date", to_date(col("event_ts")))

            final_df = self._generate_daily_balances(df_balances, start_date, end_date)

            if final_df.count() == 0:
                logger.info("No hay datos para escribir en wallet_history, omitiendo escritura.")
                return
        
            pg_config = load_config()["postgres"]
            url, props = get_postgres_connection(pg_config)
            table = f"{pg_config['schema']}.{pg_config['wallet_history_table']}"

            props.update({
                "batchsize": "10000",
                "rewriteBatchedStatements": "true",
                "reWriteBatchedInserts": "true"
            })

            num_partitions = max(8, final_df.rdd.getNumPartitions())
            final_df.repartition(num_partitions).write.jdbc(
                url=url,
                table=table,
                mode="overwrite",
                properties=props
            )

            logger.info("✅ Wallet history generado exitosamente")

        except Exception as e:
            logger.error(f"❌ Error en load_wallet_history: {str(e)}", exc_info=True)
            raise
