from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit, round as spark_round
from src.utils import get_postgres_connection, load_config
from src.schemas import cdi_rates_schema, interest_payments_schema
from src.logger import Logger
from src.spark_manager import SparkSessionManager
from src.audit_logger import InterestAuditLogger
from decimal import Decimal

logger = Logger().get_logger()
spark = SparkSessionManager().get_spark_session("InterestCalculator")
audit_logger = InterestAuditLogger(spark)

class InterestCalculator:
    def __init__(self):
        pass

    def _load_filtered_data(self, config, start_date, end_date=None):
        logger.info("Cargando datos desde PostgreSQL y Parquet")
        wallet_history = self._load_wallet_history(config)
        cdi_rates = self._load_cdi_rates(config)
        wallet_filtered = self._filter_by_partition_date(wallet_history, start_date, end_date)
        cdi_filtered = self._filter_by_partition_date(cdi_rates, start_date, end_date)
        logger.info(f"Wallet history registros cargados: {wallet_filtered.count()}")
        logger.info(f"CDI rates registros cargados: {cdi_filtered.count()}")
        return wallet_filtered, cdi_filtered

    def _load_wallet_history(self, config):
        pg_conf = config["postgres"]
        query = f"(SELECT * FROM {pg_conf['schema']}.{pg_conf['wallet_history_table']}) AS wallet_history"
        jdbc_url, connection_props = get_postgres_connection(pg_conf)
        logger.info("Conectando a PostgreSQL para cargar wallet_history")
        return spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", query) \
            .option("user", connection_props["user"]) \
            .option("password", connection_props["password"]) \
            .option("driver", connection_props["driver"]) \
            .load()

    def _load_cdi_rates(self, config):
        path = config["paths"]["cdi_rates"]
        logger.info(f"Cargando CDI rates desde Parquet en {path}")
        return spark.read.schema(cdi_rates_schema).parquet(path)

    def _filter_by_partition_date(self, df, start_date, end_date=None):
        if end_date is None:
            return df.filter(col("partition_date") == start_date)
        return df.filter((col("partition_date") >= start_date) & (col("partition_date") <= end_date))

    def _calculate_interest_with_audit(self, wallet_history: DataFrame, cdi_rates: DataFrame, config,
                                    min_balance: float = 100.0, decimal_places: int = 8):


        # Join de wallet_history y cdi_rates
        joined = wallet_history.join(
            cdi_rates.withColumnRenamed("partition_date", "cdi_date"),
            col("partition_date") == col("cdi_date"),
            "inner"
        )

        # Solo interesa si immovable_balance >= min_balance
        qualified_col = col("immovable_balance") >= lit(min_balance)
        reason_col = when(
            qualified_col, "Califica para interés"
        ).otherwise(f"Saldo inmovilizado menor a ${min_balance}")

        interest_col = when(
            qualified_col,
            spark_round(col("immovable_balance") * (col("cdi_rate") / lit(365)), decimal_places)
        ).otherwise(lit(0.0))

        # DataFrame de pagos de interés
        payment_df = joined.select(
            col("user_id"),
            col("account_id"),
            col("immovable_balance").alias("eligible_balance"),
            col("cdi_rate").alias("interest_rate"),
            interest_col.alias("interest_amount"),
            lit(False).alias("is_paid"),
            lit(None).cast("string").alias("payment_date"),
            col("partition_date")
        )

        # DataFrame de auditoría
        audit_df = joined.select(
            col("user_id"),
            col("account_id"),
            qualified_col.alias("qualified"),
            reason_col.alias("reason"),
            col("immovable_balance").alias("relevant_balance"),
            interest_col.alias("calculated_interest"),
            col("cdi_rate").alias("interest_rate"),
            lit("success").alias("process_status"),
            lit(None).cast("string").alias("error_description"),
            col("partition_date")
        )

        # Guardar el log de auditoría como DataFrame distribuido
        audit_df_final = audit_logger.create_audit_df(
            audit_df.rdd, audit_logger.job_id, audit_logger.job_timestamp
        )
        if config is not None:
            audit_logger.save_to_postgres_df(audit_df_final, config)

        return payment_df

    def _save_results(self, interest_df: DataFrame, config):
        pg_conf = config["postgres"]
        jdbc_url, connection_props = get_postgres_connection(pg_conf)
        target_table = f"{pg_conf['schema']}.{pg_conf['interest_payments_table']}"
        logger.info(f"Guardando resultados en {target_table} particionado por fecha")
        (interest_df.write
                  .jdbc(
                      url=jdbc_url,
                      table=target_table,
                      mode="overwrite",
                      properties=connection_props
                  ))
        logger.info("✅ Intereses guardados exitosamente")

    def calculate_and_save_interest(self, start_date: str, end_date: str = None):
        try:
            config = load_config()
            wallet_df, cdi_df = self._load_filtered_data(config, start_date, end_date)

            if wallet_df.rdd.isEmpty() or cdi_df.rdd.isEmpty():
                error_msg = "No se encontraron datos en wallet_history o cdi_rates"
                logger.warning(error_msg)
                return

            logger.info("Calculando intereses con auditoría")
            interest_df = self._calculate_interest_with_audit(wallet_df, cdi_df, config)

            logger.info(f"Intereses calculados: {interest_df.count()} registros")
            interest_df.show(20, False)

            self._save_results(interest_df, config)

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error en el cálculo de intereses: {error_msg}", exc_info=True)
            raise
