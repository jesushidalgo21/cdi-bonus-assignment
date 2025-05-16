from pyspark.sql import Window
from pyspark.sql.functions import (
    to_date, col, when, abs, sum, last, count
)
from src.validation import CDCDataValidator
from src.schemas import wallet_history_schema
from src.utils import get_postgres_connection, load_config
from src.logger import Logger
from src.spark_manager import SparkSessionManager

logger = Logger().get_logger()
spark = SparkSessionManager().get_spark_session("WalletHistory")

class WalletHistoryGenerator:
    def __init__(self):
        pass

    def _read_cdc_data(self, cdc_path: str):
        logger.info(f"Leyend_dateo CDC desde: {cdc_path}")
        return spark.read.parquet(cdc_path)

    def _filter_by_date_range(self, df, start_date: str, end_date: str):
        logger.info(f"Filtrando eventos entre {start_date} y {end_date}")
        df = df.withColumn("event_date", to_date("event_time"))
        return df.filter((col("event_date") >= start_date) & (col("event_date") <= end_date))

    def _validate_data(self, df):
        logger.info("Validando datos CDC")
        validator = CDCDataValidator(df)
        is_valid, errors = validator.validate()
        if not is_valid:
            error_msg = "\n".join(errors)
            logger.error(f"❌ Errores de validación:\n{error_msg}")
            raise ValueError(error_msg)
        return df

    def _normalize_amounts(self, df):
        logger.info("Normalizando montos")
        return df.withColumn(
            "normalized_amount",
            when(col("transaction_type").isin("WITHDRAWAL", "TRANSFER_OUT"),
                 -abs(col("amount")))
             .otherwise(abs(col("amount")))
        )

    def _calculate_running_balance(self, df):
        logger.info("Calculando balance cronológico por cuenta")
        window_spec = Window.partitionBy("account_id").orderBy("event_time") \
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        return df.withColumn("running_balance", sum("normalized_amount").over(window_spec))

    def _prepare_final_dataset(self, df, start_date: str, end_date: str):
        logger.info("Agregando columnas auxiliares")
        df = df.withColumn("partition_date", to_date(col("event_time")))

        logger.info("Agregando balance diario por cuenta y usuario")
        final_balance = df.groupBy("account_id", "user_id", "partition_date").agg(
            last("running_balance").alias("balance"),
            count("*").alias("transactions_count")
        ).filter((col("partition_date") >= start_date) & (col("partition_date") <= end_date))

        return final_balance

    def _cast_to_schema(self, df):
        logger.info("Casteando columnas al schema esperado")
        for field in wallet_history_schema.fields:
            if field.name in df.columns:
                df = df.withColumn(field.name, col(field.name).cast(field.dataType))
        return df

    def _write_to_postgres(self, df):
        logger.info("Conectando a PostgreSQL para escritura")
        pg_conf = load_config()["postgres"]
        jdbc_url, connection_props = get_postgres_connection(pg_conf)
        table = f"{pg_conf['schema']}.{pg_conf['wallet_history_table']}"

        df.write.jdbc(
            url=jdbc_url,
            table=table,
            mode="overwrite",
            properties=connection_props
        )
        logger.info("✅ Carga exitosa en PostgreSQL")

    def load_wallet_history(self, cdc_path: str, start_date: str, end_date: str):
        try:
            # Pipeline de procesamiento
            df = self._read_cdc_data(cdc_path)
            df = self._filter_by_date_range(df, start_date, end_date)

            if df.rdd.isEmpty():
                logger.warning("⚠️ No se encontraron datos en el rango de fechas")
                return

            df = self._validate_data(df)
            df = self._normalize_amounts(df)
            df = self._calculate_running_balance(df)
            final_balance = self._prepare_final_dataset(df, start_date, end_date)

            if final_balance.rdd.isEmpty():
                logger.warning("⚠️ No hay datos para cargar en wallet_history luego del procesamiento")
                return

            final_balance = self._cast_to_schema(final_balance)
            self._write_to_postgres(final_balance)

        except Exception as e:
            logger.critical(f"💥 Error inesperado en generate_wallet_history: {e}", exc_info=True)
            raise