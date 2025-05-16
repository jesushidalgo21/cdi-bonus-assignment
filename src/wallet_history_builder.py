from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import logging
from src.validation import validate_cdc_data
from src.schemas import wallet_history_schema  
from src.utils import get_postgres_connection, load_config

logger = logging.getLogger(__name__)

def generate_wallet_history(cdc_path: str, spark: SparkSession):
    try:
        logger.info(f"Leyendo datos CDC desde: {cdc_path}")
        df = spark.read.parquet(cdc_path)
        
        # Validación
        is_valid, errors = validate_cdc_data(df)
        if not is_valid:
            logger.error("Errores de validación:\n" + "\n".join(errors))
            raise ValueError("\n".join(errors))

        logger.info("Normalizando montos...")
        df = df.withColumn(
            "normalized_amount",
            F.when(F.col("transaction_type").isin("WITHDRAWAL", "TRANSFER_OUT"), 
                   -F.abs(F.col("amount")))
             .otherwise(F.abs(F.col("amount")))
        )

        logger.info("Calculando balance cronológico...")
        window_spec = Window.partitionBy("account_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        df = df.withColumn("running_balance", F.sum("normalized_amount").over(window_spec))

        logger.info("Agregando columna de fecha formateada...")
        df = df.withColumn("partition_date", F.to_date(F.col("event_time")))

        logger.info("Calculando balance diario por cuenta y usuario...")
        final_balance = df.groupBy("account_id", "user_id", "partition_date").agg(
            F.last("running_balance").alias("balance"),
            F.count("*").alias("transactions_count")
        )

        if final_balance.rdd.isEmpty():
            print("No hay datos para escribir en wallet_history.")
            return

        logger.info("Casteando columnas al schema esperado...")
        for field in wallet_history_schema.fields:
            if field.name in final_balance.columns:
                final_balance = final_balance.withColumn(field.name, F.col(field.name).cast(field.dataType))

        logger.info("Escribiendo resultados en PostgreSQL...")
        pg_conf = load_config()['postgres']
        jdbc_url, connection_props = get_postgres_connection(pg_conf)

        table = f"{pg_conf['schema']}.{pg_conf['wallet_history_table']}"

        final_balance.write.jdbc(
            url=jdbc_url,
            table=table,
            mode="overwrite",
            properties=connection_props
        )

        logger.info("Carga exitosa en PostgreSQL.")

    except Exception as e:
        logger.critical(f"Error inesperado: {str(e)}", exc_info=True)
        raise
