from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import uuid
from src.utils import get_postgres_connection
from src.logger import Logger
from src.schemas import audit_log_schema

logger = Logger().get_logger()

class InterestAuditLogger:
    """
    Auditoría eficiente de decisiones de cálculo de interés.
    Permite registrar logs directamente como DataFrame y guardarlos en PostgreSQL sin usar collect().
    """
    def __init__(self, spark: SparkSession = None):
        self.job_id = str(uuid.uuid4())
        self.job_timestamp = datetime.now()
        self.spark = spark

    def create_audit_df(self, audit_rdd, job_id, job_timestamp, partition_date=None) -> DataFrame:
        """
        Recibe un RDD de tuplas con los campos de auditoría y devuelve un DataFrame listo para guardar.
        """
        def enrich(record):
            # record: (user_id, account_id, qualified, reason, relevant_balance, calculated_interest, interest_rate, process_status, error_desc, partition_date)
            return {
                "job_id": job_id,
                "job_timestamp": job_timestamp,
                "user_id": record[0] or "UNKNOWN_USER",
                "account_id": record[1] or "UNKNOWN_ACCOUNT",
                "qualified": record[2],
                "reason": record[3] or "No reason provided",
                "relevant_balance": record[4] if record[4] is not None else 0.0,
                "calculated_interest": record[5] if record[5] is not None else 0.0,
                "interest_rate": record[6] if record[6] is not None else 0.0,
                "process_status": record[7] or "unknown",
                "error_description": record[8],
                "partition_date": record[9] if partition_date is None else partition_date
            }
        enriched_rdd = audit_rdd.map(enrich)
        return self.spark.createDataFrame(enriched_rdd, schema=audit_log_schema)

    def save_to_postgres_df(self, audit_df: DataFrame, config: dict) -> None:
        """
        Guarda el DataFrame de auditoría en PostgreSQL.
        """
        if audit_df.rdd.isEmpty():
            logger.warning("No hay datos de auditoría para guardar")
            return

        try:
            pg_conf = config["postgres"]
            jdbc_url, connection_props = get_postgres_connection(pg_conf)
            table = f"{pg_conf['schema']}.{pg_conf['interest_audit_log_table']}"
            logger.info(f"Guardando logs de auditoría en {table}")

            audit_df.write.jdbc(
                url=jdbc_url,
                table=table,
                mode="overwrite", # append
                properties=connection_props
            )
            logger.info(f"Guardados {audit_df.count()} registros de auditoría")
            logger.info("✅ Logs de auditoría guardados exitosamente")
        except Exception as e:
            logger.error(f"Error al guardar logs de auditoría: {str(e)}", exc_info=True)
            raise