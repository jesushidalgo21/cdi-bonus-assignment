from pyspark.sql import SparkSession
from datetime import datetime
import uuid
from src.utils import get_postgres_connection
from src.logger import Logger
from src.schemas import audit_log_schema

logger = Logger().get_logger()

class InterestAuditLogger:
    def __init__(self, spark: SparkSession = None):
        self.job_id = str(uuid.uuid4())
        self.job_timestamp = datetime.now()
        self.audit_data = []
        self.spark = spark
    
    def log_decision(self, 
                    user_id: str, 
                    account_id: str,
                    qualified: bool,
                    reason: str,
                    relevant_balance: float,
                    calculated_interest: float,
                    interest_rate: float,
                    status: str = "success",
                    error_desc: str = None,
                    partition_date: str = None) -> None:
        """
        Registra una decisión de interés para un usuario
        
        Args:
            user_id: ID del usuario
            account_id: ID de la cuenta
            qualified: Si calificó para interés
            reason: Razón de la decisión
            relevant_balance: Balance considerado
            calculated_interest: Interés calculado
            interest_rate: Tasa aplicada
            status: Estado del proceso
            error_desc: Descripción de error si aplica
            partition_date: Fecha de partición
        """
        record = {
            "job_id": self.job_id,
            "job_timestamp": self.job_timestamp,
            "user_id": user_id or "UNKNOWN_USER", 
            "account_id": account_id or "UNKNOWN_ACCOUNT",
            "qualified": qualified,
            "reason": reason or "No reason provided",
            "relevant_balance": relevant_balance if relevant_balance is not None else 0.0,
            "calculated_interest": calculated_interest if calculated_interest is not None else 0.0, 
            "interest_rate": interest_rate if interest_rate is not None else 0.0,
            "process_status": status or "unknown",
            "error_description": error_desc,
            "partition_date": partition_date
        }
        self.audit_data.append(record)

    def save_to_postgres(self, config: dict) -> None:
        """
        Guarda los registros de auditoría en PostgreSQL
        
        Args:
            config: Diccionario con configuración de conexión
        """
        if not self.audit_data:
            logger.warning("No hay datos de auditoría para guardar")
            return

        try:
            # Crear DataFrame de Spark con esquema explícito
            audit_df = self.spark.createDataFrame(self.audit_data, schema=audit_log_schema)
            
            # Obtener configuración de PostgreSQL
            pg_conf = config["postgres"]
            jdbc_url, connection_props = get_postgres_connection(pg_conf)
            table = f"{pg_conf['schema']}.{pg_conf['interest_audit_log_table']}"
            logger.info(f"Guardando logs de auditoría en {table}")

            # Escribir en PostgreSQL
            audit_df.write.jdbc(
                url=jdbc_url,
                table=table,
                mode="append",
                properties=connection_props
            )
            logger.info(f"Guardados {len(self.audit_data)} registros de auditoría")
            logger.info("✅ Logs de auditoría guardados exitosamente")        
        except Exception as e:
            logger.error(f"Error al guardar logs de auditoría: {str(e)}", exc_info=True)
            raise