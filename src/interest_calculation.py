from pyspark.sql import DataFrame
from pyspark.sql.functions import col, round as spark_round, when, lit
from src.utils import get_postgres_connection, load_config
from src.schemas import cdi_rates_schema
from src.logger import Logger
from src.spark_manager import SparkSessionManager

logger = Logger().get_logger()
spark = SparkSessionManager().get_spark_session("InterestCalculator")

class InterestCalculator:
    def __init__(self):
        pass

    def _load_filtered_data(self, config, start_date, end_date=None):
        """Carga y filtra los datos necesarios"""
        logger.info("Cargando datos desde PostgreSQL y Parquet")
        
        wallet_history = self._load_wallet_history(config)
        cdi_rates = self._load_cdi_rates(config)
        
        wallet_filtered = self._filter_by_partition_date(wallet_history, start_date, end_date)
        cdi_filtered = self._filter_by_partition_date(cdi_rates, start_date, end_date)
        
        logger.info(f"Wallet history registros cargados: {wallet_filtered.count()}")
        logger.info(f"CDI rates registros cargados: {cdi_filtered.count()}")
        
        return wallet_filtered, cdi_filtered
    
    def _load_wallet_history(self, config):
        """Carga datos de wallet_history desde PostgreSQL"""
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
        """Carga tasas CDI desde archivo Parquet"""
        path = config["paths"]["cdi_rates"]
        logger.info(f"Cargando CDI rates desde Parquet en {path}")
        return spark.read.schema(cdi_rates_schema).parquet(path)
    
    def _filter_by_partition_date(self, df, start_date, end_date=None):
        """Filtra DataFrame por rango de fechas"""
        if end_date is None:
            return df.filter(col("partition_date") == start_date)
        return df.filter(
            (col("partition_date") >= start_date) & 
            (col("partition_date") <= end_date)
        )
    
    def _calculate_interest(self, wallet_history: DataFrame, cdi_rates: DataFrame, 
                          min_balance: float = 100.0, decimal_places: int = 8) -> DataFrame:
        """
        Calcula intereses aplicando tasa CDI a balances elegibles
        """
        # Join por fecha de partición
        joined = wallet_history.join(
            cdi_rates.withColumnRenamed("partition_date", "cdi_date"),
            col("partition_date") == col("cdi_date"),
            "inner"
        )
        
        # Cálculo de intereses
        interest_df = joined.withColumn(
            "eligible_balance",
            when(col("balance") > min_balance, col("balance")).otherwise(0.0)
        ).withColumn(
            "interest_amount",
            spark_round(col("eligible_balance") * col("cdi_rate"), decimal_places)
        ).withColumn(
            "is_paid", lit(False)
        ).withColumn(
            "payment_date", lit(None).cast("timestamp")
        ).select(
            "user_id",
            "account_id",
            col("partition_date"),
            "eligible_balance",
            col("cdi_rate").alias("interest_rate"),
            "interest_amount",
            "is_paid",
            "payment_date"
        )
        
        return interest_df.filter(col("interest_amount") > 0)
    
    def _save_results(self, interest_df: DataFrame, config):
        """Guarda resultados en PostgreSQL"""
        pg_conf = config["postgres"]
        jdbc_url, connection_props = get_postgres_connection(pg_conf)
        target_table = f"{pg_conf['schema']}.{pg_conf['interest_payments_table']}"
        
        logger.info(f"Guardando resultados en la tabla {target_table}")
        interest_df.write.jdbc(
            url=jdbc_url,
            table=target_table,
            mode="overwrite",
            properties=connection_props
        )
        logger.info("✅ Intereses guardados exitosamente")

    def calculate_and_save_interest(self, start_date: str, end_date: str = None):
        """Flujo completo de cálculo y guardado de intereses"""
        try:
            config = load_config()
            
            # Carga de datos
            wallet_df, cdi_df = self._load_filtered_data(config, start_date, end_date)
            
            if wallet_df.rdd.isEmpty() or cdi_df.rdd.isEmpty():
                logger.warning("No se encontraron datos en wallet_history o cdi_rates para las fechas especificadas.")
                return
            
            # Cálculo de intereses
            logger.info("Calculando intereses")
            interest_df = self._calculate_interest(wallet_df, cdi_df)
            
            logger.info(f"Intereses calculados: {interest_df.count()} registros")
            interest_df.show(20, False)
            
            # Guardado en PostgreSQL
            self._save_results(interest_df, config)
            
        except Exception as e:
            logger.error(f"Error en el cálculo de intereses: {str(e)}", exc_info=True)
            raise
    