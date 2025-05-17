import requests
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date
from src.logger import Logger
from src.spark_manager import SparkSessionManager

logger = Logger().get_logger()
spark = SparkSessionManager().get_spark_session("CDIDataFetcher")

class CDIDataFetcher:
    def __init__(self):
        self.base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4389/dados"

    def _fetch_cdi_rates_from_api(self, start_date: str, end_date: str) -> DataFrame:
        """
        Obtiene tasas CDI desde API del BCB y las transforma a DataFrame
        
        Returns:
            DataFrame: Con columnas [partition_date, cdi_rate]
        """
        try:
            # Formatear fechas para la API
            start_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d/%m/%Y")
            end_fmt = datetime.strptime(end_date, "%Y-%m-%d").strftime("%d/%m/%Y")

            logger.info(f"Consultando API BCB para {start_fmt} a {end_fmt}")
            
            response = requests.get(
                self.base_url,
                params={
                    "formato": "json",
                    "dataInicial": start_fmt,
                    "dataFinal": end_fmt
                },
                headers={"Accept": "application/json"},
                timeout=30
            )
            response.raise_for_status()

            data = response.json()
            if not data:
                logger.warning("El API no devolvió datos")
                return spark.createDataFrame([], schema="partition_date DATE, cdi_rate DOUBLE")

            # Procesamiento de los datos crudos
            processed_df = self._process_raw_cdi_data(data)
            logger.info(f"Obtenidos {processed_df.count()} registros de tasas CDI")
            
            return processed_df

        except requests.exceptions.HTTPError as e:
            logger.error(f"Error HTTP {e.response.status_code} al consultar API")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Error de conexión: {str(e)}")
            raise

    def _process_raw_cdi_data(self, raw_data: list) -> DataFrame:
        """
        Transforma los datos crudos del API en un DataFrame estructurado
        """
        df = spark.createDataFrame(raw_data)
        
        return (df
            .withColumnRenamed("data", "partition_date")
            .withColumnRenamed("valor", "cdi_rate")
            .withColumn("partition_date", to_date(col("partition_date"), "dd/MM/yyyy"))
            .withColumn("cdi_rate", col("cdi_rate").cast("double") / 100)
            .filter(col("partition_date").isNotNull() & col("cdi_rate").isNotNull())
        )

    def _save_cdi_rates(self, df: DataFrame, output_path: str):
        """
        Guarda las tasas CDI en formato Parquet particionado por fecha
        """
        try:
            (df
                .withColumn("partition_date", col("partition_date").cast("string"))
                .write
                .partitionBy("partition_date")
                .mode("overwrite")
                .parquet(output_path, compression="snappy")
            )
            logger.info(f"Datos guardados en {output_path}")
        except Exception as e:
            logger.error(f"Error al guardar Parquet: {str(e)}")
            raise

    def fetch_and_save_cdi_rates(self, output_path: str, start_date: str, end_date: str) -> bool:
        """
        Flujo completo para obtener tasas CDI del BCB y guardarlas en Parquet particionado.
        
        Args:
            start_date (str): Fecha inicio en formato YYYY-MM-DD
            end_date (str): Fecha fin en formato YYYY-MM-DD
            output_path (str): Ruta donde guardar los datos
            
        Returns:
            bool: True si el proceso fue exitoso, False si no hay datos
        """
        try:
            logger.info(f"Obteniendo tasas CDI desde la API del BCB")
            
            # 1. Obtener datos del API
            cdi_df = self._fetch_cdi_rates_from_api(start_date, end_date)

            if cdi_df.isEmpty():
                logger.warning("No se encontraron datos CDI para el período especificado")
                return False

            cdi_df.show(20, False)

            # 2. Guardar en formato particionado
            self._save_cdi_rates(cdi_df, output_path)
            
            logger.info("Proceso completado exitosamente")
            return True
            
        except Exception as e:
            logger.error(f"Error en CDIDataFetcher: {str(e)}", exc_info=True)
            raise
