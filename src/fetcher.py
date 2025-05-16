import requests
from datetime import datetime
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

logger = logging.getLogger(__name__)

def fetch_cdi_rates(spark: SparkSession, start_date: str, end_date: str):
    base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.4389/dados"
    try:
        start_fmt = datetime.strptime(start_date, "%Y-%m-%d").strftime("%d/%m/%Y")
        end_fmt = datetime.strptime(end_date, "%Y-%m-%d").strftime("%d/%m/%Y")
        
        params = {
            "formato": "json",
            "dataInicial": start_fmt,
            "dataFinal": end_fmt
        }

        headers = {
            "Accept": "application/json"
        }
        
        logger.info(f"Obteniendo CDI desde {start_fmt} hasta {end_fmt}")
        response = requests.get(base_url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 404:
            raise ValueError("Endpoint no encontrado. Verifica la URL y parámetros.")
        
        response.raise_for_status()
        
        data = response.json()
        
        if not data:
            logger.warning("No se encontraron datos para el rango de fechas")
            return spark.createDataFrame([], schema="partition_date STRING, cdi_rate DOUBLE")
        
        # Crear un DataFrame de Spark
        df = spark.createDataFrame(data)
        df = df.withColumnRenamed("data", "partition_date").withColumnRenamed("valor", "cdi_rate")
        
        # Convertir tipos de datos
        df = df.withColumn("partition_date", to_date(col("partition_date"), "dd/MM/yyyy"))  # Convertir a yyyy-MM-dd
        df = df.withColumn("cdi_rate", col("cdi_rate").cast("double") / 100)
        
        # Filtrar valores nulos
        df = df.filter(col("partition_date").isNotNull() & col("cdi_rate").isNotNull())
        
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"Error en la conexión: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        raise

def save_partitioned_parquet(df, output_path: str):
    try:
        # Convertir partition_date a string explícitamente antes de guardar
        df = df.withColumn("partition_date", col("partition_date").cast("string"))
        
        # Guardar particionado por partition_date
        df.write.partitionBy("partition_date").parquet(output_path, mode="overwrite", compression="snappy")
        logger.info(f"Datos guardados en {output_path}")
    except Exception as e:
        logger.error(f"Error al guardar datos: {str(e)}")
        raise